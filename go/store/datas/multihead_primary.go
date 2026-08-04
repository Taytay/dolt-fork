// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Multi-head as the PRIMARY commit path (MULTIHEAD.md Step 2c).
//
// Steps 2 and 2b added multi-head storage additively, beside the untouched
// single-root path: AppendCommit/RecordTip/Tips/MergeTips are explicit calls a
// caller opts into. This file makes multi-head the primary behavior of the
// ordinary Database API — Commit and GetDataset — for a database opened in
// multi-head mode:
//
//   - Commit publishes the new commit as a *tip* with no fast-forward/lineage
//     gate and no compare-and-swap on the ref. A sequential commit names the
//     observed head as its parent, so the frontier collapses back to one tip
//     (a fast-forward); a divergent commit (from a stale Dataset, or with
//     explicit parents that don't include the current tip) simply adds a head
//     instead of returning ErrMergeNeeded. This is the anti-CAS property of the
//     nbs roots layer and the datas frontier, now on the default write path.
//
//   - GetDataset resolves the ref's frontier: zero tips -> no head; one tip ->
//     that tip is the head (indistinguishable from the single-root case to
//     every existing reader); more than one tip -> ErrMultipleHeads, telling
//     the caller to reconcile the fork (MergeTips) and record the result.
//
// Why a per-database mode rather than a Dolt-wide flip: the single-root CAS is
// assumed by essentially every reader, refspec resolver, SQL path, and the GC,
// so flipping it unconditionally would regress the whole suite (invariant #4).
// Mode-gating makes multi-head a real, first-class *primary* path — a normal
// Commit is multi-tip when the mode is on — while a database opened normally is
// byte-identical to stock. Flipping the global default (every reader taught to
// expect a frontier) remains the larger follow-on; this is the seam it hangs
// on.
package datas

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// EnableMultihead switches db to the primary multi-head commit path (see the
// package comment). It is opt-in and off by default: a database constructed
// through NewDatabase/NewTypesDatabase keeps the stock single-root behavior
// until this is called. It returns an error for a Database not produced by the
// public constructors.
func EnableMultihead(db Database) error {
	d, err := asDatabase(db)
	if err != nil {
		return err
	}
	d.multihead = true
	return nil
}

// IsMultihead reports whether db is in the primary multi-head mode.
func IsMultihead(db Database) bool {
	d, err := asDatabase(db)
	if err != nil {
		return false
	}
	return d.multihead
}

// commitMultihead is the multi-head primary Commit: it records the new commit
// as a tip with no lineage gate and no ref CAS. A sequential commit fast-
// forwards (the new tip names the observed head, so the frontier drops it); a
// divergent commit adds a head. The returned Dataset points at the newly
// written commit regardless of whether the ref is now forked — the fork is
// observable through GetDataset/Tips, which is where ErrMultipleHeads surfaces.
func (db *database) commitMultihead(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	if opts.Meta == nil {
		opts.Meta = &CommitMeta{}
	}
	// Default the parent to the head this caller observed, so an ordinary
	// sequential commit supersedes the current tip (fast-forward) rather than
	// forking. A caller that wants an explicit lineage sets opts.Parents; a
	// commit off a stale Dataset naturally forks because its parent is no
	// longer the frontier.
	if len(opts.Parents) == 0 {
		if headAddr, hasHead := ds.MaybeHeadAddr(); hasHead {
			opts.Parents = []hash.Hash{headAddr}
		}
	}

	addr, err := db.appendCommit(ctx, ds.ID(), v, opts)
	if err != nil {
		return Dataset{}, err
	}

	cv, err := db.ReadValue(ctx, addr)
	if err != nil {
		return Dataset{}, err
	}
	return newDataset(ctx, db, ds.ID(), cv, addr)
}

// datasetFromFrontier resolves a ref to a Dataset via its frontier, for the
// primary multi-head path. It computes the frontier against the address map the
// caller already loaded (so GetDatasetByRootHash resolves against that specific
// root, not the current one). Zero tips is a headless dataset; one tip is the
// head; more than one tip is ErrMultipleHeads.
func (db *database) datasetFromFrontier(ctx context.Context, datasetID string, dsmap DatasetsMap) (Dataset, error) {
	rmdsmap, ok := dsmap.(refmapDatasetsMap)
	if !ok {
		return Dataset{}, fmt.Errorf("multihead: unsupported DatasetsMap type %T", dsmap)
	}

	frontier, err := db.frontierOf(ctx, rmdsmap.am, datasetID)
	if err != nil {
		return Dataset{}, err
	}

	switch len(frontier) {
	case 0:
		// No tips: fall back to any single-root head recorded under the bare
		// dataset id, so a ref written before the mode was enabled (or by a
		// non-multihead writer) is still visible. Absent that, it is headless.
		curr, err := rmdsmap.am.Get(ctx, datasetID)
		if err != nil {
			return Dataset{}, err
		}
		if curr.IsEmpty() {
			return newDataset(ctx, db, datasetID, nil, hash.Hash{})
		}
		head, err := db.ReadValue(ctx, curr)
		if err != nil {
			return Dataset{}, err
		}
		return newDataset(ctx, db, datasetID, head, curr)
	case 1:
		head, err := db.ReadValue(ctx, frontier[0])
		if err != nil {
			return Dataset{}, err
		}
		return newDataset(ctx, db, datasetID, head, frontier[0])
	default:
		return Dataset{}, fmt.Errorf("%w: ref %q has %d tips", ErrMultipleHeads, datasetID, len(frontier))
	}
}
