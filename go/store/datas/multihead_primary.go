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
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
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

// EnableMultiheadResolve switches db to multi-head mode AND makes reads
// fork-tolerant: GetDataset resolves a forked ref to a single canonical head
// (the lowest-hash tip) instead of returning ErrMultipleHeads, and Datasets
// presents each ref once. This is the mode the CLI/SQL stack uses (via the
// DOLT_MULTIHEAD env var) so ordinary commands keep working across a fork; the
// full frontier is still available through Tips. It is opt-in and off by
// default. A caller that wants the strict "error on fork" behavior uses
// EnableMultihead instead.
func EnableMultiheadResolve(db Database) error {
	d, err := asDatabase(db)
	if err != nil {
		return err
	}
	d.multihead = true
	d.multiheadResolveHead = true
	return nil
}

// IsMultiheadResolve reports whether db resolves forks to a canonical head
// (rather than returning ErrMultipleHeads).
func IsMultiheadResolve(db Database) bool {
	d, err := asDatabase(db)
	if err != nil {
		return false
	}
	return d.multiheadResolveHead
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

// commitWithWorkingSetMultihead is the multi-head CommitWithWorkingSet: it
// records the new commit as a TIP of the head ref (no lineage gate, no CAS on
// the head — a divergent commit adds a head instead of ErrMergeNeeded) while
// still updating the working set atomically under its optimistic check. The
// current head is filled in as a parent so a sequential commit fast-forwards;
// only a commit off a stale/observed-elsewhere head forks. This is the write
// path the CLI/SQL stack takes (dolt commit -> doltdb.CommitWithWorkingSet).
func (db *database) commitWithWorkingSetMultihead(
	ctx context.Context,
	commitDS, workingSetDS Dataset,
	val types.Value, workingSetSpec WorkingSetSpec,
	prevWsHash hash.Hash, opts CommitOptions,
) (Dataset, Dataset, error) {
	if opts.Meta == nil {
		opts.Meta = &CommitMeta{}
	}
	wsAddr, err := newWorkingSet(ctx, db, workingSetSpec)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	// Fill the observed head in as a parent (so a sequential commit
	// fast-forwards), but with NO ErrMergeNeeded gate — a divergent commit just
	// adds a tip.
	if headAddr, ok := commitDS.MaybeHeadAddr(); ok {
		if len(opts.Parents) == 0 {
			opts.Parents = []hash.Hash{headAddr}
		} else if !hasParentHash(opts, headAddr) {
			opts.Parents = append([]hash.Hash{headAddr}, opts.Parents...)
		}
	}

	commit, err := newCommitForValue(ctx, db.chunkStore(), db, db.nodeStore(), val, opts)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}
	if _, err := db.WriteValue(ctx, commit.NomsValue()); err != nil {
		return Dataset{}, Dataset{}, err
	}
	addr := commit.Addr()

	err = db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		// The working set keeps its optimistic check (it is local mutable state,
		// not the shared fork point).
		currWS, err := am.Get(ctx, workingSetDS.ID())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if currWS != prevWsHash {
			return prolly.AddressMap{}, ErrOptimisticLockFailed
		}
		ae := am.Editor()
		// Head update is append-a-tip: no "curr != expected" CAS, so a divergent
		// commit coexists as another head instead of being rejected.
		if err := ae.Update(ctx, tipKey(commitDS.ID(), addr), addr); err != nil {
			return prolly.AddressMap{}, err
		}
		if err := ae.Update(ctx, workingSetDS.ID(), wsAddr); err != nil {
			return prolly.AddressMap{}, err
		}
		return ae.Flush(ctx)
	})
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	currentDatasets, err := db.Datasets(ctx)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}
	commitDS, err = db.datasetFromMap(ctx, commitDS.ID(), currentDatasets)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}
	workingSetDS, err = db.datasetFromMap(ctx, workingSetDS.ID(), currentDatasets)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}
	return commitDS, workingSetDS, nil
}

// datasetFromFrontier resolves a ref to a Dataset via its frontier, for the
// primary multi-head path. It computes the frontier against the address map the
// caller already loaded (so GetDatasetByRootHash resolves against that specific
// root, not the current one). Zero tips is a headless dataset; one tip is the
// head; more than one tip is ErrMultipleHeads.
func (db *database) datasetFromFrontier(ctx context.Context, datasetID string, dsmap DatasetsMap) (Dataset, error) {
	am, ok := addrMapOf(dsmap)
	if !ok {
		return Dataset{}, fmt.Errorf("multihead: unsupported DatasetsMap type %T", dsmap)
	}

	frontier, err := db.frontierOf(ctx, am, datasetID)
	if err != nil {
		return Dataset{}, err
	}

	switch {
	case len(frontier) == 0:
		// No tips: fall back to any single-root head recorded under the bare
		// dataset id, so a ref written before the mode was enabled (or by a
		// non-multihead writer, e.g. repo init) is still visible. Absent that,
		// it is headless.
		curr, err := am.Get(ctx, datasetID)
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
	case len(frontier) == 1:
		return db.datasetAtTip(ctx, datasetID, frontier[0])
	case db.multiheadResolveHead:
		// Fork-tolerant reads: resolve to the canonical (lowest-hash) tip.
		// frontierOf returns tips sorted by hash, so frontier[0] is canonical.
		// The whole set stays available via Tips.
		return db.datasetAtTip(ctx, datasetID, frontier[0])
	default:
		return Dataset{}, fmt.Errorf("%w: ref %q has %d tips", ErrMultipleHeads, datasetID, len(frontier))
	}
}

func (db *database) datasetAtTip(ctx context.Context, datasetID string, tip hash.Hash) (Dataset, error) {
	head, err := db.ReadValue(ctx, tip)
	if err != nil {
		return Dataset{}, err
	}
	return newDataset(ctx, db, datasetID, head, tip)
}

// addrMapOf extracts the underlying address map from either DatasetsMap
// implementation the datas layer produces.
func addrMapOf(dsmap DatasetsMap) (prolly.AddressMap, bool) {
	switch m := dsmap.(type) {
	case refmapDatasetsMap:
		return m.am, true
	case multiheadDatasetsMap:
		return m.am, true
	}
	return prolly.AddressMap{}, false
}

// multiheadDatasetsMap presents a root address map with multi-tip encoding as an
// ordinary DatasetsMap: it enumerates each ref exactly once (collapsing the
// content-addressed tip sub-keys) resolved to a single head, so branch/tag
// enumeration never sees the internal encoding. A ref with tips resolves to its
// canonical (lowest-hash) tip; a ref with only a bare single-root entry resolves
// to that. The authoritative frontier is still available via Tips.
type multiheadDatasetsMap struct {
	db *database
	am prolly.AddressMap
}

// uniqueRefs returns the set of ref names in the map, stripping the tip sub-key
// suffix from multi-tip keys so each ref appears once.
func (m multiheadDatasetsMap) uniqueRefs(ctx context.Context) ([]string, error) {
	seen := make(map[string]struct{})
	err := m.am.IterAll(ctx, func(key string, _ hash.Hash) error {
		ref := key
		if i := strings.Index(key, tipKeyInfix); i >= 0 {
			ref = key[:i]
		}
		seen[ref] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	refs := make([]string, 0, len(seen))
	for r := range seen {
		refs = append(refs, r)
	}
	sort.Strings(refs)
	return refs, nil
}

// headOf resolves a ref to its single head within this map: the canonical tip if
// it has tips, else the bare single-root entry, else the empty hash.
func (m multiheadDatasetsMap) headOf(ctx context.Context, ref string) (hash.Hash, error) {
	frontier, err := m.db.frontierOf(ctx, m.am, ref)
	if err != nil {
		return hash.Hash{}, err
	}
	if len(frontier) > 0 {
		return frontier[0], nil // sorted by hash -> canonical
	}
	return m.am.Get(ctx, ref)
}

func (m multiheadDatasetsMap) IterAll(ctx context.Context, cb func(string, hash.Hash) error) error {
	refs, err := m.uniqueRefs(ctx)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		head, err := m.headOf(ctx, ref)
		if err != nil {
			return err
		}
		if head.IsEmpty() {
			continue
		}
		if err := cb(ref, head); err != nil {
			return err
		}
	}
	return nil
}

func (m multiheadDatasetsMap) Len() (uint64, error) {
	refs, err := m.uniqueRefs(context.Background())
	if err != nil {
		return 0, err
	}
	return uint64(len(refs)), nil
}
