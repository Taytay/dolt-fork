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

// Multi-head datasets: the datas-layer half of the CAS-free, multi-head,
// clone-safe design (Step 2 of MULTIHEAD.md; the nbs roots layer is Step 1).
//
// Stock nbs/datas advances ONE head per ref via an optimistic
// compare-and-swap: datas/database_common.go doCommit rejects a commit whose
// parent is not the current head with ErrMergeNeeded (the "curr !=
// datasetCurrentAddr" gate), and BuildNewCommit refuses a non-fast-forward
// with the same error. That single contended cell needs a CAS the store may
// lack on S3, corrupts under concurrent writers on a synced folder, and
// rejects non-fast-forward pushes.
//
// This file makes a ref hold a SET OF TIPS (the frontier of its commit DAG)
// instead of one CAS'd head. Appending a commit is append-only and takes no
// CAS on the ref: a divergent commit adds a tip rather than erroring. The
// current state of a ref is the set of tips no other tip names as a parent
// (one tip = writers agree; several = a live fork to reconcile later, via
// Dolt's existing three-way merge). Tips are recorded as content-addressed
// sub-keys of the ref in the same root prolly.AddressMap, so:
//
//   - writes never collide (the key IS the commit hash) and are idempotent;
//   - there is no writer id, so a cloned disk becomes another fork, never a
//     silent overwrite (the clone-safety property, mirrored from the nbs
//     roots layer);
//   - the single-root commit path (doCommit/BuildNewCommit) is untouched, so
//     existing behavior is unchanged until it is deliberately replaced.
//
// This is the datas analogue of the nbs multiheadRoots layer and of the
// experiments/multihead-store Python spike; it is validated against the
// portable merge-conformance suite (see multihead_conf/ and the driver in
// taytays_stuff experiments/multihead-dolt-nbs/conformance/).
package datas

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

// tipKeyInfix separates a ref name from a tip's content hash in the root
// address map. It contains NUL bytes so it cannot occur inside a validated
// dataset id, keeping multi-tip keys disjoint from the single-root keys that
// share the map.
const tipKeyInfix = "\x00multihead-tip\x00"

func tipKeyPrefix(ref string) string { return ref + tipKeyInfix }

func tipKey(ref string, tip hash.Hash) string {
	return tipKeyPrefix(ref) + tip.String()
}

// asDatabase recovers the concrete *database. NewDatabase/NewTypesDatabase
// always return one, so this holds for any Database made through the public
// constructors.
func asDatabase(db Database) (*database, error) {
	if d, ok := db.(*database); ok {
		return d, nil
	}
	return nil, fmt.Errorf("multihead: unsupported Database implementation %T", db)
}

// AppendCommit builds a commit for v with the given parents and records it as
// a tip of ref. Unlike Commit/BuildNewCommit there is NO fast-forward or
// lineage gate: a commit whose parents do not include the ref's current tip
// is allowed and simply adds a head. Recording the tip is append-only and
// takes no CAS on the ref.
//
// It returns the new commit's address. Recording a tip is idempotent (the key
// is the commit hash); the whole call is idempotent when opts.Meta pins the
// commit's dates, since only then does an identical (v, parents) build the
// same commit.
func AppendCommit(ctx context.Context, db Database, ref string, v types.Value, opts CommitOptions) (hash.Hash, error) {
	d, err := asDatabase(db)
	if err != nil {
		return hash.Hash{}, err
	}
	return d.appendCommit(ctx, ref, v, opts)
}

func (db *database) appendCommit(ctx context.Context, ref string, v types.Value, opts CommitOptions) (hash.Hash, error) {
	if opts.Meta == nil {
		opts.Meta = &CommitMeta{}
	}
	// Build the commit WITHOUT the lineage gate: call newCommitForValue
	// directly rather than BuildNewCommit, so a divergent commit is allowed.
	// This is the Step-2 relaxation of the ErrMergeNeeded gate, confined to
	// the multi-tip path.
	commit, err := newCommitForValue(ctx, db.chunkStore(), db, db.nodeStore(), v, opts)
	if err != nil {
		return hash.Hash{}, err
	}
	if _, err = db.WriteValue(ctx, commit.NomsValue()); err != nil {
		return hash.Hash{}, err
	}
	addr := commit.Addr()
	if err = db.recordTip(ctx, ref, addr); err != nil {
		return hash.Hash{}, err
	}
	return addr, nil
}

// RecordTip records an already-written commit address as a tip of ref. It is
// append-only, takes no CAS on the ref, and is idempotent (the key is the
// commit hash). Use it to publish a commit built elsewhere (e.g. a merge
// result) as a head.
func RecordTip(ctx context.Context, db Database, ref string, commitAddr hash.Hash) error {
	d, err := asDatabase(db)
	if err != nil {
		return err
	}
	return d.recordTip(ctx, ref, commitAddr)
}

func (db *database) recordTip(ctx context.Context, ref string, commitAddr hash.Hash) error {
	// Append-only into the root address map. No "curr != expected" CAS on the
	// ref: two concurrent appends add distinct content-addressed keys and both
	// survive. The low-level nbs root retry in db.update only serializes the
	// map writes; it never rejects a divergent tip.
	return db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		ae := am.Editor()
		if err := ae.Update(ctx, tipKey(ref, commitAddr), commitAddr); err != nil {
			return prolly.AddressMap{}, err
		}
		return ae.Flush(ctx)
	})
}

// AllFrontierTips returns, for every ref that has multi-head tips recorded, the
// ref's frontier (the tips no other tip of that ref names as an immediate
// parent). Refs that carry only a bare single-root head — no tip sub-keys — are
// omitted, since Datasets already enumerates those normally.
//
// This is the garbage collector's view of the live multi-head heads: Datasets
// collapses a forked ref to its one canonical (lowest-hash) tip, so the OTHER
// tips of a fork are invisible to a reader that walks Datasets. GC must keep
// every frontier tip as a root, or a divergent fork's unique history would be
// collected out from under a not-yet-reconciled branch. A superseded (non-
// frontier) tip is an ancestor of some frontier tip, so keeping the frontier
// keeps the whole recorded DAG by reachability.
func AllFrontierTips(ctx context.Context, db Database) (map[string][]hash.Hash, error) {
	d, err := asDatabase(db)
	if err != nil {
		return nil, err
	}
	rootHash, err := d.rt.Root(ctx)
	if err != nil {
		return nil, err
	}
	am, err := d.loadDatasetsRefmap(ctx, rootHash)
	if err != nil {
		return nil, err
	}

	// One pass to find which refs have tip sub-keys at all.
	refs := make(map[string]struct{})
	err = am.IterAll(ctx, func(key string, _ hash.Hash) error {
		if i := strings.Index(key, tipKeyInfix); i >= 0 {
			refs[key[:i]] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out := make(map[string][]hash.Hash, len(refs))
	for ref := range refs {
		frontier, err := d.frontierOf(ctx, am, ref)
		if err != nil {
			return nil, err
		}
		if len(frontier) > 0 {
			out[ref] = frontier
		}
	}
	return out, nil
}

// RefFrontierAcrossRoots computes a ref's frontier across SEVERAL whole-store
// roots — the read side of relational multi-head over the append-only nbs
// `roots/` layer. Where Tips reads the ref's tips from the one current root
// address map, this unions the ref's tips from every root in |roots| (each the
// Noms root of a `roots/` frontier record, via nbs.MultiheadFolderRootHashes)
// and then drops any tip another names as an immediate parent, so a fork
// published by independent writers into a shared folder collapses to its true
// frontier.
//
// It is the counterpart of a manifest-CAS RecordTip for the lazy-sync path: a
// writer deposits its whole root as an append-only `roots/` record (no manifest
// overwrite), and a reader reconstructs the per-ref frontier here without any
// writer ever having contended on a single mutable head. The store must be able
// to read the chunks of every root (e.g. after nbs.MaterializeFrontierManifest
// has unioned the frontier's table specs).
func RefFrontierAcrossRoots(ctx context.Context, db Database, refStr string, roots []hash.Hash) ([]hash.Hash, error) {
	d, err := asDatabase(db)
	if err != nil {
		return nil, err
	}
	prefix := tipKeyPrefix(refStr)

	// Gather the union of the ref's tip addresses across every root.
	seen := make(map[hash.Hash]struct{})
	var all []hash.Hash
	for _, root := range roots {
		am, err := d.loadDatasetsRefmap(ctx, root)
		if err != nil {
			return nil, err
		}
		err = am.IterAll(ctx, func(key string, addr hash.Hash) error {
			if strings.HasPrefix(key, prefix) {
				if _, ok := seen[addr]; !ok {
					seen[addr] = struct{}{}
					all = append(all, addr)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// A tip is superseded iff some tip names it as an immediate parent.
	superseded := make(map[hash.Hash]struct{})
	for _, a := range all {
		cv, err := d.ReadValue(ctx, a)
		if err != nil {
			return nil, err
		}
		parents, err := GetCommitParents(ctx, d, cv)
		if err != nil {
			return nil, err
		}
		for _, p := range parents {
			superseded[p.Addr()] = struct{}{}
		}
	}

	var frontier []hash.Hash
	for _, a := range all {
		if _, ok := superseded[a]; !ok {
			frontier = append(frontier, a)
		}
	}
	sort.Slice(frontier, func(i, j int) bool {
		return bytes.Compare(frontier[i][:], frontier[j][:]) < 0
	})
	return frontier, nil
}

// TipValue reads the committed value (the stored root value) of a commit tip.
// It is a small convenience for callers that hold a tip address from Tips and
// want the value it commits, without reaching for the unexported value reader.
// An empty address returns (nil, nil).
func TipValue(ctx context.Context, db Database, commitAddr hash.Hash) (types.Value, error) {
	d, err := asDatabase(db)
	if err != nil {
		return nil, err
	}
	if commitAddr.IsEmpty() {
		return nil, nil
	}
	cv, err := d.ReadValue(ctx, commitAddr)
	if err != nil {
		return nil, err
	}
	return GetCommittedValue(ctx, d, cv)
}

// Tips returns the frontier of ref: the tip commit addresses that no other
// tip names as a parent, sorted for a stable result. One tip means the
// writers agree (a fast-forward chain collapses to its head); several tips
// mean a live fork to reconcile later. An empty ref returns no tips.
//
// The frontier is computed from direct parents only — a tip is dropped iff
// another tip names it as an immediate parent. That is correct for the
// append-a-tip-naming-its-predecessor discipline used here (every commit on a
// chain is recorded and names the one before it), and it costs one read per
// tip rather than a full ancestry walk, so it stays cheap on a dumb remote
// that may not hold deep history.
func Tips(ctx context.Context, db Database, ref string) ([]hash.Hash, error) {
	d, err := asDatabase(db)
	if err != nil {
		return nil, err
	}
	return d.tips(ctx, ref)
}

func (db *database) tips(ctx context.Context, ref string) ([]hash.Hash, error) {
	rootHash, err := db.rt.Root(ctx)
	if err != nil {
		return nil, err
	}
	am, err := db.loadDatasetsRefmap(ctx, rootHash)
	if err != nil {
		return nil, err
	}
	return db.frontierOf(ctx, am, ref)
}

// frontierOf computes ref's frontier within a given root address map: the tip
// commit addresses that no other tip names as an immediate parent, sorted for a
// stable result. It is factored out of tips so the primary multi-head path
// (datasetFromFrontier) can resolve a ref against the exact map it already
// holds, rather than re-reading the current root.
func (db *database) frontierOf(ctx context.Context, am prolly.AddressMap, ref string) ([]hash.Hash, error) {
	prefix := tipKeyPrefix(ref)
	var all []hash.Hash
	err := am.IterAll(ctx, func(key string, addr hash.Hash) error {
		if strings.HasPrefix(key, prefix) {
			all = append(all, addr)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// A tip is superseded iff some tip names it as an immediate parent.
	superseded := make(map[hash.Hash]struct{})
	for _, a := range all {
		cv, err := db.ReadValue(ctx, a)
		if err != nil {
			return nil, err
		}
		parents, err := GetCommitParents(ctx, db, cv)
		if err != nil {
			return nil, err
		}
		for _, p := range parents {
			superseded[p.Addr()] = struct{}{}
		}
	}

	var frontier []hash.Hash
	for _, a := range all {
		if _, ok := superseded[a]; !ok {
			frontier = append(frontier, a)
		}
	}
	sort.Slice(frontier, func(i, j int) bool {
		return bytes.Compare(frontier[i][:], frontier[j][:]) < 0
	})
	return frontier, nil
}
