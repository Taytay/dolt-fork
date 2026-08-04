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

// Multi-head reconcile: collapse a fork with Dolt's *real* three-way merge.
//
// Step 2 gave a ref a set of tips (multihead_datasets.go). This file resolves a
// two-tip fork by three-way merging the tips' committed prolly maps with
// `prolly.MergeMaps` — the same tree-level merge Dolt's SQL row merge is built
// on — using `FindCommonAncestor` for the base. It is the Step-2b relaxation of
// the model three-way the conformance harness used before: the merge decision
// (auto-merge vs. collision, deletes, convergent edits, ordering) is now Dolt's
// own, not a reimplementation.
//
// These helpers are schema- and policy-agnostic: the caller passes the table's
// key/value descriptors and a CollisionFn (the merge policy). A table is
// committed as a tip with AppendMapCommit (the committed value is the map's
// root node, exactly how datas persists its own AddressMaps, so the map's
// chunks are properly referenced); TipMap reads it back; MergeTips returns the
// merged map WITHOUT committing so the caller can refuse (e.g. a "fail" policy
// on any conflict) before AppendMapCommit-ing the result as the collapsing tip.
package datas

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// NodeStore exposes the database's tree.NodeStore so a caller can build and
// read prolly maps that share this store — e.g. to commit a table as a tip and
// reconcile forks with Dolt's real three-way merge.
func NodeStore(db Database) (tree.NodeStore, error) {
	d, err := asDatabase(db)
	if err != nil {
		return nil, err
	}
	return d.nodeStore(), nil
}

// AppendMapCommit commits a prolly.Map as a tip of ref (via the gate-free
// multi-tip append, so a divergent commit adds a head). The committed value is
// the map's root node — the same representation datas uses for its own
// AddressMaps — so the map's chunks are referenced and persist with the commit.
func AppendMapCommit(ctx context.Context, db Database, ref string, m prolly.Map, opts CommitOptions) (hash.Hash, error) {
	return AppendCommit(ctx, db, ref, tree.ValueFromNode(m.Node()), opts)
}

// TipMap loads the prolly.Map committed at a tip by AppendMapCommit, using the
// caller-supplied key/value descriptors (the table schema).
func TipMap(ctx context.Context, db Database, tip hash.Hash, kd, vd *val.TupleDesc) (prolly.Map, error) {
	d, err := asDatabase(db)
	if err != nil {
		return prolly.Map{}, err
	}
	cv, err := d.ReadValue(ctx, tip)
	if err != nil {
		return prolly.Map{}, err
	}
	rootHash, err := GetCommitRootHash(cv)
	if err != nil {
		return prolly.Map{}, err
	}
	return loadMap(ctx, d.nodeStore(), rootHash, kd, vd)
}

func loadMap(ctx context.Context, ns tree.NodeStore, rootHash hash.Hash, kd, vd *val.TupleDesc) (prolly.Map, error) {
	if rootHash.IsEmpty() {
		return prolly.NewMapFromTuples(ctx, ns, kd, vd)
	}
	node, err := ns.Read(ctx, rootHash)
	if err != nil {
		return prolly.Map{}, err
	}
	return prolly.NewMap(node, ns, kd, vd), nil
}

// MergeTips three-way merges the maps at two tips with Dolt's real prolly merge
// (prolly.MergeMaps). ours is the "left"/destination and theirs the
// "right"/source; the base is the maps' common-ancestor commit
// (FindCommonAncestor), or an empty map if there is none. |collide| resolves
// keys changed differently on both sides (the merge policy). The merged map is
// returned WITHOUT being committed: the caller decides whether to keep it (and
// then AppendMapCommit-s it as the tip that collapses the fork) or to refuse.
func MergeTips(ctx context.Context, db Database, ours, theirs hash.Hash, kd, vd *val.TupleDesc, collide tree.CollisionFn) (prolly.Map, error) {
	d, err := asDatabase(db)
	if err != nil {
		return prolly.Map{}, err
	}

	oursMap, err := TipMap(ctx, db, ours, kd, vd)
	if err != nil {
		return prolly.Map{}, err
	}
	theirsMap, err := TipMap(ctx, db, theirs, kd, vd)
	if err != nil {
		return prolly.Map{}, err
	}
	baseMap, err := baseMapOf(ctx, d, ours, theirs, kd, vd)
	if err != nil {
		return prolly.Map{}, err
	}

	merged, _, err := prolly.MergeMaps(ctx, oursMap, theirsMap, baseMap, collide)
	if err != nil {
		return prolly.Map{}, err
	}
	return merged, nil
}

// baseMapOf returns the committed map of the common-ancestor commit of two
// tips, or an empty map if the tips share no ancestor.
func baseMapOf(ctx context.Context, d *database, ours, theirs hash.Hash, kd, vd *val.TupleDesc) (prolly.Map, error) {
	oc, err := LoadCommitAddr(ctx, d, ours)
	if err != nil {
		return prolly.Map{}, err
	}
	tc, err := LoadCommitAddr(ctx, d, theirs)
	if err != nil {
		return prolly.Map{}, err
	}
	baseAddr, found, err := FindCommonAncestor(ctx, oc, tc, d, d, d.nodeStore(), d.nodeStore())
	if err != nil {
		return prolly.Map{}, err
	}
	if !found {
		return prolly.NewMapFromTuples(ctx, d.nodeStore(), kd, vd)
	}
	return TipMap(ctx, d, baseAddr, kd, vd)
}
