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

package datas

import (
	"bytes"
	"context"
	"io"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// The conformance table schema: pk id:int64; value (v:string, n:int64 nullable).
var (
	mhKd = val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc})
	mhVd = val.NewTupleDescriptor(
		val.Type{Enc: val.StringEnc},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)
)

type testRow struct {
	v string
	n *int
}

func mapFromState(t *testing.T, db *database, s map[int]testRow) prolly.Map {
	t.Helper()
	ctx := context.Background()
	ns := db.nodeStore()
	pool := ns.Pool()

	ids := make([]int, 0, len(s))
	for k := range s {
		ids = append(ids, k)
	}
	sort.Ints(ids)

	var tups []val.Tuple
	for _, id := range ids {
		kb := val.NewTupleBuilder(mhKd, ns)
		kb.PutInt64(0, int64(id))
		k, err := kb.Build(ctx, pool)
		require.NoError(t, err)

		vb := val.NewTupleBuilder(mhVd, ns)
		require.NoError(t, vb.PutString(0, s[id].v))
		if s[id].n != nil {
			vb.PutInt64(1, int64(*s[id].n))
		}
		v, err := vb.Build(ctx, pool)
		require.NoError(t, err)

		tups = append(tups, k, v)
	}
	m, err := prolly.NewMapFromTuples(ctx, ns, mhKd, mhVd, tups...)
	require.NoError(t, err)
	return m
}

func stateFromMap(t *testing.T, m prolly.Map) map[int]testRow {
	t.Helper()
	ctx := context.Background()
	it, err := m.IterAll(ctx)
	require.NoError(t, err)
	out := map[int]testRow{}
	for {
		k, v, err := it.Next(ctx)
		if err == io.EOF || k == nil {
			break
		}
		require.NoError(t, err)
		id, _ := mhKd.GetInt64(0, k)
		vs, _ := mhVd.GetString(0, v)
		var np *int
		if n64, ok := mhVd.GetInt64(1, v); ok {
			n := int(n64)
			np = &n
		}
		out[int(id)] = testRow{v: vs, n: np}
	}
	return out
}

// policyCollide resolves a colliding key per policy and records the conflict.
func policyCollide(policy string, conflicts *[]int) tree.CollisionFn {
	return func(l, r tree.Diff) (tree.Diff, bool) {
		if l.Type == r.Type && bytes.Equal(l.To, r.To) {
			return l, true // convergent edit: not a conflict
		}
		id, _ := mhKd.GetInt64(0, val.Tuple(l.Key))
		*conflicts = append(*conflicts, int(id))
		if policy == "theirs" {
			return r, true
		}
		return l, true // ours / record / fail keep ours
	}
}

func str(s string) testRow { return testRow{v: s} }

func appendMap(t *testing.T, db *database, ref string, s map[int]testRow, parents ...hash.Hash) hash.Hash {
	t.Helper()
	addr, err := AppendMapCommit(context.Background(), db, ref,
		mapFromState(t, db, s), CommitOptions{Parents: parents, Meta: pinnedMeta()})
	require.NoError(t, err)
	return addr
}

// A disjoint fork merges cleanly via Dolt's real prolly merge, and committing
// the result collapses the frontier to one tip.
func TestReconcile_DisjointCleanMerge(t *testing.T) {
	db := newMHDatabase(t)
	ctx := context.Background()
	const ref = "refs/heads/main"

	base := appendMap(t, db, ref, map[int]testRow{1: str("a"), 2: str("b"), 3: str("c")})
	ours := appendMap(t, db, ref, map[int]testRow{1: str("A"), 2: str("b"), 3: str("c")}, base)
	theirs := appendMap(t, db, ref, map[int]testRow{1: str("a"), 2: str("b"), 3: str("c"), 4: str("d")}, base)
	require.Len(t, mustTips(t, db, ref), 2)

	var conflicts []int
	merged, err := MergeTips(ctx, db, ours, theirs, mhKd, mhVd, policyCollide("fail", &conflicts))
	require.NoError(t, err)
	assert.Empty(t, conflicts, "disjoint changes do not conflict")
	assert.Equal(t, map[int]testRow{1: str("A"), 2: str("b"), 3: str("c"), 4: str("d")}, stateFromMap(t, merged))

	mergeAddr := appendMap2(t, db, ref, merged, ours, theirs)
	tips := mustTips(t, db, ref)
	require.Len(t, tips, 1, "committing the merge collapses the frontier")
	assert.Equal(t, mergeAddr, tips[0])
}

// appendMap2 commits an already-merged prolly.Map as the collapsing tip.
func appendMap2(t *testing.T, db *database, ref string, m prolly.Map, parents ...hash.Hash) hash.Hash {
	t.Helper()
	addr, err := AppendMapCommit(context.Background(), db, ref, m,
		CommitOptions{Parents: parents, Meta: pinnedMeta()})
	require.NoError(t, err)
	return addr
}

// An update/update collision is resolved by Dolt's merge under each policy; the
// conflict key is reported; "fail" lets the caller refuse.
func TestReconcile_ConflictUpdateUpdate(t *testing.T) {
	ctx := context.Background()
	const ref = "refs/heads/main"

	run := func(policy string) map[int]testRow {
		db := newMHDatabase(t)
		base := appendMap(t, db, ref, map[int]testRow{2: str("b")})
		ours := appendMap(t, db, ref, map[int]testRow{2: str("O")}, base)
		theirs := appendMap(t, db, ref, map[int]testRow{2: str("T")}, base)

		var conflicts []int
		merged, err := MergeTips(ctx, db, ours, theirs, mhKd, mhVd, policyCollide(policy, &conflicts))
		require.NoError(t, err)
		assert.Equal(t, []int{2}, conflicts, "%s: key 2 conflicts", policy)
		return stateFromMap(t, merged)
	}

	assert.Equal(t, map[int]testRow{2: str("O")}, run("ours"))
	assert.Equal(t, map[int]testRow{2: str("O")}, run("record"))
	assert.Equal(t, map[int]testRow{2: str("T")}, run("theirs"))
	// "fail" produces a merged map too, but the caller refuses on conflicts:
	assert.Equal(t, map[int]testRow{2: str("O")}, run("fail"))
}

// Update-vs-delete is a conflict; "theirs" keeps the update, "ours" keeps the
// delete (row absent).
func TestReconcile_UpdateVsDelete(t *testing.T) {
	ctx := context.Background()
	const ref = "refs/heads/main"

	run := func(policy string) map[int]testRow {
		db := newMHDatabase(t)
		base := appendMap(t, db, ref, map[int]testRow{1: str("a"), 2: str("b")})
		ours := appendMap(t, db, ref, map[int]testRow{1: str("a"), 2: str("O")}, base) // update 2
		theirs := appendMap(t, db, ref, map[int]testRow{1: str("a")}, base)            // delete 2

		var conflicts []int
		merged, err := MergeTips(ctx, db, ours, theirs, mhKd, mhVd, policyCollide(policy, &conflicts))
		require.NoError(t, err)
		assert.Equal(t, []int{2}, conflicts)
		return stateFromMap(t, merged)
	}

	assert.Equal(t, map[int]testRow{1: str("a"), 2: str("O")}, run("ours"))
	assert.Equal(t, map[int]testRow{1: str("a")}, run("theirs"), "delete wins under theirs")
}

// A no common ancestor merges against an empty base (two independent roots).
func TestReconcile_NoCommonAncestorUsesEmptyBase(t *testing.T) {
	db := newMHDatabase(t)
	ctx := context.Background()
	const ref = "refs/heads/main"

	// Two roots with disjoint keys and no shared parent.
	ours := appendMap(t, db, ref, map[int]testRow{1: str("a")})
	theirs := appendMap(t, db, ref, map[int]testRow{2: str("b")})

	var conflicts []int
	merged, err := MergeTips(ctx, db, ours, theirs, mhKd, mhVd, policyCollide("fail", &conflicts))
	require.NoError(t, err)
	assert.Empty(t, conflicts)
	assert.Equal(t, map[int]testRow{1: str("a"), 2: str("b")}, stateFromMap(t, merged))
}
