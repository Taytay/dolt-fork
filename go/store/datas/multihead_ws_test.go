// Copyright 2026 Dolthub, Inc.
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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// CommitWithWorkingSet is the write path the CLI/SQL stack uses (dolt commit ->
// doltdb.CommitWithWorkingSet). In multi-head mode it records the commit as a
// tip (no head CAS) while updating the working set atomically, so sequential
// commits fast-forward and a commit off a stale head forks instead of returning
// ErrMergeNeeded.
func TestMultiheadCommitWithWorkingSet(t *testing.T) {
	db := newMHDatabase(t)
	require.NoError(t, EnableMultiheadResolve(db))
	ctx := context.Background()
	const branch = "refs/heads/main"
	const wsRef = "refs/workingSets/heads/main"

	// A minimal, stable working-set spec (content-addressed, so its address is
	// constant across commits — we thread it as prevWsHash).
	r, err := db.WriteValue(ctx, types.String("root"))
	require.NoError(t, err)
	rootRef, err := types.ToRefOfValue(r, db.Format())
	require.NoError(t, err)
	spec := WorkingSetSpec{
		Meta:        &WorkingSetMeta{Name: "t", Email: "t@t", Timestamp: 0},
		WorkingRoot: rootRef,
		StagedRoot:  rootRef,
	}

	commit := func(commitDS, wsDS Dataset, val string, prevWs hash.Hash) (Dataset, Dataset) {
		t.Helper()
		c, w, err := db.CommitWithWorkingSet(ctx, commitDS, wsDS, types.String(val), spec, prevWs,
			CommitOptions{Meta: pinnedMeta()})
		require.NoError(t, err)
		return c, w
	}
	addrOf := func(ds Dataset) hash.Hash {
		a, ok := ds.MaybeHeadAddr()
		require.True(t, ok)
		return a
	}

	// Commit 1 (from a headless ref).
	c0, err := db.GetDataset(ctx, branch)
	require.NoError(t, err)
	w0, err := db.GetDataset(ctx, wsRef)
	require.NoError(t, err)
	c1, w1 := commit(c0, w0, "r1", hash.Hash{})
	t1 := addrOf(c1)
	require.Equal(t, []hash.Hash{t1}, mustTips(t, db, branch), "first commit is the sole tip")
	wsAddr := addrOf(w1)

	// Commit 2 (sequential, from the current head) fast-forwards: still one tip.
	c1Fresh, err := db.GetDataset(ctx, branch)
	require.NoError(t, err)
	c2, w2 := commit(c1Fresh, w1, "r2", wsAddr)
	t2 := addrOf(c2)
	require.Equal(t, []hash.Hash{t2}, mustTips(t, db, branch), "sequential commit fast-forwards")
	wsAddr2 := addrOf(w2)

	// Commit 3 from the STALE handle c1 (head is t2 now). No ErrMergeNeeded — it
	// forks: t2 and t3 are both children of t1, which is superseded.
	c3, _ := commit(c1, w2, "r3", wsAddr2)
	t3 := addrOf(c3)
	tips := mustTips(t, db, branch)
	require.Len(t, tips, 2, "a commit off a stale head forks with no ErrMergeNeeded")
	assert.ElementsMatch(t, []hash.Hash{t2, t3}, tips)

	// Reads still resolve (canonical head) rather than erroring.
	ds, err := db.GetDataset(ctx, branch)
	require.NoError(t, err)
	head := addrOf(ds)
	assert.Equal(t, tips[0], head, "resolve mode returns the canonical tip")

	// The working set ref resolves to a single head (no tips of its own).
	wsDS, err := db.GetDataset(ctx, wsRef)
	require.NoError(t, err)
	assert.Equal(t, wsAddr2, addrOf(wsDS))
}
