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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// newMHPrimaryDatabase returns a database with the primary multi-head path on.
func newMHPrimaryDatabase(t *testing.T) *database {
	db := newMHDatabase(t)
	require.NoError(t, EnableMultihead(db))
	require.True(t, IsMultihead(db))
	return db
}

func mustCommit(t *testing.T, db *database, ds Dataset, val string, parents ...hash.Hash) Dataset {
	t.Helper()
	ds, err := db.Commit(context.Background(), ds, types.String(val),
		CommitOptions{Parents: parents, Meta: pinnedMeta()})
	require.NoError(t, err)
	return ds
}

// In multi-head mode a divergent commit through the ORDINARY Commit API adds a
// head instead of returning ErrMergeNeeded, and GetDataset then reports the
// fork with ErrMultipleHeads. This is the anti-CAS property on the primary
// write path — the Step 2c goal.
func TestMultiheadPrimary_ForkViaNormalCommit(t *testing.T) {
	db := newMHPrimaryDatabase(t)
	ctx := context.Background()
	const ref = "refs/heads/main"

	// A fresh ref is headless, then a normal Commit establishes the base.
	ds, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	require.False(t, ds.HasHead(), "a fresh ref has no head")
	ds = mustCommit(t, db, ds, "base")
	baseAddr, ok := ds.MaybeHeadAddr()
	require.True(t, ok)

	// Two handles observe the same base, then both commit. Neither errors:
	// stock doCommit would reject the second with ErrMergeNeeded.
	h1, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	h2, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)

	dsMain := mustCommit(t, db, h1, "main-edit")
	dsBranch := mustCommit(t, db, h2, "branch-edit")

	mainAddr, _ := dsMain.MaybeHeadAddr()
	branchAddr, _ := dsBranch.MaybeHeadAddr()
	assert.NotEqual(t, mainAddr, branchAddr, "the two commits diverge")

	// The ref now has a live fork: two tips, base superseded.
	tips := mustTips(t, db, ref)
	require.Len(t, tips, 2, "a fork leaves two tips")
	assert.ElementsMatch(t, []hash.Hash{mainAddr, branchAddr}, tips)
	assert.NotContains(t, tips, baseAddr)

	// GetDataset surfaces the fork.
	_, err = db.GetDataset(ctx, ref)
	require.ErrorIs(t, err, ErrMultipleHeads, "a forked ref cannot resolve to one head")
}

// Sequential commits through fresh handles fast-forward: the frontier stays a
// single tip and GetDataset returns the latest commit, exactly as a single-root
// ref would — a multi-head database is indistinguishable from stock when
// writers do not diverge.
func TestMultiheadPrimary_SequentialCommitFastForwards(t *testing.T) {
	db := newMHPrimaryDatabase(t)
	ctx := context.Background()
	const ref = "refs/heads/main"

	ds, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	ds = mustCommit(t, db, ds, "c1")
	ds = mustCommit(t, db, ds, "c2")
	ds = mustCommit(t, db, ds, "c3")

	require.Len(t, mustTips(t, db, ref), 1, "a linear chain has one tip")

	got, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	require.True(t, got.HasHead())
	addr, _ := got.MaybeHeadAddr()
	assert.Equal(t, "c3", committedString(t, db, addr), "GetDataset returns the latest head")
	dsAddr, _ := ds.MaybeHeadAddr()
	assert.Equal(t, dsAddr, addr, "GetDataset and the commit handle agree on the head")
}

// After a fork, recording a merge commit that names both tips collapses the
// frontier, and GetDataset resolves to that single head again — reconcile
// clears ErrMultipleHeads on the primary path.
func TestMultiheadPrimary_ReconcileCollapsesToSingleHead(t *testing.T) {
	db := newMHPrimaryDatabase(t)
	ctx := context.Background()
	const ref = "refs/heads/main"

	ds, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	ds = mustCommit(t, db, ds, "base")

	h1, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	h2, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	mainAddr, _ := mustCommit(t, db, h1, "main-edit").MaybeHeadAddr()
	branchAddr, _ := mustCommit(t, db, h2, "branch-edit").MaybeHeadAddr()

	_, err = db.GetDataset(ctx, ref)
	require.ErrorIs(t, err, ErrMultipleHeads)

	// Reconcile: append a commit naming both tips (the merge result).
	mergeAddr := mustAppend(t, db, ref, "merged", mainAddr, branchAddr)
	require.Len(t, mustTips(t, db, ref), 1, "the merge collapses the fork")

	got, err := db.GetDataset(ctx, ref)
	require.NoError(t, err, "a reconciled ref resolves to one head again")
	addr, ok := got.MaybeHeadAddr()
	require.True(t, ok)
	assert.Equal(t, mergeAddr, addr)
	assert.Equal(t, "merged", committedString(t, db, addr))
}

// The mode is off by default: a database opened normally keeps the single-root
// CAS, so a divergent commit is still rejected with ErrMergeNeeded. This guards
// invariant #4 — multi-head is opt-in and never regresses the stock path.
func TestMultiheadPrimary_DefaultModeKeepsCAS(t *testing.T) {
	db := newMHDatabase(t) // NOT multi-head
	require.False(t, IsMultihead(db))
	ctx := context.Background()
	const ref = "refs/heads/main"

	ds, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	ds = mustCommit(t, db, ds, "a")
	aAddr, _ := ds.MaybeHeadAddr()

	// Fast-forward through a fresh handle is fine.
	fresh, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	_, err = db.Commit(ctx, fresh, types.String("b"), CommitOptions{Parents: []hash.Hash{aAddr}, Meta: pinnedMeta()})
	require.NoError(t, err)

	// The stale handle's divergent commit is rejected.
	_, err = db.Commit(ctx, ds, types.String("c"), CommitOptions{Parents: []hash.Hash{aAddr}, Meta: pinnedMeta()})
	require.ErrorIs(t, err, ErrMergeNeeded)
}
