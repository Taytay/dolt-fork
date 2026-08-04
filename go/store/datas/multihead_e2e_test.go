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

	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

// TestMultihead_SharedFolderReconcileEndToEnd is the relational end-to-end of
// "one folder, two writers, a head each, reconcile later" — on a REAL on-disk
// store, driven through the ordinary multi-head Commit/GetDataset API (Step 2c)
// and reconciled with Dolt's real merge (Step 2b).
//
// A single on-disk folder backs the store. Two writers each derive a divergent
// commit from a common base and commit through the multi-head primary path — no
// ErrMergeNeeded, no CAS on the ref — so the folder ends with a two-tip frontier
// that GetDataset reports as ErrMultipleHeads. A reconciler then three-way
// merges the tips with prolly.MergeMaps and records the merged tip, collapsing
// the fork back to a single head reachable through the same API.
//
// Concurrency honesty (same as the nbs sibling): stock nbs takes an exclusive
// manifest lock per open, so the writers take turns (open -> commit -> close),
// modelling a synced folder rather than simultaneous handles. What is genuinely
// coordination-free is the multi-head semantics: a divergent commit adds a head
// instead of being rejected, and both heads survive on the shared folder to be
// reconciled later. Full concurrent, lock-free writes need the "flip the nbs
// default" follow-on (roots/ as the only root layer).
// mhE2EMemTableSize is a small memtable budget for the on-disk store in this
// demo; chunks here are tiny so it only needs to be nonzero.
const mhE2EMemTableSize = 1 << 20

func TestMultihead_SharedFolderReconcileEndToEnd(t *testing.T) {
	ctx := context.Background()
	shared := t.TempDir()
	const ref = "refs/heads/main"

	open := func() (*database, func()) {
		st, err := nbs.NewLocalStore(ctx, constants.FormatDefaultString, shared, mhE2EMemTableSize, nbs.NewUnlimitedMemQuotaProvider(), false)
		require.NoError(t, err)
		db := NewDatabase(st).(*database)
		require.NoError(t, EnableMultihead(db)) // multi-head is the primary path for this store
		return db, func() { require.NoError(t, st.Close()) }
	}

	// commitTable commits a table (a real prolly.Map) through the primary
	// multi-head Commit and returns the new tip's address.
	commitTable := func(db *database, ds Dataset, s map[int]testRow, parents ...hash.Hash) hash.Hash {
		m := mapFromState(t, db, s)
		ds2, err := db.Commit(ctx, ds, tree.ValueFromNode(m.Node()),
			CommitOptions{Parents: parents, Meta: pinnedMeta()})
		require.NoError(t, err)
		addr, ok := ds2.MaybeHeadAddr()
		require.True(t, ok)
		return addr
	}

	// Session 0: bootstrap a shared base through the primary Commit API.
	db0, close0 := open()
	ds0, err := db0.GetDataset(ctx, ref)
	require.NoError(t, err)
	require.False(t, ds0.HasHead())
	base := commitTable(db0, ds0, map[int]testRow{1: str("a"), 2: str("b")})
	close0()

	// Writer A: opens the shared folder (head == base) and commits a child of
	// base. The parent defaults to the observed head, so this fast-forwards.
	dbA, closeA := open()
	dsA, err := dbA.GetDataset(ctx, ref)
	require.NoError(t, err)
	baseAddr, ok := dsA.MaybeHeadAddr()
	require.True(t, ok)
	require.Equal(t, base, baseAddr, "writer A sees the shared base")
	ours := commitTable(dbA, dsA, map[int]testRow{1: str("A"), 2: str("b")})
	closeA()

	// Writer B: had synced at base and edited offline. It commits a child of
	// base (explicit parent), diverging from writer A's head. No ErrMergeNeeded —
	// the divergent commit adds a second tip.
	dbB, closeB := open()
	dsB, err := dbB.GetDataset(ctx, ref) // frontier is [ours] now; single tip resolves fine
	require.NoError(t, err)
	theirs := commitTable(dbB, dsB, map[int]testRow{1: str("a"), 2: str("b"), 3: str("c")}, base)
	closeB()

	// Reconciler: the shared folder shows a live fork.
	dbR, closeR := open()
	defer closeR()
	_, err = dbR.GetDataset(ctx, ref)
	require.ErrorIs(t, err, ErrMultipleHeads, "two writers -> a live fork on the shared folder")
	tips := mustTips(t, dbR, ref)
	require.Len(t, tips, 2)
	require.ElementsMatch(t, []hash.Hash{ours, theirs}, tips)

	// Reconcile with Dolt's real three-way merge; A changed key 1, B added key 3,
	// so the merge is clean.
	var conflicts []int
	merged, err := MergeTips(ctx, dbR, ours, theirs, mhKd, mhVd, policyCollide("record", &conflicts))
	require.NoError(t, err)
	assert.Empty(t, conflicts, "disjoint edits do not conflict")
	assert.Equal(t, map[int]testRow{1: str("A"), 2: str("b"), 3: str("c")}, stateFromMap(t, merged))

	// Record the merged tip; the fork collapses to one head, reachable again
	// through the primary GetDataset API.
	mergeAddr, err := AppendMapCommit(ctx, dbR, ref, merged,
		CommitOptions{Parents: []hash.Hash{ours, theirs}, Meta: pinnedMeta()})
	require.NoError(t, err)

	got, err := dbR.GetDataset(ctx, ref)
	require.NoError(t, err, "the reconciled folder resolves to a single head")
	addr, ok := got.MaybeHeadAddr()
	require.True(t, ok)
	assert.Equal(t, mergeAddr, addr)
	require.Len(t, mustTips(t, dbR, ref), 1, "the merge collapsed the frontier")
}
