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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// pinnedMeta gives commits fixed dates so that an identical (value, parents)
// builds a byte-identical commit — the precondition for AppendCommit being
// idempotent.
func pinnedMeta() *CommitMeta {
	epoch := CommitDateAt(time.UnixMilli(0))
	return &CommitMeta{Author: CommitIdent{Date: epoch}, Committer: CommitIdent{Date: epoch}}
}

func newMHDatabase(t *testing.T) *database {
	storage := &chunks.TestStorage{}
	return NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
}

func mustAppend(t *testing.T, db *database, ref, val string, parents ...hash.Hash) hash.Hash {
	t.Helper()
	addr, err := AppendCommit(context.Background(), db, ref, types.String(val),
		CommitOptions{Parents: parents, Meta: pinnedMeta()})
	require.NoError(t, err)
	return addr
}

func mustTips(t *testing.T, db *database, ref string) []hash.Hash {
	t.Helper()
	tips, err := Tips(context.Background(), db, ref)
	require.NoError(t, err)
	return tips
}

func committedString(t *testing.T, db *database, commitAddr hash.Hash) string {
	t.Helper()
	ctx := context.Background()
	cv, err := db.ReadValue(ctx, commitAddr)
	require.NoError(t, err)
	v, err := GetCommittedValue(ctx, db, cv)
	require.NoError(t, err)
	return string(v.(types.String))
}

// A fork produces two coexisting tips with NO ErrMergeNeeded — the anti-CAS
// property. Stock doCommit would reject the second divergent commit; the
// multi-tip path records both.
func TestMultiheadDatasets_ForkProducesTwoTipsNoMergeNeeded(t *testing.T) {
	db := newMHDatabase(t)
	const ref = "refs/heads/main"

	base := mustAppend(t, db, ref, "base")
	require.Len(t, mustTips(t, db, ref), 1, "one tip after the base commit")

	// Two divergent children of the same base. Neither call errors: a
	// divergent commit adds a tip instead of returning ErrMergeNeeded.
	mainH := mustAppend(t, db, ref, "main-edit", base)
	branchH := mustAppend(t, db, ref, "branch-edit", base)

	tips := mustTips(t, db, ref)
	require.Len(t, tips, 2, "a fork leaves two tips")
	assert.ElementsMatch(t, []hash.Hash{mainH, branchH}, tips)
	assert.NotContains(t, tips, base, "the shared base is superseded by both children")
}

// A linear chain (fast-forward) always collapses to a single tip.
func TestMultiheadDatasets_FastForwardKeepsOneTip(t *testing.T) {
	db := newMHDatabase(t)
	const ref = "refs/heads/main"

	base := mustAppend(t, db, ref, "base")
	c1 := mustAppend(t, db, ref, "c1", base)
	c2 := mustAppend(t, db, ref, "c2", c1)

	tips := mustTips(t, db, ref)
	require.Len(t, tips, 1, "a linear chain has one tip")
	assert.Equal(t, c2, tips[0])
	assert.Equal(t, "c2", committedString(t, db, tips[0]))
}

// A merge commit naming both tips as parents collapses the frontier.
func TestMultiheadDatasets_MergeCollapsesFrontier(t *testing.T) {
	db := newMHDatabase(t)
	const ref = "refs/heads/main"

	base := mustAppend(t, db, ref, "base")
	mainH := mustAppend(t, db, ref, "main-edit", base)
	branchH := mustAppend(t, db, ref, "branch-edit", base)
	require.Len(t, mustTips(t, db, ref), 2)

	merge := mustAppend(t, db, ref, "merged", mainH, branchH)

	tips := mustTips(t, db, ref)
	require.Len(t, tips, 1, "the merge collapses the fork")
	assert.Equal(t, merge, tips[0])
}

// Appending the same commit, or recording the same tip, is idempotent: no
// duplicate head appears.
func TestMultiheadDatasets_AppendIsIdempotent(t *testing.T) {
	db := newMHDatabase(t)
	ctx := context.Background()
	const ref = "refs/heads/main"

	base := mustAppend(t, db, ref, "base")
	again := mustAppend(t, db, ref, "base") // identical value, parents, pinned meta
	assert.Equal(t, base, again, "identical content builds the same commit")
	require.Len(t, mustTips(t, db, ref), 1, "no duplicate tip")

	// RecordTip of an existing tip is a no-op too.
	require.NoError(t, RecordTip(ctx, db, ref, base))
	require.Len(t, mustTips(t, db, ref), 1)
}

// Separate refs keep separate frontiers.
func TestMultiheadDatasets_RefsAreIsolated(t *testing.T) {
	db := newMHDatabase(t)

	a := mustAppend(t, db, "refs/heads/a", "a-base")
	b1 := mustAppend(t, db, "refs/heads/b", "b-base")
	b2 := mustAppend(t, db, "refs/heads/b", "b-next", b1)

	assert.Equal(t, []hash.Hash{a}, mustTips(t, db, "refs/heads/a"))
	assert.Equal(t, []hash.Hash{b2}, mustTips(t, db, "refs/heads/b"))
}

// Invariant #4: the single-root commit path is unchanged — a stale,
// non-fast-forward commit still returns ErrMergeNeeded. The multi-tip layer
// sits alongside it and does not relax the legacy gate.
func TestMultiheadDatasets_SingleRootPathUnaffected(t *testing.T) {
	db := newMHDatabase(t)
	ctx := context.Background()
	const ref = "refs/heads/legacy"

	dsA, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	dsA, err = db.Commit(ctx, dsA, types.String("a"), CommitOptions{Meta: pinnedMeta()})
	require.NoError(t, err)
	aAddr, ok := dsA.MaybeHeadAddr()
	require.True(t, ok)

	// Fast-forward a -> b through a fresh handle succeeds.
	dsB, err := db.GetDataset(ctx, ref)
	require.NoError(t, err)
	_, err = db.Commit(ctx, dsB, types.String("b"), CommitOptions{Parents: []hash.Hash{aAddr}, Meta: pinnedMeta()})
	require.NoError(t, err)

	// The stale handle (still at a) tries a divergent commit: still rejected.
	_, err = db.Commit(ctx, dsA, types.String("c"), CommitOptions{Parents: []hash.Hash{aAddr}, Meta: pinnedMeta()})
	require.ErrorIs(t, err, ErrMergeNeeded, "single-root CAS must still reject a non-fast-forward")
}
