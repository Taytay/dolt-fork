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

// End-to-end for merge.ReconcileFrontier: build a real forked branch (two tips
// off one base) and collapse it through the full engine, asserting the frontier
// becomes a single merged head with two parents. multihead_reconcile_test.go
// already covers the engine (MergeRoots over tips); this drives it against a
// live ref's frontier — the piece dolt_reconcile() calls.
package merge_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// commitRootAsTip writes root and records a commit for it (parents = parentCommits)
// as a tip of main, returning the commit. With no parents it is the base.
func commitRootAsTip(t *testing.T, ctx context.Context, ddb *doltdb.DoltDB, root doltdb.RootValue, desc string, parents ...*doltdb.Commit) *doltdb.Commit {
	t.Helper()
	_, valHash, err := ddb.WriteRootValue(ctx, root)
	require.NoError(t, err)
	meta, err := datas.NewCommitMeta("Test", "t@t.com", desc)
	require.NoError(t, err)

	mainRef := ref.NewBranchRef("main")
	if len(parents) == 0 {
		cm, err := ddb.CommitWithParentCommits(ctx, valHash, mainRef, nil, meta)
		require.NoError(t, err)
		return cm
	}
	// Divergent tips: build dangling (so it is not auto-parented on the current
	// head) then record it as a tip of main.
	cm, err := ddb.CommitDanglingWithParentCommits(ctx, valHash, parents, meta)
	require.NoError(t, err)
	h, err := cm.HashOf()
	require.NoError(t, err)
	require.NoError(t, ddb.RecordTip(ctx, mainRef.String(), h))
	return cm
}

func mhTestDB(t *testing.T) (*doltdb.DoltDB, *sql.Context) {
	t.Helper()
	t.Setenv(dconfig.EnvMultihead, "1")
	denv := dtestutils.CreateTestEnv()
	ddb := denv.DoltDB(context.Background())
	require.True(t, ddb.IsMultihead(), "test DB must open in multi-head mode with DOLT_MULTIHEAD set")
	return ddb, sql.NewContext(context.Background())
}

// A two-tip fork with disjoint row edits collapses to one merged head that names
// both tips as parents, with no conflicts.
func TestReconcileFrontier_DisjointCollapsesToOneHead(t *testing.T) {
	ddb, ctx := mhTestDB(t)
	var eo editor.Options
	mainRef := ref.NewBranchRef("main")

	s := sch("CREATE TABLE t (id int PRIMARY KEY, v int)")
	base := commitRootAsTip(t, ctx, ddb, mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 10))), "base")
	// ours adds row 2; theirs adds row 3 — both children of base.
	commitRootAsTip(t, ctx, ddb, mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 10), row(2, 20))), "ours", base)
	commitRootAsTip(t, ctx, ddb, mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 10), row(3, 30))), "theirs", base)

	// The branch is forked: two tips.
	tips, err := ddb.MultiheadTips(ctx, mainRef.String())
	require.NoError(t, err)
	require.Len(t, tips, 2, "branch should be forked before reconcile")

	meta, err := datas.NewCommitMeta("Test", "t@t.com", "reconcile")
	require.NoError(t, err)
	res, err := merge.ReconcileFrontier(ctx, ddb, mainRef, meta, eo)
	require.NoError(t, err)
	assert.Equal(t, 2, res.TipsBefore)
	assert.False(t, res.HasConflicts(), "disjoint edits merge cleanly")

	// The frontier collapsed to the single merged head.
	after, err := ddb.MultiheadTips(ctx, mainRef.String())
	require.NoError(t, err)
	require.Len(t, after, 1, "reconcile collapses the fork to one head")
	assert.Equal(t, res.Merged, after[0])

	// The merged head is a real merge commit: two parents.
	mergedCommit := mustReadCommit(t, ctx, ddb, res.Merged)
	assert.Equal(t, 2, mergedCommit.NumParents(), "merged head names both tips as parents")

	// Reconcile again is a no-op: one head, nothing to merge.
	res2, err := merge.ReconcileFrontier(ctx, ddb, mainRef, meta, eo)
	require.NoError(t, err)
	assert.Equal(t, 1, res2.TipsBefore)
	assert.Equal(t, res.Merged, res2.Merged, "already reconciled: sole tip unchanged")
}

// A two-tip fork that edits the SAME row differently still collapses to one
// head, and the reconcile reports the recorded conflict.
func TestReconcileFrontier_ConflictRecordedStillCollapses(t *testing.T) {
	ddb, ctx := mhTestDB(t)
	var eo editor.Options
	mainRef := ref.NewBranchRef("main")

	s := sch("CREATE TABLE t (id int PRIMARY KEY, v int)")
	base := commitRootAsTip(t, ctx, ddb, mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 10))), "base")
	commitRootAsTip(t, ctx, ddb, mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 11))), "ours", base)   // 10 -> 11
	commitRootAsTip(t, ctx, ddb, mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 12))), "theirs", base) // 10 -> 12

	meta, err := datas.NewCommitMeta("Test", "t@t.com", "reconcile")
	require.NoError(t, err)
	res, err := merge.ReconcileFrontier(ctx, ddb, mainRef, meta, eo)
	require.NoError(t, err)
	assert.Equal(t, 2, res.TipsBefore)
	assert.True(t, res.DataConflicts > 0, "same-row edits must record a data conflict")

	after, err := ddb.MultiheadTips(ctx, mainRef.String())
	require.NoError(t, err)
	require.Len(t, after, 1, "reconcile collapses the fork even when a conflict is recorded")
	assert.Equal(t, res.Merged, after[0])

	// The conflict is recorded in the merged root's conflict table.
	mergedRoot, err := mustReadCommit(t, ctx, ddb, res.Merged).GetRootValue(ctx)
	require.NoError(t, err)
	tbl, _, err := mergedRoot.GetTable(ctx, doltdb.TableName{Name: "t"})
	require.NoError(t, err)
	hasConflict, err := tbl.HasConflicts(ctx)
	require.NoError(t, err)
	assert.True(t, hasConflict, "merged root records the row conflict")
}

func mustReadCommit(t *testing.T, ctx context.Context, ddb *doltdb.DoltDB, h hash.Hash) *doltdb.Commit {
	t.Helper()
	optCmt, err := ddb.ReadCommit(ctx, h)
	require.NoError(t, err)
	cm, ok := optCmt.ToCommit()
	require.True(t, ok)
	return cm
}
