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

package doltdb_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// gcTestSchema is a minimal single-column schema for the multi-head GC test
// (this external test package cannot see the internal createTestSchema helper).
func gcTestSchema(t *testing.T) schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	return sch
}

// TestMultiheadGCKeepsFrontier is the regression guard for frontier-aware GC:
// when a branch is forked (a frontier of several tips, as multi-head push/fetch
// produces), every tip and its unique table data survive GC — not only the
// canonical (lowest-hash) tip that Datasets collapses the ref to — while a
// genuinely unreferenced commit is still collected.
//
// The guarantee holds on two levels. The underlying store already roots the raw
// manifest address map, which holds every tip as a content-addressed sub-key, so
// a divergent tip is reachable and never collected even without the frontier
// roots DoltDB.GC adds; those roots exist for correct generational placement (a
// fork's tips classified with the other branch heads) and to make the "a live
// fork is a GC root" invariant explicit. This test locks in the observable
// property either way: a fork survives GC intact.
func TestMultiheadGCKeepsFrontier(t *testing.T) {
	t.Setenv(dconfig.EnvMultihead, "1")
	ctx := context.Background()

	dir, err := os.MkdirTemp(t.TempDir(), "multihead-gc-")
	require.NoError(t, err)
	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, "file://"+dir, filesys.LocalFS)
	require.NoError(t, err)
	defer ddb.Close()
	require.True(t, ddb.IsMultihead(), "DOLT_MULTIHEAD must open the db in multi-head mode")

	require.NoError(t, ddb.WriteEmptyRepo(ctx, "main", "Test", "t@t.com"))
	mainRef := ref.NewBranchRef("main")

	// Base commit C0 (the fork point) and its empty root value.
	cs, err := doltdb.NewCommitSpec("main")
	require.NoError(t, err)
	optC0, err := ddb.Resolve(ctx, cs, nil)
	require.NoError(t, err)
	c0, ok := optC0.ToCommit()
	require.True(t, ok)
	baseRoot, err := c0.GetRootValue(ctx)
	require.NoError(t, err)

	sch := gcTestSchema(t)

	// commitTipWithTable builds a divergent child of C0 whose root carries a
	// uniquely-named table (so the tip has unique root/table chunks, not just a
	// unique commit chunk), records it as a tip of main, and returns its hash.
	commitTipWithTable := func(table, author, msg string) hash.Hash {
		root, err := doltdb.CreateEmptyTable(ctx, baseRoot, doltdb.TableName{Name: table}, sch)
		require.NoError(t, err)
		_, valHash, err := ddb.WriteRootValue(ctx, root)
		require.NoError(t, err)
		meta, err := datas.NewCommitMeta(author, author+"@t.com", msg)
		require.NoError(t, err)
		cm, err := ddb.CommitDanglingWithParentCommits(ctx, valHash, []*doltdb.Commit{c0}, meta)
		require.NoError(t, err)
		h, err := cm.HashOf()
		require.NoError(t, err)
		require.NoError(t, ddb.RecordTip(ctx, mainRef.String(), h))
		return h
	}

	tipA := commitTipWithTable("table_a", "A", "A change")
	tipB := commitTipWithTable("table_b", "B", "B change")
	require.NotEqual(t, tipA, tipB, "the two tips must be distinct commits")

	// A control: a dangling commit that is NOT recorded as a tip. It is
	// unreachable from any root, so a working GC must collect it — this proves
	// the test would actually catch a regression (GC really is collecting).
	orphanRoot, err := doltdb.CreateEmptyTable(ctx, baseRoot, doltdb.TableName{Name: "table_orphan"}, sch)
	require.NoError(t, err)
	_, orphanValHash, err := ddb.WriteRootValue(ctx, orphanRoot)
	require.NoError(t, err)
	orphanMeta, err := datas.NewCommitMeta("Orphan", "orphan@t.com", "unreferenced")
	require.NoError(t, err)
	orphanCm, err := ddb.CommitDanglingWithParentCommits(ctx, orphanValHash, []*doltdb.Commit{c0}, orphanMeta)
	require.NoError(t, err)
	orphanHash, err := orphanCm.HashOf()
	require.NoError(t, err)

	// main is forked: the frontier holds both tips (the orphan is not a tip).
	before, err := ddb.MultiheadTips(ctx, mainRef.String())
	require.NoError(t, err)
	require.ElementsMatch(t, []hash.Hash{tipA, tipB}, before, "branch should be forked before GC")

	gcConfig := chunks.GCConfig{
		Mode:                chunks.GCMode_Default,
		ArchiveLevel:        chunks.NoArchive,
		IncrementalFileSize: chunks.IncrementalGCTablesDisabled,
	}
	require.NoError(t, ddb.GC(ctx, gcConfig, purgingSafepointController{ddb}))

	// Both tips (and their unique table data) survive: the frontier is intact.
	after, err := ddb.MultiheadTips(ctx, mainRef.String())
	require.NoError(t, err)
	assert.ElementsMatch(t, []hash.Hash{tipA, tipB}, after, "GC must keep every frontier tip, not just the canonical one")

	for _, tip := range []hash.Hash{tipA, tipB} {
		optCmt, err := ddb.ReadCommit(ctx, tip)
		require.NoError(t, err, "frontier tip %s must survive GC", tip.String())
		cm, ok := optCmt.ToCommit()
		require.True(t, ok, "frontier tip %s must resolve to a commit after GC", tip.String())
		_, err = cm.GetRootValue(ctx)
		require.NoError(t, err, "frontier tip %s root value must survive GC", tip.String())
	}

	// The unreferenced orphan commit was collected (control).
	orphanCs, err := doltdb.NewCommitSpec(orphanHash.String())
	require.NoError(t, err)
	_, err = ddb.Resolve(ctx, orphanCs, nil)
	require.Error(t, err, "an unreferenced dangling commit must be collected by GC")
}
