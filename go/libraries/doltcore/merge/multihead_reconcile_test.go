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

// Multi-head reconcile through Dolt's FULL relational merge engine.
//
// The CAS-free multi-head work (MULTIHEAD.md, and experiments/multihead-dolt-nbs
// in taytays_stuff) makes a ref hold a set of tips; Step 2b reconciled a fork
// with prolly.MergeMaps — Dolt's real *tree-level* three-way merge — but that
// merges ONE keyed map. A real dataset tip is a whole RootValue: many tables,
// their schemas, secondary indexes, and constraints. Composing the per-map merge
// across a RootValue is exactly what libraries/doltcore/merge.MergeRoots does
// (it is what `dolt merge` calls). store/datas cannot import doltcore (the layer
// runs the other way), so this — the SQL/relational merge engine tier — is where
// the multi-head fork meets the full engine.
//
// These tests frame `ours` and `theirs` as two tips of one forked ref and
// reconcile them with the real MergeRoots, asserting the merged RootValue with
// the package's own verifyMerge. They cover the fidelity prolly.MergeMaps alone
// cannot: MULTIPLE TABLES in one root, a SCHEMA change (add column) on one side,
// and a row-level CONFLICT recorded into the merged root's conflict tables.
package merge_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// mhRootWithTables builds a RootValue containing several tables, so a multi-head
// tip can be a whole multi-table root (not just one map). It mirrors
// makeRootWithTable but puts every table into a single working root and flushes
// once, so all tables share the one root's storage.
func mhRootWithTables(t *testing.T, ddb *doltdb.DoltDB, eo editor.Options, tbls ...table) doltdb.RootValue {
	ctx := context.Background()
	wsr, err := ref.WorkingSetRefForHead(ref.NewBranchRef("main"))
	require.NoError(t, err)
	ws, err := ddb.ResolveWorkingSet(ctx, wsr)
	require.NoError(t, err)

	root := ws.WorkingRoot()
	for _, tbl := range tbls {
		dt, err := doltdb.NewEmptyTable(ctx, ddb.ValueReadWriter(), ddb.NodeStore(), tbl.ns.sch)
		require.NoError(t, err)
		root, err = root.PutTable(ctx, doltdb.TableName{Name: tbl.ns.name}, dt)
		require.NoError(t, err)
	}
	ws = ws.WithWorkingRoot(root)

	gst, err := dsess.NewAutoIncrementTracker(ctx, "dolt", ws)
	require.NoError(t, err)
	noop := func(ctx *sql.Context, dbName string, root doltdb.RootValue) (err error) { return }
	sess := writer.NewWriteSession("test", ws, gst, noop, eo)

	sctx := sql.NewEmptyContext()
	for _, tbl := range tbls {
		wr, err := sess.GetTableWriter(sql.NewContext(ctx), doltdb.TableName{Name: tbl.ns.name})
		require.NoError(t, err)
		columns := tbl.ns.sch.GetAllCols().GetColumns()
		for _, r := range tbl.rows {
			for i, column := range columns {
				r[i], _, err = column.TypeInfo.ToSqlType().Convert(ctx, r[i])
				require.NoError(t, err)
			}
			require.NoError(t, wr.Insert(sctx, r))
		}
	}

	ws, err = sess.Flush(sql.NewContext(ctx))
	require.NoError(t, err)
	return ws.WorkingRoot()
}

// reconcileTips runs Dolt's real three-way merge over two multi-head tips
// (`ours`, `theirs`) against their common ancestor `base` — the RootValue
// analogue of datas.MergeTips, but through the full engine. It returns the
// merged Result.
func reconcileTips(t *testing.T, ctx context.Context, base, ours, theirs doltdb.RootValue) *merge.Result {
	var mo merge.MergeOpts
	var eo editor.Options
	result, err := merge.MergeRoots(
		sql.NewContext(ctx),
		doltdb.SimpleTableResolver{},
		ours, theirs, base,
		rootish{theirs}, rootish{base},
		eo, mo,
	)
	require.NoError(t, err)
	return result
}

// A multi-head fork over TWO tables, with disjoint edits on each side, merges
// cleanly through the full engine: ours edits t1, theirs edits t2, and the
// merged root carries both. This is the whole-RootValue composition that a
// single prolly.MergeMaps cannot do.
func TestMultiheadReconcile_MultiTableDisjointClean(t *testing.T) {
	ctx := context.Background()
	denv := dtestutils.CreateTestEnv()
	ddb := denv.DoltDB(ctx)
	var eo editor.Options

	t1 := sch("CREATE TABLE t1 (id int PRIMARY KEY, v int)")
	t2 := sch("CREATE TABLE t2 (id int PRIMARY KEY, v int)")

	// The forked base: two tables, one row each.
	base := mhRootWithTables(t, ddb, eo,
		*tbl(t1, row(1, 10)),
		*tbl(t2, row(1, 100)),
	)
	// ours: appends a row to t1, leaves t2.
	ours := mhRootWithTables(t, ddb, eo,
		*tbl(t1, row(1, 10), row(2, 20)),
		*tbl(t2, row(1, 100)),
	)
	// theirs: appends a row to t2, leaves t1.
	theirs := mhRootWithTables(t, ddb, eo,
		*tbl(t1, row(1, 10)),
		*tbl(t2, row(1, 100), row(2, 200)),
	)
	// Expected reconciled state: both appends present.
	expected := mhRootWithTables(t, ddb, eo,
		*tbl(t1, row(1, 10), row(2, 20)),
		*tbl(t2, row(1, 100), row(2, 200)),
	)

	result := reconcileTips(t, ctx, base, ours, theirs)
	verifyMerge(t, ctx, expected, result, ddb.NodeStore(), false, nil)
}

// A multi-head fork where one tip ALSO changes the schema (adds a nullable
// column) while the other only edits rows: the full engine merges schema + data
// together. prolly.MergeMaps has no notion of schema; this is the fidelity Step
// 2b explicitly left to this tier.
func TestMultiheadReconcile_SchemaAddColumnOnOneSide(t *testing.T) {
	ctx := context.Background()
	denv := dtestutils.CreateTestEnv()
	ddb := denv.DoltDB(ctx)
	var eo editor.Options

	baseSch := sch("CREATE TABLE t (id int PRIMARY KEY, a int)")
	addColSch := sch("CREATE TABLE t (id int PRIMARY KEY, a int, b int)")

	base := mhRootWithTables(t, ddb, eo, *tbl(baseSch, row(1, 10)))
	// ours: add column b, set it on the existing row.
	ours := mhRootWithTables(t, ddb, eo, *tbl(addColSch, row(1, 10, 100)))
	// theirs: no schema change, append a row.
	theirs := mhRootWithTables(t, ddb, eo, *tbl(baseSch, row(1, 10), row(2, 20)))
	// Expected: new column present, ours' value kept, theirs' new row with b=NULL.
	expected := mhRootWithTables(t, ddb, eo, *tbl(addColSch, row(1, 10, 100), row(2, 20, nil)))

	result := reconcileTips(t, ctx, base, ours, theirs)
	verifyMerge(t, ctx, expected, result, ddb.NodeStore(), false, nil)
}

// A multi-head fork where both tips change the SAME row differently produces a
// data conflict, recorded into the merged root's conflict table — the engine's
// conflict machinery, one thing the tree-level merge could only signal via the
// CollisionFn.
func TestMultiheadReconcile_RowConflictRecorded(t *testing.T) {
	ctx := context.Background()
	denv := dtestutils.CreateTestEnv()
	ddb := denv.DoltDB(ctx)
	var eo editor.Options

	s := sch("CREATE TABLE t (id int PRIMARY KEY, v int)")

	base := mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 10)))
	ours := mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 11)))   // 10 -> 11
	theirs := mhRootWithTables(t, ddb, eo, *tbl(s, row(1, 12))) // 10 -> 12

	result := reconcileTips(t, ctx, base, ours, theirs)

	// The merged root records the conflict rather than silently picking a side.
	tbl, _, err := result.Root.GetTable(ctx, doltdb.TableName{Name: s.name})
	require.NoError(t, err)
	hasConflict, err := tbl.HasConflicts(ctx)
	require.NoError(t, err)
	assert.True(t, hasConflict, "an update/update on the same row must record a conflict")
}
