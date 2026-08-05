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

package dprocedures

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// doltReconcileSchema is the result of dolt_reconcile(): the ref's single head
// after reconcile, how many tips were collapsed, whether the merge recorded
// conflicts, and a human-readable message.
var doltReconcileSchema = []*sql.Column{
	{Name: "hash", Type: gmstypes.LongText, Nullable: true},
	{Name: "tips", Type: gmstypes.Int64, Nullable: false},
	{Name: "conflicts", Type: gmstypes.Int64, Nullable: false},
	{Name: "message", Type: gmstypes.LongText, Nullable: true},
}

// doltReconcile is the stored procedure that collapses the current branch's
// multi-head frontier into a single head. It is the frontier analogue of
// `dolt merge`: where `dolt merge <branch>` merges another branch into the
// current one, dolt_reconcile() merges the current branch's own divergent tips
// (a fork produced by two writers off a common base, brought together by
// multi-head push/fetch) back into one head, using the same three-way merge
// engine. Meaningful only in multi-head mode (DOLT_MULTIHEAD).
//
// With an optional ref argument — dolt_reconcile('refs/remotes/origin/main') —
// it first folds that ref's frontier onto the current branch (recording each of
// its tips as a tip of the current branch), then reconciles. This is the
// "merge a fetched multi-head remote into my branch" flow: after `dolt fetch`
// brings a forked remote's tips into the tracking ref, this merges them into the
// current branch and collapses the fork to one head.
func doltReconcile(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	mergedHash, tips, conflicts, message, err := doDoltReconcile(ctx, args)
	if err != nil {
		return nil, err
	}
	if message == "" {
		return rowToIter(mergedHash, int64(tips), int64(conflicts), nil), nil
	}
	return rowToIter(mergedHash, int64(tips), int64(conflicts), message), nil
}

func doDoltReconcile(ctx *sql.Context, args []string) (string, int, int, string, error) {
	dbName := ctx.GetCurrentDatabase()
	if dbName == "" {
		return "", 0, 0, "", fmt.Errorf("empty database name")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	ddb, ok := sess.GetDoltDB(ctx, dbName)
	if !ok {
		return "", 0, 0, "", fmt.Errorf("could not load database %s", dbName)
	}

	if !ddb.IsMultihead() {
		return "", 0, 0, "dolt_reconcile requires multi-head mode (DOLT_MULTIHEAD)", nil
	}

	headRef, err := sess.CWBHeadRef(ctx, dbName)
	if err != nil {
		return "", 0, 0, "", err
	}

	// An optional source ref folds that ref's frontier onto the current branch
	// before reconciling — the "merge a fetched multi-head remote" flow. Its
	// tips' chunks are already local (dolt fetch pulled the whole frontier).
	if len(args) > 1 {
		return "", 0, 0, "", fmt.Errorf("dolt_reconcile takes at most one ref argument")
	}
	if len(args) == 1 {
		srcTips, err := ddb.MultiheadTips(ctx, args[0])
		if err != nil {
			return "", 0, 0, "", err
		}
		for _, t := range srcTips {
			if err := ddb.RecordTip(ctx, headRef.String(), t); err != nil {
				return "", 0, 0, "", err
			}
		}
	}

	name, email, _, _, err := dsess.ResolveNameEmail(ctx, dsess.DoltCommitterName, dsess.DoltCommitterEmail)
	if err != nil {
		return "", 0, 0, "", err
	}
	meta, err := datas.NewCommitMeta(name, email, fmt.Sprintf("Reconcile multi-head frontier of %s", headRef.String()))
	if err != nil {
		return "", 0, 0, "", err
	}

	var eo editor.Options
	state, _, err := sess.LookupDbState(ctx, dbName)
	if err != nil {
		return "", 0, 0, "", err
	}
	if ws := state.WriteSession(); ws != nil {
		eo = ws.GetOptions()
	}

	result, err := merge.ReconcileFrontier(ctx, ddb, headRef, meta, eo)
	if err != nil {
		return "", 0, 0, "", err
	}

	if result.TipsBefore <= 1 {
		return result.Merged.String(), result.TipsBefore, 0, "nothing to reconcile: the branch has a single head", nil
	}

	// Bring the session's working set into line with the merged root so queries
	// see the reconciled data (mirrors executeFFMerge's working-set advance).
	if err := syncWorkingSetToCommit(ctx, sess, dbName, ddb, result.Merged, result.HasConflicts()); err != nil {
		return "", 0, 0, "", err
	}

	conflicts := 0
	if result.HasConflicts() {
		conflicts = 1
	}
	msg := fmt.Sprintf("reconciled %d heads into %s", result.TipsBefore, result.Merged.String())
	return result.Merged.String(), result.TipsBefore, conflicts, msg, nil
}

// syncWorkingSetToCommit points the session's working and staged roots at the
// root of |commitHash| and commits the working set, so a completed reconcile is
// visible to subsequent queries. When the reconcile recorded conflicts, the
// merged root legitimately carries conflict artifacts (as a committed conflicted
// merge does); |hasConflicts| lets the working-set commit through so those
// conflicts persist to be resolved (dolt conflicts) rather than rolling back.
func syncWorkingSetToCommit(ctx *sql.Context, sess *dsess.DoltSession, dbName string, ddb *doltdb.DoltDB, commitHash hash.Hash, hasConflicts bool) error {
	optCmt, err := ddb.ReadCommit(ctx, commitHash)
	if err != nil {
		return err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}
	mergedRoot, err := cm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	ws, err := sess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}
	ws = ws.WithWorkingRoot(mergedRoot).WithStagedRoot(mergedRoot)
	if err := sess.SetWorkingSet(ctx, dbName, ws); err != nil {
		return err
	}

	if hasConflicts {
		// The merged head records conflicts; allow the working-set commit to carry
		// them so the transaction is not rolled back (they are resolved afterward
		// with dolt_conflicts_resolve, exactly as for a committed conflicted merge).
		if err := ctx.SetSessionVariable(ctx, dsess.AllowCommitConflicts, 1); err != nil {
			return err
		}
	}
	return sess.CommitWorkingSet(ctx, dbName, sess.GetTransaction())
}
