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

// Frontier-aware reconcile: collapse a ref's multi-head fork through the full
// relational merge engine (MULTIHEAD.md).
//
// The multi-head fetch/push work makes a ref hold a FRONTIER of tips (several
// heads, produced by two writers off a common base). This turns that frontier
// back into a single head: it three-way merges the tips with the same engine
// `dolt merge` uses (merge.MergeRoots), writes the merged RootValue as a commit
// naming every tip as a parent, and lets the frontier collapse — because every
// original tip is now a named parent of the merged commit, datas.Tips drops them
// and the ref resolves to the one merged head again.
//
// The engine tier was already validated in multihead_reconcile_test.go (real
// MergeRoots over multiple tables, a schema change, and a recorded conflict);
// this is the driver that runs it against a live ref's frontier so `dolt` can
// reconcile a fork end-to-end. It deliberately lives in the merge package (not
// doltdb) because it calls MergeRoots, which imports doltdb.
package merge

import (
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// ErrNoCommonAncestor is returned when two tips of a forked ref share no common
// ancestor, so there is no base for a three-way merge. A genuine multi-head fork
// always shares the commit it forked from, so this indicates unrelated histories.
var ErrNoCommonAncestor = errors.New("multihead reconcile: tips share no common ancestor")

// ReconcileResult reports what a frontier reconcile did.
type ReconcileResult struct {
	// Merged is the ref's single head after reconcile: the new merged tip when a
	// fork was collapsed, the sole existing tip when there was nothing to do, or
	// empty for a headless ref.
	Merged hash.Hash
	// TipsBefore is how many tips the frontier held before reconcile (<=1 means
	// nothing was merged).
	TipsBefore int
	// DataConflicts / SchemaConflicts / ConstraintViolations aggregate the merge
	// artifacts recorded across all tables of the merged root. Non-zero means the
	// merged head carries unresolved conflicts to resolve (dolt conflicts) as
	// usual — the reconcile still collapses the frontier so the branch has one
	// head to work from.
	DataConflicts        int
	SchemaConflicts      int
	ConstraintViolations int
}

// HasConflicts reports whether the reconcile recorded any conflict or violation.
func (r *ReconcileResult) HasConflicts() bool {
	return r.DataConflicts > 0 || r.SchemaConflicts > 0 || r.ConstraintViolations > 0
}

// ReconcileFrontier collapses the multi-head frontier of |branchRef| into a
// single head by three-way merging its tips through the real merge engine and
// recording the merged RootValue as a commit that names every tip as a parent
// (so the frontier collapses to that one merged tip).
//
// It is a no-op that returns the sole tip when the ref is not forked. It only
// advances the branch's committed frontier; the caller is responsible for
// bringing the working set into line with the merged root (see the
// dolt_reconcile procedure). Meaningful only in multi-head mode
// (DOLT_MULTIHEAD).
//
// For a two-tip fork (two writers off one base — the common case) the base is
// their exact common ancestor and the merge is a faithful three-way. For more
// than two tips it folds the remaining tips against the accumulated root using
// the canonical tip's ancestor as the base (an octopus-style approximation);
// the merged commit still names all tips as parents, so the frontier fully
// collapses.
func ReconcileFrontier(
	ctx *sql.Context,
	ddb *doltdb.DoltDB,
	branchRef ref.DoltRef,
	meta *datas.CommitMeta,
	eo editor.Options,
) (*ReconcileResult, error) {
	tips, err := ddb.MultiheadTips(ctx, branchRef.String())
	if err != nil {
		return nil, err
	}

	res := &ReconcileResult{TipsBefore: len(tips)}
	if len(tips) == 0 {
		return res, nil
	}
	if len(tips) == 1 {
		res.Merged = tips[0]
		return res, nil
	}

	// tips[0] is the canonical (lowest-hash) tip; accumulate the merge onto its
	// root. The remaining tips become the other parents of the merged commit.
	oursCommit, err := readCommit(ctx, ddb, tips[0])
	if err != nil {
		return nil, err
	}
	mergedRoot, err := oursCommit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	mo := MergeOpts{KeepSchemaConflicts: true}
	var otherCommits []*doltdb.Commit
	for i := 1; i < len(tips); i++ {
		theirsCommit, err := readCommit(ctx, ddb, tips[i])
		if err != nil {
			return nil, err
		}
		otherCommits = append(otherCommits, theirsCommit)

		baseCommit, err := commonAncestor(ctx, oursCommit, theirsCommit)
		if err != nil {
			return nil, err
		}
		theirsRoot, err := theirsCommit.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}
		baseRoot, err := baseCommit.GetRootValue(ctx)
		if err != nil {
			return nil, err
		}

		result, err := MergeRoots(ctx, doltdb.SimpleTableResolver{}, mergedRoot, theirsRoot, baseRoot, theirsCommit, baseCommit, eo, mo)
		if err != nil {
			return nil, err
		}
		mergedRoot = result.Root
		for _, s := range result.Stats {
			res.DataConflicts += s.DataConflicts
			res.SchemaConflicts += s.SchemaConflicts
			res.ConstraintViolations += s.ConstraintViolations
		}
	}

	_, valHash, err := ddb.WriteRootValue(ctx, mergedRoot)
	if err != nil {
		return nil, err
	}

	// Record the merged commit naming every tip as a parent. CommitWithParentCommits
	// auto-includes the ref's current (canonical) head, tips[0]; otherCommits adds
	// tips[1..]. In multi-head mode this appends the commit as a tip; since it
	// names all prior tips as parents, datas.Tips drops them and the frontier
	// collapses to this one merged head.
	mergedCommit, err := ddb.CommitWithParentCommits(ctx, valHash, branchRef, otherCommits, meta)
	if err != nil {
		return nil, err
	}
	mh, err := mergedCommit.HashOf()
	if err != nil {
		return nil, err
	}
	res.Merged = mh
	return res, nil
}

func readCommit(ctx *sql.Context, ddb *doltdb.DoltDB, h hash.Hash) (*doltdb.Commit, error) {
	optCmt, err := ddb.ReadCommit(ctx, h)
	if err != nil {
		return nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}
	return cm, nil
}

// commonAncestor returns the common-ancestor commit of two tips, which serves as
// the three-way merge base. A genuine multi-head fork always shares the commit it
// forked from, so this is expected to be found.
func commonAncestor(ctx *sql.Context, ours, theirs *doltdb.Commit) (*doltdb.Commit, error) {
	optBase, err := doltdb.GetCommitAncestor(ctx, ours, theirs)
	if err != nil {
		return nil, err
	}
	base, ok := optBase.ToCommit()
	if !ok {
		return nil, ErrNoCommonAncestor
	}
	return base, nil
}
