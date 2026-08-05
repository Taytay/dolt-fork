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

package actions

// Multi-head fetch/push (MULTIHEAD.md). Stock push advances the remote's single
// root with a fast-forward check (Push -> CanFastForward -> FastForward): a
// divergent push is rejected as non-fast-forward. Stock fetch enumerates one
// head per remote ref and fast-forwards the local tracking ref to it.
//
// When a database is opened in multi-head mode (DOLT_MULTIHEAD, so both the
// local repo and a file:// remote come up multi-head — see
// doltdb.LoadDoltDBWithParams), a ref holds a FRONTIER of tips rather than one
// CAS'd head. These two functions are the fetch/push analogue of that:
//
//   - pushMultihead publishes the pushed commit as a *tip* of the remote ref
//     (DoltDB.RecordTip) with no fast-forward gate. Two writers sharing one
//     folder remote each land a head; the remote holds both as a fork, which
//     dolt_frontier() surfaces and the reconcile machinery collapses later.
//   - fetchRefSpecsMultihead pulls EVERY tip of each remote ref (DoltDB.Multihead-
//     Tips) and records them all on the local tracking ref, so the other
//     writers' heads become visible locally.
//
// The chunk transfer itself (PullChunks) is content-addressed and head-agnostic,
// so it is reused unchanged; only the ref-update discipline differs. The stock
// single-root path in remotes.go is untouched — these run only when the relevant
// DoltDB.IsMultihead() is true.

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// pushMultihead publishes |commit| as a tip of the remote's |destRef| with no
// fast-forward or lineage gate, so a divergent push forks the remote instead of
// being rejected. The commit's chunk closure is transferred first (unchanged,
// content-addressed); then the pushed commit is recorded as a tip on the remote
// and mirrored onto the local remote-tracking ref |remoteRef|. `dolt fetch` is
// what later brings the OTHER writers' tips into the tracking ref.
func pushMultihead(
	ctx context.Context,
	tempTableDir string,
	destRef ref.BranchRef,
	remoteRef ref.RemoteRef,
	srcDB, destDB *doltdb.DoltDB,
	commit *doltdb.Commit,
	statsCh chan pull.Stats,
) error {
	h, err := commit.HashOf()
	if err != nil {
		return err
	}

	err = destDB.PullChunks(ctx, tempTableDir, srcDB, []hash.Hash{h}, statsCh, nil)
	if errors.Is(err, nbs.ErrGhostChunkRequested) {
		err = ErrShallowPushImpossible
	}
	if err != nil {
		return err
	}

	// Record the pushed commit as a tip of the remote branch. No CAS, no
	// fast-forward gate: a concurrent divergent push adds a second head rather
	// than being rejected (the whole point of multi-head push). A sequential
	// push names the previous tip as a parent, so the remote's frontier collapses
	// back to one head.
	if err = destDB.RecordTip(ctx, destRef.String(), h); err != nil {
		return err
	}

	// Mirror the pushed tip onto the local remote-tracking ref so it reflects
	// what we published. This is append-only (RecordTip), so it never rejects.
	return srcDB.RecordTip(ctx, remoteRef.String(), h)
}

// fetchRefSpecsMultihead is the multi-head counterpart of fetchRefSpecsWithDepth:
// for each remote branch matched by |refSpecs| it pulls EVERY tip of the ref's
// frontier (not just a single head) and records them all on the local tracking
// ref, so an unresolved fork on the remote becomes visible locally. It takes no
// fast-forward gate — a fetched tip that diverges from the local tracking ref
// simply adds a head. Shallow (depth-limited) fetch is not supported here; the
// caller falls back to the stock path for that.
func fetchRefSpecsMultihead[C doltdb.Context](
	ctx context.Context,
	dbData env.DbData[C],
	srcDB *doltdb.DoltDB,
	refSpecs []ref.RemoteRefSpec,
	defaultRefSpecs bool,
	remote *env.Remote,
	mode ref.UpdateMode,
	statsCh chan pull.Stats,
) error {
	var branchRefs []doltdb.RefWithHash
	err := srcDB.VisitRefsOfType(ctx, ref.HeadRefTypes, func(r ref.DoltRef, addr hash.Hash) error {
		branchRefs = append(branchRefs, doltdb.RefWithHash{Ref: r, Hash: addr})
		return nil
	})
	if err != nil {
		return err
	}

	if len(branchRefs) == 0 {
		if defaultRefSpecs {
			// The remote has no branches. Nothing to do, like stock/Git.
			return nil
		}
		return fmt.Errorf("no branches found in remote '%s'", remote.Name)
	}

	// For each (refSpec, remote branch) pair, expand the branch to its full
	// frontier and remember which tips belong to which local tracking ref.
	type trackUpdate struct {
		trackRef ref.DoltRef
		tips     []hash.Hash
	}
	var updates []trackUpdate
	var toFetch []hash.Hash
	seen := make(map[hash.Hash]struct{})

	for _, rs := range refSpecs {
		rsSeen := false
		for _, branchRef := range branchRefs {
			remoteTrackRef := rs.DestRef(branchRef.Ref)
			if remoteTrackRef == nil {
				continue
			}
			rsSeen = true

			// Tags are single-headed; keep the stock single-hash behavior.
			if remoteTrackRef.GetType() == ref.TagRefType {
				updates = append(updates, trackUpdate{remoteTrackRef, []hash.Hash{branchRef.Hash}})
				if _, ok := seen[branchRef.Hash]; !ok {
					seen[branchRef.Hash] = struct{}{}
					toFetch = append(toFetch, branchRef.Hash)
				}
				continue
			}

			tips, err := srcDB.MultiheadTips(ctx, branchRef.Ref.String())
			if err != nil {
				return err
			}
			// A ref with no recorded tips yet (e.g. a stock remote read in
			// multi-head mode) falls back to its resolved head.
			if len(tips) == 0 {
				tips = []hash.Hash{branchRef.Hash}
			}
			updates = append(updates, trackUpdate{remoteTrackRef, tips})
			for _, t := range tips {
				if _, ok := seen[t]; !ok {
					seen[t] = struct{}{}
					toFetch = append(toFetch, t)
				}
			}
		}
		if !rsSeen {
			return fmt.Errorf("%w: '%s'", ref.ErrInvalidRefSpec, rs.GetRemRefToLocal())
		}
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return err
	}

	err = dbData.Ddb.PullChunks(ctx, tmpDir, srcDB, toFetch, statsCh, nil)
	if err == pull.ErrDBUpToDate {
		err = nil
	}
	if err != nil {
		return err
	}

	// Record every fetched tip on its local tracking ref. Append-only, no
	// fast-forward gate: divergent tips accumulate as a frontier the local repo
	// can inspect (dolt_frontier()) and reconcile.
	for _, u := range updates {
		if u.trackRef.GetType() == ref.TagRefType {
			if err := dbData.Ddb.SetHead(ctx, u.trackRef, u.tips[0]); err != nil {
				return err
			}
			continue
		}
		for _, t := range u.tips {
			if err := dbData.Ddb.RecordTip(ctx, u.trackRef.String(), t); err != nil {
				return err
			}
		}
	}

	if mode.Prune {
		var newHeads []doltdb.RefWithHash
		for _, u := range updates {
			newHeads = append(newHeads, doltdb.RefWithHash{Ref: u.trackRef, Hash: u.tips[0]})
		}
		if err := pruneBranches(ctx, dbData, *remote, newHeads); err != nil {
			return err
		}
	}

	// Tags: reuse the stock follow path so annotated tags come across too.
	err = FetchFollowTags(ctx, tmpDir, srcDB, dbData.Ddb, statsCh)
	if err != nil {
		return err
	}

	return nil
}
