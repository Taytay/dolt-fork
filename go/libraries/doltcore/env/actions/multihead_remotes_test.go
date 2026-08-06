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

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// loadEmptyFileDB creates and loads a fresh on-disk DoltDB with no commits — the
// model of a bare file:// remote before its first push.
func loadEmptyFileDB(t *testing.T, dir string) *doltdb.DoltDB {
	ctx := context.Background()
	require.NoError(t, filesys.LocalFS.MkDirs(filepath.Join(dir, dbfactory.DoltDataDir)))
	url := "file://" + filepath.Join(dir, dbfactory.DoltDataDir)
	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, url, filesys.LocalFS)
	require.NoError(t, err)
	return ddb
}

// TestPushMultihead_FolderRootsAreAuthoritative proves the lazy-sync property:
// a multi-head push records the head in the append-only `roots/` set, so the
// remote's frontier survives even when its mutable `manifest` file is lost
// (as a Dropbox/Drive conflict-copy would lose it). Two divergent pushes fork
// the remote; the manifest is then deleted; a fresh handle still reconstructs
// the full two-tip frontier from `roots/`.
func TestPushMultihead_FolderRootsAreAuthoritative(t *testing.T) {
	t.Setenv(dconfig.EnvMultihead, "1")
	ctx := context.Background()

	local := loadEmptyFileDB(t, t.TempDir())
	require.NoError(t, local.WriteEmptyRepo(ctx, "main", "Test", "t@t.com"))
	mainRef := ref.NewBranchRef("main")
	originMain := ref.NewRemoteRef("origin", "main")

	cs, _ := doltdb.NewCommitSpec("main")
	optC0, err := local.Resolve(ctx, cs, nil)
	require.NoError(t, err)
	c0, ok := optC0.ToCommit()
	require.True(t, ok)
	rv, err := c0.GetRootValue(ctx)
	require.NoError(t, err)
	_, valHash, err := local.WriteRootValue(ctx, rv)
	require.NoError(t, err)

	metaA, err := datas.NewCommitMeta("A", "a@a.com", "A change")
	require.NoError(t, err)
	c1a, err := local.CommitDanglingWithParentCommits(ctx, valHash, []*doltdb.Commit{c0}, metaA)
	require.NoError(t, err)
	c1aH, err := c1a.HashOf()
	require.NoError(t, err)

	metaB, err := datas.NewCommitMeta("B", "b@b.com", "B change")
	require.NoError(t, err)
	c1b, err := local.CommitDanglingWithParentCommits(ctx, valHash, []*doltdb.Commit{c0}, metaB)
	require.NoError(t, err)
	c1bH, err := c1b.HashOf()
	require.NoError(t, err)

	remoteDir := t.TempDir()
	remote := loadEmptyFileDB(t, remoteDir)
	tmpDir := t.TempDir()

	// Push the base then two divergent children: the remote forks.
	require.NoError(t, pushMultihead(ctx, tmpDir, mainRef, originMain, local, remote, c0, nil))
	require.NoError(t, pushMultihead(ctx, tmpDir, mainRef, originMain, local, remote, c1a, nil))
	require.NoError(t, pushMultihead(ctx, tmpDir, mainRef, originMain, local, remote, c1b, nil))

	// Each push deposited an append-only roots/ record.
	roots, ok, err := remote.FolderFrontierRootHashes(ctx)
	require.NoError(t, err)
	require.True(t, ok, "a file:// remote is a local folder store")
	require.NotEmpty(t, roots)

	// LAZY-SYNC SIMULATION: the shared manifest is lost (conflict-copied away).
	require.NoError(t, os.Remove(filepath.Join(remoteDir, dbfactory.DoltDataDir, "manifest")))

	// A fresh handle opens the manifest-less folder and reconstructs the frontier
	// from roots/ alone — no head was lost with the manifest.
	reopened := loadEmptyFileDB(t, remoteDir)
	materialized, err := reopened.MaterializeFolderFrontier(ctx)
	require.NoError(t, err)
	require.True(t, materialized, "the folder has a roots/ set to materialize")

	recovered, ok, err := reopened.FolderFrontierRootHashes(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	tips, err := reopened.RefFrontierAcrossFolderRoots(ctx, mainRef.String(), recovered)
	require.NoError(t, err)
	assert.ElementsMatch(t, []hash.Hash{c1aH, c1bH}, tips,
		"the two-tip fork is recovered from roots/ after the manifest was lost")
}

// mustTips returns the frontier of refStr on ddb.
func mustTips(t *testing.T, ddb *doltdb.DoltDB, refStr string) []hash.Hash {
	t.Helper()
	tips, err := ddb.MultiheadTips(context.Background(), refStr)
	require.NoError(t, err)
	return tips
}

// TestPushMultihead_DivergentPushForks proves the core of multi-head push: two
// commits parented on a common base, pushed one after the other, leave the
// remote with a FRONTIER of two tips rather than the second push being rejected
// as non-fast-forward. A sequential (child) push instead collapses the frontier
// back to one tip.
func TestPushMultihead_DivergentPushForks(t *testing.T) {
	t.Setenv(dconfig.EnvMultihead, "1")
	ctx := context.Background()

	// A local repo with a base commit C0 on main.
	local := loadEmptyFileDB(t, t.TempDir())
	require.NoError(t, local.WriteEmptyRepo(ctx, "main", "Test", "t@t.com"))
	mainRef := ref.NewBranchRef("main")
	originMain := ref.NewRemoteRef("origin", "main")

	cs, _ := doltdb.NewCommitSpec("main")
	optC0, err := local.Resolve(ctx, cs, nil)
	require.NoError(t, err)
	c0, ok := optC0.ToCommit()
	require.True(t, ok)
	c0h, err := c0.HashOf()
	require.NoError(t, err)

	// Two divergent children of C0 (distinct meta -> distinct commits), built
	// dangling so neither is recorded as a head locally.
	rv, err := c0.GetRootValue(ctx)
	require.NoError(t, err)
	_, valHash, err := local.WriteRootValue(ctx, rv)
	require.NoError(t, err)

	metaA, err := datas.NewCommitMeta("A", "a@a.com", "A change")
	require.NoError(t, err)
	c1a, err := local.CommitDanglingWithParentCommits(ctx, valHash, []*doltdb.Commit{c0}, metaA)
	require.NoError(t, err)
	c1aH, err := c1a.HashOf()
	require.NoError(t, err)

	metaB, err := datas.NewCommitMeta("B", "b@b.com", "B change")
	require.NoError(t, err)
	c1b, err := local.CommitDanglingWithParentCommits(ctx, valHash, []*doltdb.Commit{c0}, metaB)
	require.NoError(t, err)
	c1bH, err := c1b.HashOf()
	require.NoError(t, err)

	require.NotEqual(t, c1aH, c1bH, "divergent commits must differ")

	// A bare remote.
	remoteDir := t.TempDir()
	remote := loadEmptyFileDB(t, remoteDir)
	tmpDir := t.TempDir()

	// Push the base: the remote's main frontier is exactly C0.
	require.NoError(t, pushMultihead(ctx, tmpDir, mainRef, originMain, local, remote, c0, nil))
	assert.Equal(t, []hash.Hash{c0h}, mustTips(t, remote, mainRef.String()))

	// Push C1a (a child of C0): it supersedes C0, so the frontier collapses to C1a.
	require.NoError(t, pushMultihead(ctx, tmpDir, mainRef, originMain, local, remote, c1a, nil))
	assert.Equal(t, []hash.Hash{c1aH}, mustTips(t, remote, mainRef.String()),
		"a sequential (child) push fast-forwards to one tip")

	// Push C1b (the OTHER child of C0). No fast-forward gate: it is accepted and
	// the remote now holds a fork of two tips.
	require.NoError(t, pushMultihead(ctx, tmpDir, mainRef, originMain, local, remote, c1b, nil))
	forked := mustTips(t, remote, mainRef.String())
	require.Len(t, forked, 2, "a divergent push forks the remote instead of being rejected")
	assert.ElementsMatch(t, []hash.Hash{c1aH, c1bH}, forked)

	// The pushing side's local remote-tracking ref reflects what it published
	// (its own two pushed tips; the base is superseded).
	trackTips := mustTips(t, local, originMain.String())
	assert.ElementsMatch(t, []hash.Hash{c1aH, c1bH}, trackTips)

	// A THIRD writer fetching sees the whole fork. Simulate the fetch mechanics
	// (what fetchRefSpecsMultihead does): enumerate the remote frontier, pull the
	// tips' chunks, and record them all on the local tracking ref.
	third := loadEmptyFileDB(t, t.TempDir())
	remoteFrontier := mustTips(t, remote, mainRef.String())
	require.NoError(t, third.PullChunks(ctx, t.TempDir(), remote, remoteFrontier, nil, nil))
	for _, tip := range remoteFrontier {
		require.NoError(t, third.RecordTip(ctx, originMain.String(), tip))
	}
	fetched := mustTips(t, third, originMain.String())
	require.Len(t, fetched, 2, "fetch brings the remote's whole frontier into the tracking ref")
	assert.ElementsMatch(t, []hash.Hash{c1aH, c1bH}, fetched)
}
