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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

// TestMultihead_FolderBridgeNoManifestOverwrite is the relational bridge between
// the two multi-head layers: it publishes per-ref frontier data (the datas
// layer's tips-in-one-AddressMap) through the append-only nbs `roots/` layer
// (the store layer's whole-store roots), and reads the per-ref frontier back
// across the fork — WITHOUT ever overwriting a manifest on the shared folder.
//
// This is the write/read shape a lazily-synced folder (Dropbox/Drive) needs: no
// mutable single-file cell to conflict-copy or clobber. Two independent writers
// (separate stores seeded from a common base, modelling clones) each commit a
// divergent relational tip and PublishHeadTo the shared folder — each deposit is
// content-addressed table files plus a write-once `roots/` record. The shared
// folder holds NO manifest after the writes. A reader then unions the ref's tips
// across the frontier records (RefFrontierAcrossRoots) and reconciles them with
// Dolt's real three-way merge; a merged deposit collapses the frontier to one.
//
// Contrast TestMultihead_SharedFolderReconcileEndToEnd, which drives the same
// relational fork through db.Commit — and so through the manifest CAS, where the
// writers must take turns. Here nothing is overwritten and nothing serializes on
// a shared cell.
func TestMultihead_FolderBridgeNoManifestOverwrite(t *testing.T) {
	ctx := context.Background()
	const ref = "refs/heads/main"

	openAt := func(dir string) (*nbs.NomsBlockStore, *database, func()) {
		require.NoError(t, os.MkdirAll(dir, 0o777))
		st, err := nbs.NewLocalStore(ctx, constants.FormatDefaultString, dir, mhE2EMemTableSize, nbs.NewUnlimitedMemQuotaProvider(), false)
		require.NoError(t, err)
		db := NewDatabase(st).(*database)
		require.NoError(t, EnableMultihead(db))
		return st, db, func() { require.NoError(t, st.Close()) }
	}

	commitTip := func(db *database, ds Dataset, s map[int]testRow, parents ...hash.Hash) hash.Hash {
		m := mapFromState(t, db, s)
		ds2, err := db.Commit(ctx, ds, tree.ValueFromNode(m.Node()),
			CommitOptions{Parents: parents, Meta: pinnedMeta()})
		require.NoError(t, err)
		addr, ok := ds2.MaybeHeadAddr()
		require.True(t, ok)
		return addr
	}

	shared := filepath.Join(t.TempDir(), "shared")
	require.NoError(t, os.MkdirAll(shared, 0o777))

	// Session 0: commit a base in its own store and PUBLISH it (append-only) to
	// the shared folder. base's chunks land as content-addressed table files and
	// a `roots/` record names the whole-store root.
	baseDir := filepath.Join(t.TempDir(), "base")
	st0, db0, close0 := openAt(baseDir)
	ds0, err := db0.GetDataset(ctx, ref)
	require.NoError(t, err)
	base := commitTip(db0, ds0, map[int]testRow{1: str("a"), 2: str("b")})
	rec0, err := st0.PublishHeadTo(ctx, shared, nil)
	require.NoError(t, err)
	close0()

	// Two writers seeded from base (clones of the base store on disk).
	dirA := filepath.Join(t.TempDir(), "a")
	dirB := filepath.Join(t.TempDir(), "b")
	cloneDirFiles(t, baseDir, dirA)
	cloneDirFiles(t, baseDir, dirB)

	// Writer A: commit a child of base (fast-forward off the observed head) and
	// publish it, naming base's record as parent.
	stA, dbA, closeA := openAt(dirA)
	dsA, err := dbA.GetDataset(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, base, mustHeadAddr(dsA), "writer A is seeded at base")
	tipA := commitTip(dbA, dsA, map[int]testRow{1: str("A"), 2: str("b")})
	_, err = stA.PublishHeadTo(ctx, shared, []hash.Hash{rec0})
	require.NoError(t, err)
	closeA()

	// Writer B: a divergent child of base (explicit parent), disjoint edit.
	stB, dbB, closeB := openAt(dirB)
	dsB, err := dbB.GetDataset(ctx, ref)
	require.NoError(t, err)
	tipB := commitTip(dbB, dsB, map[int]testRow{1: str("a"), 2: str("b"), 3: str("c")}, base)
	_, err = stB.PublishHeadTo(ctx, shared, []hash.Hash{rec0})
	require.NoError(t, err)
	closeB()
	require.NotEqual(t, tipA, tipB, "the two writers produced divergent tips")

	// THE PROPERTY: publishing never wrote a manifest to the shared folder. The
	// only mutable-looking file a lazy-sync tool could conflict-copy would be
	// `manifest`; there is none — just content-addressed table files and roots/.
	_, statErr := os.Stat(filepath.Join(shared, "manifest"))
	require.True(t, os.IsNotExist(statErr), "publish must not write a manifest to the shared folder")
	entries, err := os.ReadDir(filepath.Join(shared, "roots"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 3, "base + two writers each appended a roots/ record")

	// Reader: reconstruct the per-ref frontier across the roots/ records. First
	// materialize a readable union manifest (a read-side compaction, not a write
	// on the concurrent path), then open a reader store over the folder.
	_, err = nbs.MaterializeFrontierManifest(ctx, shared)
	require.NoError(t, err)
	stR, dbR, closeR := openAt(shared)
	defer closeR()

	roots, err := nbs.MultiheadFolderRootHashes(ctx, shared)
	require.NoError(t, err)
	require.Len(t, roots, 2, "the folder frontier is the two writers' roots (base superseded)")

	frontier, err := RefFrontierAcrossRoots(ctx, dbR, ref, roots)
	require.NoError(t, err)
	require.ElementsMatch(t, []hash.Hash{tipA, tipB}, frontier,
		"the per-ref frontier is read back across the roots/ fork")

	// Reconcile the fork with Dolt's real three-way merge (disjoint edits -> clean).
	var conflicts []int
	merged, err := MergeTips(ctx, dbR, tipA, tipB, mhKd, mhVd, policyCollide("record", &conflicts))
	require.NoError(t, err)
	assert.Empty(t, conflicts, "disjoint edits do not conflict")
	assert.Equal(t, map[int]testRow{1: str("A"), 2: str("b"), 3: str("c")}, stateFromMap(t, merged))

	// Deposit the merged head as another append-only record; the frontier
	// collapses to one tip when read back across roots/.
	mergeAddr, err := AppendMapCommit(ctx, dbR, ref, merged,
		CommitOptions{Parents: []hash.Hash{tipA, tipB}, Meta: pinnedMeta()})
	require.NoError(t, err)
	_, err = stR.PublishHeadTo(ctx, shared, roots)
	require.NoError(t, err)

	rootsAfter, err := nbs.MultiheadFolderRootHashes(ctx, shared)
	require.NoError(t, err)
	collapsed, err := RefFrontierAcrossRoots(ctx, dbR, ref, rootsAfter)
	require.NoError(t, err)
	require.Equal(t, []hash.Hash{mergeAddr}, collapsed, "the merged deposit collapses the frontier to one head")
}

// cloneDirFiles copies the top-level regular files of a non-generational nbs
// store dir (its table files and manifest) into dst, modelling an on-disk clone
// seeded at the source's committed state.
func cloneDirFiles(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dst, 0o777))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		copyFile(t, filepath.Join(src, e.Name()), filepath.Join(dst, e.Name()))
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	require.NoError(t, err)
	defer in.Close()
	out, err := os.Create(dst)
	require.NoError(t, err)
	defer out.Close()
	_, err = io.Copy(out, in)
	require.NoError(t, err)
}
