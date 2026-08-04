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

package nbs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

// TestMultihead_SharedFolderTwoWritersOneHeadEach is the end-to-end storage
// demo for "one folder, two writers, a head each" (the shared-roots/ model).
//
// One on-disk folder is the shared store. Two writers each derive from a common
// base and publish their head into the folder's append-only, content-addressed
// roots/ set (no lock, no CAS, no writer id) over shared, content-addressed
// table files. The folder ends with TWO heads — discovered by LISTing roots/ —
// and every head's chunks are readable from the one shared store, because the
// table files are shared. This is the Go analogue of the multihead-store Python
// spike, on a real NomsBlockStore.
//
// Honesty note on concurrency: stock nbs takes an exclusive manifest lock per
// open, so the writers here take turns (open -> write -> publish -> close),
// which models a synced folder (Dropbox/S3 delivering one writer's changes, then
// the other's) rather than simultaneous writers on one process handle. The
// roots/ publish itself is lock-free and coordination-free (that is the point);
// removing the physical manifest lock so roots/ is the *only* root layer — i.e.
// truly concurrent writers — is the "flip the nbs default" follow-on. The
// clone-safety sibling of this property (two handles, no id, both survive) is in
// TestMultihead_ClonedDiskBecomesAFork.
func TestMultihead_SharedFolderTwoWritersOneHeadEach(t *testing.T) {
	ctx := context.Background()
	shared := t.TempDir()

	open := func() *NomsBlockStore {
		st, err := NewLocalStore(ctx, constants.FormatDefaultString, shared, testMemTableSize, NewUnlimitedMemQuotaProvider(), false)
		require.NoError(t, err)
		return st
	}
	putCommit := func(st *NomsBlockStore, payload string, last hash.Hash) hash.Hash {
		c := chunks.NewChunk([]byte(payload))
		require.NoError(t, st.Put(ctx, c, noopGetAddrs))
		ok, err := st.Commit(ctx, c.Hash(), last)
		require.NoError(t, err)
		require.True(t, ok)
		return c.Hash()
	}

	// Bootstrap a shared base and record it as the first head.
	st0 := open()
	base := putCommit(st0, "shared-base", hash.Hash{})
	rec0, err := st0.PublishHead(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, st0.Close())

	// Writer A: an edit derived from base, published as a head naming base's
	// record as its parent.
	stA := open()
	rootA := putCommit(stA, "writer-A-edit", base)
	_, err = stA.PublishHead(ctx, []hash.Hash{rec0})
	require.NoError(t, err)
	require.NoError(t, stA.Close())

	// Writer B: had synced at base and edited independently. It publishes a head
	// that also names base as its parent (NOT writer A's) — so the two are a
	// fork, not a fast-forward. No coordination, no rejection.
	stB := open()
	rootB := putCommit(stB, "writer-B-edit", rootA) // physical single-root CAS: last is the current upstream (rootA)
	_, err = stB.PublishHead(ctx, []hash.Hash{rec0})
	require.NoError(t, err)
	require.NoError(t, stB.Close())

	// A reader opening the shared folder sees the frontier: two heads.
	stR := open()
	defer stR.Close()
	heads, err := stR.MultiheadRoots(ctx)
	require.NoError(t, err)
	require.Len(t, heads, 2, "two writers -> two heads on the shared roots/ set, no CAS")
	assert.ElementsMatch(t, []hash.Hash{rootA, rootB},
		[]hash.Hash{heads[0].root, heads[1].root},
		"the frontier is exactly the two writers' roots")

	// Both heads' chunks are readable from the ONE shared store: table files are
	// shared and content-addressed, so neither writer clobbered the other.
	for _, h := range []hash.Hash{base, rootA, rootB} {
		got, err := stR.Get(ctx, h)
		require.NoError(t, err)
		assert.False(t, got.IsEmpty(), "chunk %s must be present in the shared store", h.String())
	}

	// The roots/ set really is on disk in the shared folder: at least two record
	// files (the two tips; the base record may be conjoined/superseded but the
	// tips are always present).
	entries, err := os.ReadDir(filepath.Join(shared, rootsDirName))
	require.NoError(t, err)
	nRecords := 0
	for _, e := range entries {
		if _, ok := hash.MaybeParse(e.Name()); ok {
			nRecords++
		}
	}
	assert.GreaterOrEqual(t, nRecords, 2, "the shared folder holds the append-only root records")
}
