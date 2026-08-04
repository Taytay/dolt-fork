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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

// TestMultihead_ConcurrentWritersNoLock is the payoff of the "flip the nbs
// default" follow-on: N writers publish divergent heads into ONE shared folder
// SIMULTANEOUSLY, with no lock and no CAS, and all N heads survive.
//
// Each writer is a separate store (its own dir), commits its own edit, then
// PublishHeadTo the shared folder — copying its content-addressed table files
// and appending a `roots/` record naming the common base. All N fire at once off
// a barrier. Afterwards the shared folder's frontier is exactly the N heads, and
// every head's chunks read back from the folder (nothing clobbered). Contrast
// this with TestMultihead_StockSingleRootRejectsConcurrentDivergent below, where
// the stock single-root Commit admits only one of two concurrent divergent
// writers — the CAS that `roots/` removes.
func TestMultihead_ConcurrentWritersNoLock(t *testing.T) {
	ctx := context.Background()
	parent := t.TempDir()
	shared := filepath.Join(parent, "shared")
	require.NoError(t, os.MkdirAll(shared, 0o777))

	newStoreIn := func(dir string) *NomsBlockStore {
		require.NoError(t, os.MkdirAll(dir, 0o777))
		st, err := NewLocalStore(ctx, constants.FormatDefaultString, dir, testMemTableSize, NewUnlimitedMemQuotaProvider(), false)
		require.NoError(t, err)
		return st
	}

	// Bootstrap a shared base and publish it as the first head.
	stBase := newStoreIn(filepath.Join(parent, "base"))
	baseChunk := chunks.NewChunk([]byte("shared-base"))
	require.NoError(t, stBase.Put(ctx, baseChunk, noopGetAddrs))
	ok, err := stBase.Commit(ctx, baseChunk.Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)
	rec0, err := stBase.PublishHeadTo(ctx, shared, nil)
	require.NoError(t, err)
	require.NoError(t, stBase.Close())

	// N writers, each in its own store, all publishing to the shared folder at
	// once. Pre-create the dirs on the test goroutine (t.TempDir/os are not for
	// concurrent sub-goroutines); the workers only open + write + publish.
	const N = 8
	dirs := make([]string, N)
	for i := range dirs {
		dirs[i] = filepath.Join(parent, fmt.Sprintf("w%d", i))
		require.NoError(t, os.MkdirAll(dirs[i], 0o777))
	}

	roots := make([]hash.Hash, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // fire all writers simultaneously
			st, err := NewLocalStore(ctx, constants.FormatDefaultString, dirs[i], testMemTableSize, NewUnlimitedMemQuotaProvider(), false)
			if err != nil {
				errs[i] = err
				return
			}
			defer st.Close()
			c := chunks.NewChunk([]byte(fmt.Sprintf("writer-%d-edit", i)))
			if err := st.Put(ctx, c, noopGetAddrs); err != nil {
				errs[i] = err
				return
			}
			if ok, err := st.Commit(ctx, c.Hash(), hash.Hash{}); err != nil || !ok {
				errs[i] = fmt.Errorf("local commit ok=%v err=%w", ok, err)
				return
			}
			roots[i] = c.Hash()
			// The lock-free, CAS-free publish into the shared folder. Every
			// writer names the same base record as parent -> a fan of N tips.
			if _, err := st.PublishHeadTo(ctx, shared, []hash.Hash{rec0}); err != nil {
				errs[i] = err
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, e := range errs {
		require.NoErrorf(t, e, "writer %d", i)
	}

	// The shared folder now has exactly the N divergent heads (the base is
	// superseded because every writer named it as a parent). No writer was
	// rejected; none clobbered another.
	frontier, err := MultiheadFolderFrontier(ctx, shared)
	require.NoError(t, err)
	require.Len(t, frontier, N, "N concurrent writers -> N heads, no lock, no CAS rejection")
	got := make([]hash.Hash, 0, N)
	for _, h := range frontier {
		got = append(got, h.root)
	}
	assert.ElementsMatch(t, roots, got, "the frontier is exactly the writers' heads")

	// Reconcile-read: compact the frontier into a manifest and read every head's
	// chunk back from the one shared folder — the table files really are shared.
	_, err = MaterializeFrontierManifest(ctx, shared)
	require.NoError(t, err)
	stR := newStoreIn(shared)
	defer stR.Close()
	for i, r := range roots {
		c, err := stR.Get(ctx, r)
		require.NoError(t, err)
		assert.Falsef(t, c.IsEmpty(), "head %d (%s) must be readable from the shared folder", i, r.String())
	}
}

// TestMultihead_StockSingleRootRejectsConcurrentDivergent is the contrast: two
// stock stores on one folder both commit a divergent edit off the same base at
// once, and the single-root manifest CAS admits exactly one. This is the
// coordination that PublishHeadTo/`roots/` removes.
func TestMultihead_StockSingleRootRejectsConcurrentDivergent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	open := func() *NomsBlockStore {
		st, err := NewLocalStore(ctx, constants.FormatDefaultString, dir, testMemTableSize, NewUnlimitedMemQuotaProvider(), false)
		require.NoError(t, err)
		return st
	}

	// Two handles on ONE folder (the file manifest permits concurrent opens; it
	// coordinates at Commit, not at open).
	stA := open()
	defer stA.Close()
	stB := open()
	defer stB.Close()

	cA := chunks.NewChunk([]byte("edit-A"))
	cB := chunks.NewChunk([]byte("edit-B"))
	require.NoError(t, stA.Put(ctx, cA, noopGetAddrs))
	require.NoError(t, stB.Put(ctx, cB, noopGetAddrs))

	var wg sync.WaitGroup
	start := make(chan struct{})
	var okA, okB bool
	var errA, errB error
	wg.Add(2)
	go func() { defer wg.Done(); <-start; okA, errA = stA.Commit(ctx, cA.Hash(), hash.Hash{}) }()
	go func() { defer wg.Done(); <-start; okB, errB = stB.Commit(ctx, cB.Hash(), hash.Hash{}) }()
	close(start)
	wg.Wait()

	require.NoError(t, errA)
	require.NoError(t, errB)
	successes := 0
	if okA {
		successes++
	}
	if okB {
		successes++
	}
	assert.Equal(t, 1, successes,
		"stock single-root Commit admits only one concurrent divergent writer (the CAS multi-head removes)")
}
