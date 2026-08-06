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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

// TestMultihead_IntegratesWithRealStore drives a real NomsBlockStore: put a
// chunk, commit it as the root, then publish that committed head into the
// content-addressed multi-head root set and read it back. This proves the
// bridge — a real nbs commit (root + table specs over shared table files)
// becomes a head in the multi-head DAG — with no change to the store's own
// single-root commit path.
func TestMultihead_IntegratesWithRealStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	st, err := NewLocalStore(ctx, constants.FormatDefaultString, dir, testMemTableSize, NewUnlimitedMemQuotaProvider(), false)
	require.NoError(t, err)
	defer st.Close()

	// Write a real chunk and commit it as the store root.
	c := chunks.NewChunk([]byte("hello-multihead"))
	require.NoError(t, st.Put(ctx, c, noopGetAddrs))
	ok, err := st.Commit(ctx, c.Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)

	root, err := st.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, c.Hash(), root)

	// Record the committed head in the multi-head root DAG.
	rec, err := st.PublishHead(ctx, nil)
	require.NoError(t, err)
	require.False(t, rec.IsEmpty())

	heads, err := st.MultiheadRoots(ctx)
	require.NoError(t, err)
	require.Len(t, heads, 1)
	assert.Equal(t, c.Hash(), heads[0].root, "recorded head is the real store root")

	// Idempotent: publishing the same head again adds no second tip.
	_, err = st.PublishHead(ctx, nil)
	require.NoError(t, err)
	heads, err = st.MultiheadRoots(ctx)
	require.NoError(t, err)
	assert.Len(t, heads, 1)
}
