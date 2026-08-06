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

	"github.com/dolthub/dolt/go/store/hash"
)

func mhContents(name string) manifestContents {
	r := hash.Of([]byte(name))
	return manifestContents{
		manifestVers: StorageVersion,
		nbfVers:      "test-nbf",
		root:         r,
		gcGen:        hash.Hash{},
		lock:         generateLockHash(r, nil, nil, nil),
	}
}

func rootSet(cs []manifestContents) map[hash.Hash]struct{} {
	s := make(map[hash.Hash]struct{}, len(cs))
	for _, c := range cs {
		s[c.root] = struct{}{}
	}
	return s
}

// TestMultihead_ClonedDiskBecomesAFork is the point of the whole design: a
// disk clone shares no coordinating state and needs no writer id, yet two
// copies publishing divergent records both survive as a fork instead of
// silently clobbering each other. Two handles on the same store stand in for
// original + clone.
func TestMultihead_ClonedDiskBecomesAFork(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	original, err := getMultiheadRoots(root)
	require.NoError(t, err)
	clone, err := getMultiheadRoots(root) // same store, no id — a "clone"
	require.NoError(t, err)

	base := mhContents("base")
	rBase, err := original.Publish(ctx, base, nil)
	require.NoError(t, err)

	// original and clone each advance from base with NO coordination:
	ca := mhContents("original-work")
	rA, err := original.Publish(ctx, ca, []hash.Hash{rBase})
	require.NoError(t, err)
	cb := mhContents("clone-work")
	rB, err := clone.Publish(ctx, cb, []hash.Hash{rBase})
	require.NoError(t, err)
	require.NotEqual(t, rA, rB)

	tips, err := original.Roots(ctx)
	require.NoError(t, err)
	got := rootSet(tips)
	assert.Contains(t, got, ca.root, "original's work survived")
	assert.Contains(t, got, cb.root, "clone's work survived (not clobbered)")
	assert.NotContains(t, got, base.root, "base superseded via parent chain")
	assert.Len(t, tips, 2, "a two-head fork, no writer id involved")
}

// TestMultihead_PublishIsIdempotent — content addressing means republishing
// identical content (e.g. a retry, or two clones doing the same thing) is a
// no-op returning the same address, never a duplicate or an overwrite.
func TestMultihead_PublishIsIdempotent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	a, err := getMultiheadRoots(root)
	require.NoError(t, err)
	b, err := getMultiheadRoots(root)
	require.NoError(t, err)

	base := mhContents("base")
	rBase, err := a.Publish(ctx, base, nil)
	require.NoError(t, err)

	c := mhContents("work")
	r1, err := a.Publish(ctx, c, []hash.Hash{rBase})
	require.NoError(t, err)
	r2, err := b.Publish(ctx, c, []hash.Hash{rBase}) // identical, other handle
	require.NoError(t, err)
	assert.Equal(t, r1, r2, "identical content => identical address")

	tips, err := a.Roots(ctx)
	require.NoError(t, err)
	assert.Len(t, tips, 1, "no duplicate head from the idempotent republish")
}

// TestMultihead_MergeCollapsesFrontier — a record naming both tips as parents
// resolves the fork; the frontier collapses to the single merge head.
func TestMultihead_MergeCollapsesFrontier(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	m, err := getMultiheadRoots(root)
	require.NoError(t, err)

	base := mhContents("base")
	rBase, err := m.Publish(ctx, base, nil)
	require.NoError(t, err)
	rA, err := m.Publish(ctx, mhContents("a"), []hash.Hash{rBase})
	require.NoError(t, err)
	rB, err := m.Publish(ctx, mhContents("b"), []hash.Hash{rBase})
	require.NoError(t, err)

	forked, err := m.Roots(ctx)
	require.NoError(t, err)
	require.Len(t, forked, 2)

	merged := mhContents("merged")
	_, err = m.Publish(ctx, merged, []hash.Hash{rA, rB})
	require.NoError(t, err)

	tips, err := m.Roots(ctx)
	require.NoError(t, err)
	require.Len(t, tips, 1, "merge naming both parents collapses the fork")
	assert.Equal(t, merged.root, tips[0].root)
}
