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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

// ResolveHead is sticky: a writer that holds a head keeps it as new tips appear
// (no flip-flop), and only falls back to the canonical pick once its head has
// been superseded. This is the anti-flip-flop guarantee.
func TestResolveHead_StickyDoesNotFlipFlop(t *testing.T) {
	db := newMHDatabase(t)
	ctx := context.Background()
	const ref = "refs/heads/main"

	base := mustAppend(t, db, ref, "base")

	// Writer A commits and adopts its head. One tip, so ResolveHead returns it
	// and reports no fork.
	a := mustAppend(t, db, ref, "A-edit", base)
	head, forked, err := ResolveHead(ctx, db, ref, hash.Hash{})
	require.NoError(t, err)
	require.False(t, forked)
	require.Equal(t, a, head)
	myHead := head // A remembers its head

	// Writer B forks from base. Now there are two tips. A's sticky head must NOT
	// change just because B's tip appeared — regardless of hash ordering.
	b := mustAppend(t, db, ref, "B-edit", base)
	tips := mustTips(t, db, ref)
	require.Len(t, tips, 2)
	require.ElementsMatch(t, []hash.Hash{a, b}, tips)

	head, forked, err = ResolveHead(ctx, db, ref, myHead)
	require.NoError(t, err)
	assert.True(t, forked, "the fork is surfaced")
	assert.Equal(t, a, head, "A keeps its own head; another writer's tip does not move it")

	// A reader with NO prior head gets the canonical (deterministic) pick, which
	// every reader agrees on with no coordination.
	canon, forked, err := ResolveHead(ctx, db, ref, hash.Hash{})
	require.NoError(t, err)
	assert.True(t, forked)
	assert.Equal(t, CanonicalTip(tips), canon)

	// After reconciling, A's old head is superseded; the sticky pick falls back
	// to the sole merged head.
	merged := mustAppend(t, db, ref, "merged", a, b)
	head, forked, err = ResolveHead(ctx, db, ref, myHead)
	require.NoError(t, err)
	assert.False(t, forked, "the fork is resolved")
	assert.Equal(t, merged, head, "a superseded head falls back to the current tip")
}

// An empty ref resolves to the empty hash with no fork.
func TestResolveHead_EmptyRef(t *testing.T) {
	db := newMHDatabase(t)
	head, forked, err := ResolveHead(context.Background(), db, "refs/heads/none", hash.Hash{})
	require.NoError(t, err)
	assert.True(t, head.IsEmpty())
	assert.False(t, forked)
}
