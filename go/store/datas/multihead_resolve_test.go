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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

// In resolve mode a forked ref resolves to the canonical (lowest-hash) head
// instead of returning ErrMultipleHeads — the behavior the CLI/SQL stack needs
// so ordinary reads keep working across a fork. The full frontier is still
// available via Tips.
func TestMultiheadResolve_GetDatasetPicksCanonicalHead(t *testing.T) {
	db := newMHDatabase(t)
	require.NoError(t, EnableMultiheadResolve(db))
	require.True(t, IsMultiheadResolve(db))
	ctx := context.Background()
	const ref = "refs/heads/main"

	base := mustAppend(t, db, ref, "base")
	a := mustAppend(t, db, ref, "A", base)
	b := mustAppend(t, db, ref, "B", base)

	tips := mustTips(t, db, ref)
	require.Len(t, tips, 2)
	require.ElementsMatch(t, []hash.Hash{a, b}, tips)

	// GetDataset does NOT error; it resolves to the canonical tip.
	ds, err := db.GetDataset(ctx, ref)
	require.NoError(t, err, "resolve mode must not return ErrMultipleHeads")
	addr, ok := ds.MaybeHeadAddr()
	require.True(t, ok)
	assert.Equal(t, tips[0], addr, "resolves to the canonical (lowest-hash) tip")
	assert.Equal(t, CanonicalTip(tips), addr)
}

// The strict mode (EnableMultihead) still errors on a fork — resolve mode is a
// separate opt-in, so the Step 2c contract is unchanged.
func TestMultiheadResolve_StrictModeStillErrors(t *testing.T) {
	db := newMHDatabase(t)
	require.NoError(t, EnableMultihead(db)) // strict, NOT resolve
	require.False(t, IsMultiheadResolve(db))
	ctx := context.Background()
	const ref = "refs/heads/main"

	base := mustAppend(t, db, ref, "base")
	mustAppend(t, db, ref, "A", base)
	mustAppend(t, db, ref, "B", base)

	_, err := db.GetDataset(ctx, ref)
	require.ErrorIs(t, err, ErrMultipleHeads)
}

// Datasets() collapses the internal tip sub-keys: a forked ref appears exactly
// once, resolved to its canonical head, so branch/tag enumeration never sees the
// multi-tip encoding.
func TestMultiheadResolve_DatasetsCollapsesTipKeys(t *testing.T) {
	db := newMHDatabase(t)
	require.NoError(t, EnableMultiheadResolve(db))
	ctx := context.Background()
	const ref = "refs/heads/main"

	base := mustAppend(t, db, ref, "base")
	a := mustAppend(t, db, ref, "A", base)
	b := mustAppend(t, db, ref, "B", base)
	tips := mustTips(t, db, ref)
	require.ElementsMatch(t, []hash.Hash{a, b}, tips)

	dsmap, err := db.Datasets(ctx)
	require.NoError(t, err)

	seen := map[string]hash.Hash{}
	require.NoError(t, dsmap.IterAll(ctx, func(id string, addr hash.Hash) error {
		assert.False(t, strings.Contains(id, tipKeyInfix), "tip sub-key leaked into enumeration: %q", id)
		seen[id] = addr
		return nil
	}))

	require.Len(t, seen, 1, "the forked ref appears exactly once")
	head, ok := seen[ref]
	require.True(t, ok, "the ref is enumerated under its plain name")
	assert.Equal(t, tips[0], head, "enumerated head is the canonical tip")

	n, err := dsmap.Len()
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

// Two refs, one forked and one linear, both enumerate cleanly.
func TestMultiheadResolve_MultipleRefsEnumerated(t *testing.T) {
	db := newMHDatabase(t)
	require.NoError(t, EnableMultiheadResolve(db))
	ctx := context.Background()

	// refs/heads/main forks; refs/heads/dev stays linear.
	base := mustAppend(t, db, "refs/heads/main", "base")
	mustAppend(t, db, "refs/heads/main", "A", base)
	mustAppend(t, db, "refs/heads/main", "B", base)
	d1 := mustAppend(t, db, "refs/heads/dev", "d1")

	dsmap, err := db.Datasets(ctx)
	require.NoError(t, err)
	seen := map[string]hash.Hash{}
	require.NoError(t, dsmap.IterAll(ctx, func(id string, addr hash.Hash) error {
		seen[id] = addr
		return nil
	}))
	require.Len(t, seen, 2)
	assert.Contains(t, seen, "refs/heads/main")
	assert.Equal(t, d1, seen["refs/heads/dev"], "linear ref resolves to its single head")
}
