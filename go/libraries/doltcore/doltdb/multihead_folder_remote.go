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

package doltdb

// Multi-head push/fetch over a shared FOLDER, with the append-only nbs `roots/`
// layer as the authoritative head — the piece that lets a lazily-synced folder
// (Dropbox/Drive) be a remote (MULTIHEAD.md).
//
// The existing multi-head push (pushMultihead -> RecordTip) advances the
// remote's single `manifest` with an optimistic CAS. A synced folder mangles
// that mutable cell: two writers' concurrent advances get conflict-copied and
// one head is lost from the manifest. These helpers make the folder's `roots/`
// set — append-only, content-addressed, never overwritten — the source of
// truth, so the manifest advance becomes a best-effort cache nobody relies on:
// a head lost from the manifest is still present in its own `roots/` record, and
// a reader reconstructs the frontier from `roots/`.
//
// All of these are no-ops (ok=false) for a database that is not a local folder
// store (in-memory, http remote, ...), so callers fall back to the stock path.

import (
	"context"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// localFolderNBS returns the new-generation NomsBlockStore and its on-disk
// directory for a database backed by a local folder store, or ok=false for any
// other backend. The `roots/` multi-head deposit lives in that directory.
func (ddb *DoltDB) localFolderNBS(ctx context.Context) (store *nbs.NomsBlockStore, dir string, ok bool) {
	cs := datas.ChunkStoreFromDatabase(ddb.db)
	switch s := cs.(type) {
	case *nbs.GenerationalNBS:
		ng, isNBS := s.NewGen().(*nbs.NomsBlockStore)
		if !isNBS {
			return nil, "", false
		}
		store = ng
	case *nbs.NomsBlockStore:
		store = s
	default:
		return nil, "", false
	}
	dir, ok, err := store.Path(ctx)
	if err != nil || !ok {
		return nil, "", false
	}
	return store, dir, true
}

// IsLocalFolderStore reports whether this database is backed by a local folder
// (so the append-only `roots/` deposit applies). Meaningful only in multi-head
// mode; a caller gates on IsMultihead() && IsLocalFolderStore().
func (ddb *DoltDB) IsLocalFolderStore(ctx context.Context) bool {
	_, _, ok := ddb.localFolderNBS(ctx)
	return ok
}

// DepositFolderFrontierRecord appends this store's current committed head (its
// whole-store root and table specs) as an append-only, content-addressed
// `roots/` record — the authoritative multi-head head on a shared folder,
// written with no manifest overwrite and no lock. |parents| names the frontier
// records this one descends from (nil is always safe: the true frontier is
// recovered from commit parents by RefFrontierAcrossFolderRoots, so an
// un-parented record only lingers, it never corrupts the frontier). ok=false for
// a non-folder backend.
func (ddb *DoltDB) DepositFolderFrontierRecord(ctx context.Context, parents []hash.Hash) (rec hash.Hash, ok bool, err error) {
	store, _, ok := ddb.localFolderNBS(ctx)
	if !ok {
		return hash.Hash{}, false, nil
	}
	// RecordFrontierHead deposits the record in the store's own directory without
	// copying table files (they are already present from the push transfer).
	rec, err = store.RecordFrontierHead(ctx, parents)
	return rec, true, err
}

// FolderFrontierRootHashes returns the whole-store root hash of every frontier
// record in this store's `roots/` set. ok=false for a non-folder backend.
func (ddb *DoltDB) FolderFrontierRootHashes(ctx context.Context) (roots []hash.Hash, ok bool, err error) {
	_, dir, ok := ddb.localFolderNBS(ctx)
	if !ok {
		return nil, false, nil
	}
	roots, err = nbs.MultiheadFolderRootHashes(ctx, dir)
	return roots, true, err
}

// MaterializeFolderFrontier compacts the folder's `roots/` frontier into a
// readable union manifest (so every frontier head's chunks are reachable by an
// ordinary read) and rebases this store onto it. It returns ok=false when there
// is no `roots/` set to materialize (a plain single-root remote, or a non-folder
// backend), in which case the caller uses the stock read path.
//
// This is a READ-side compaction: its manifest is deterministic (the union of
// the frontier's specs) and non-authoritative (rebuilt from `roots/` on demand),
// so it does not reintroduce the mutable-head hazard the write path avoids.
func (ddb *DoltDB) MaterializeFolderFrontier(ctx context.Context) (ok bool, err error) {
	_, dir, ok := ddb.localFolderNBS(ctx)
	if !ok {
		return false, nil
	}
	roots, err := nbs.MultiheadFolderRootHashes(ctx, dir)
	if err != nil {
		return false, err
	}
	if len(roots) == 0 {
		return false, nil // no roots/ set: stock single-root remote
	}
	if _, err := nbs.MaterializeFrontierManifest(ctx, dir); err != nil {
		return false, err
	}
	if err := datas.ChunkStoreFromDatabase(ddb.db).Rebase(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// RefFrontierAcrossFolderRoots returns the frontier of |refStr| unioned across
// the given whole-store |roots| (from FolderFrontierRootHashes), dropping any
// tip another names as a parent. The store must already be able to read the
// roots' chunks (call MaterializeFolderFrontier first).
func (ddb *DoltDB) RefFrontierAcrossFolderRoots(ctx context.Context, refStr string, roots []hash.Hash) ([]hash.Hash, error) {
	if err := datas.ValidateDatasetId(refStr); err != nil {
		return nil, err
	}
	return datas.RefFrontierAcrossRoots(ctx, ddb.db.Database, refStr, roots)
}
