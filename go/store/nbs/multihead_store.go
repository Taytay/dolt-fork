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

// Additive wiring between a real NomsBlockStore and the content-addressed,
// multi-head root set (multiheadRoots). These methods do NOT touch the
// single-root commit path (NomsBlockStore.Commit / the manifest CAS); they
// sit alongside it, so the store's existing behavior is unchanged.
//
// The composition they demonstrate is the whole point of the port:
//
//   - Chunks live in shared, content-addressed **table files** in the store
//     directory, exactly as today (the table persister is untouched).
//   - A **head** is a (root, table-specs) pair — i.e. a manifestContents —
//     recorded in the multi-head root DAG under `roots/`.
//
// So publishing a head is CAS-free and coordination-free: any number of
// writers or disk clones can PublishHead into one shared store, producing
// several heads over the same shared table files, to be reconciled later.
// This is the store-level bridge; a full integration would make this the
// primary commit path and expose Roots up through the datas layer (see the
// experiment BACKLOG).

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
)

// PublishHead records this store's current committed state (its root and
// table specs) as a content-addressed root record naming |parents|, and
// returns the record's content address. It is the CAS-free, multi-head
// alternative to advancing the single manifest root.
func (nbs *NomsBlockStore) PublishHead(ctx context.Context, parents []hash.Hash) (hash.Hash, error) {
	nbs.mu.RLock()
	contents := nbs.upstream
	nbs.mu.RUnlock()

	mr, err := getMultiheadRoots(nbs.manifest.Name())
	if err != nil {
		return hash.Hash{}, err
	}
	return mr.Publish(ctx, contents, parents)
}

// MultiheadRoots returns the multi-head frontier recorded alongside this
// store — the tips of the root-record DAG under `roots/`, each a
// (root, specs) pair over the shared table files. One tip means the writers
// agree; several is a live fork awaiting reconciliation.
func (nbs *NomsBlockStore) MultiheadRoots(ctx context.Context) ([]manifestContents, error) {
	mr, err := getMultiheadRoots(nbs.manifest.Name())
	if err != nil {
		return nil, err
	}
	return mr.Roots(ctx)
}
