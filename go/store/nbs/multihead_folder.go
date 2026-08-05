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

// Lock-free, coordination-free multi-head publish into a SHARED FOLDER — the
// "dumb remote / simultaneous S3 writers" model.
//
// Stock nbs coordinates on two things when it commits: (1) the classic file
// manifest's optimistic root CAS (updateWithChecker: `lastLock != upstream.lock`
// under a transient LOCK), and (2) — for the journaling store — a session-held
// exclusive manifest lock. Both make a shared folder a contended cell: a second
// divergent writer is either rejected (non-fast-forward) or blocked.
//
// This file replaces that coordination for the WRITE path with the
// content-addressed, append-only `roots/` set (multiheadRoots). A writer
// publishes by (a) copying its committed table files into the shared folder and
// (b) appending a `roots/` record naming its parents. Both are pure
// content-addressed, write-once operations (temp + atomic rename), so:
//
//   - No lock is taken on the shared folder, ever.
//   - No CAS can reject a divergent writer: two writers producing different
//     content write different files (both survive as a fork); identical content
//     writes the same file (idempotent). Collision is structurally impossible.
//   - Any number of separate processes/handles may publish SIMULTANEOUSLY.
//
// The current state of the folder is the frontier of the `roots/` DAG
// (MultiheadFolderFrontier). To read or reconcile it, MaterializeFrontierManifest
// compacts the frontier's table specs into a single manifest so an ordinary
// NomsBlockStore opened on the folder can read every head's chunks; the datas
// layer then reconciles the heads with Dolt's real merge (see
// store/datas/MergeTips).
//
// This is the store-level realization of the follow-on flagged in the
// experiment BACKLOG: make `roots/` the coordination point so writers are truly
// concurrent and lock-free, rather than taking turns under the single-root
// manifest.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

// PublishHeadTo publishes this store's current committed head (its root and
// table specs) into the shared folder |sharedDir|: it copies the head's table
// files there (content-addressed, idempotent) and appends a content-addressed
// `roots/` record naming |parents|. It returns the record's content address.
//
// Unlike Commit it takes no manifest lock and no root CAS on the shared folder,
// so any number of writers may call it concurrently; divergent heads all survive
// as a frontier to be reconciled later. The caller should have committed locally
// first (so this store's upstream root and specs reflect the chunks being
// published).
func (nbs *NomsBlockStore) PublishHeadTo(ctx context.Context, sharedDir string, parents []hash.Hash) (hash.Hash, error) {
	nbs.mu.RLock()
	contents := nbs.upstream
	nbs.mu.RUnlock()

	if err := os.MkdirAll(sharedDir, 0o777); err != nil {
		return hash.Hash{}, err
	}

	srcDir := nbs.manifest.Name()
	// Copy every table file this head references. Table files are named by their
	// content hash on disk (fsTablePersister writes dir/<addr>), so this is a
	// content-addressed, write-once, lock-free deposit — safe under concurrent
	// writers depositing the same or different files.
	for _, s := range append(append([]tableSpec(nil), contents.appendix...), contents.specs...) {
		if err := copyContentAddressedFile(srcDir, sharedDir, s.name.String()); err != nil {
			return hash.Hash{}, fmt.Errorf("publish head: copy table file %s: %w", s.name.String(), err)
		}
	}

	mr, err := getMultiheadRoots(sharedDir)
	if err != nil {
		return hash.Hash{}, err
	}
	return mr.Publish(ctx, contents, parents)
}

// copyContentAddressedFile copies srcDir/name to sharedDir/name via a temp file
// and atomic rename. Because name is a content hash the copy is idempotent: if
// the destination already exists it is left as-is (concurrent writers depositing
// the same file do not conflict). A missing source is an error.
func copyContentAddressedFile(srcDir, sharedDir, name string) error {
	dst := filepath.Join(sharedDir, name)
	if _, err := os.Stat(dst); err == nil {
		return nil // already present: content-addressed, so identical
	} else if !os.IsNotExist(err) {
		return err
	}

	in, err := os.Open(filepath.Join(srcDir, name))
	if err != nil {
		return err
	}
	defer in.Close()

	temp, err := tempfiles.MovableTempFileProvider.NewFile(sharedDir, "nbs_tf_")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	if _, err = io.Copy(temp, in); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	// Another writer may have won the race to create dst between our Stat and
	// now; rename is atomic and the content is identical, so overwriting is
	// harmless.
	return file.Rename(tempName, dst)
}

// MultiheadFolderFrontier returns the multi-head frontier of a shared folder:
// the tips of the `roots/` record DAG, each a (root, specs) pair over the shared
// table files. One tip means the writers agree; several is a live fork.
func MultiheadFolderFrontier(ctx context.Context, sharedDir string) ([]manifestContents, error) {
	mr, err := getMultiheadRoots(sharedDir)
	if err != nil {
		return nil, err
	}
	return mr.Roots(ctx)
}

// MultiheadFolderRootHashes returns the Noms root hash of every frontier record
// in a shared folder — one whole-store root per un-superseded `roots/` record.
// manifestContents is unexported, so this is the accessor a higher layer (the
// datas frontier bridge) uses to load each root's dataset AddressMap and read
// the per-ref tips it carries. The order matches MultiheadFolderFrontier (sorted
// by store root), so it is stable and coordination-free.
func MultiheadFolderRootHashes(ctx context.Context, sharedDir string) ([]hash.Hash, error) {
	frontier, err := MultiheadFolderFrontier(ctx, sharedDir)
	if err != nil {
		return nil, err
	}
	roots := make([]hash.Hash, 0, len(frontier))
	for _, c := range frontier {
		roots = append(roots, c.root)
	}
	return roots, nil
}

// MaterializeFrontierManifest compacts the shared folder's frontier into a
// single ordinary nbs manifest: it writes a `manifest` file whose specs are the
// UNION of every frontier head's table specs, so a plain NomsBlockStore opened
// on the folder can read every head's chunks (the reconcile-read prelude). Its
// root is set to one of the heads arbitrarily; a reconciler reads all heads via
// MultiheadFolderFrontier and merges them, then commits/publishes the merged
// head. It returns the materialized contents.
//
// This is the single-reconciler step and MAY take the manifest lock — that is
// fine: the lock-free property is a property of the concurrent WRITERS, not of
// an after-the-fact compaction. It is a no-op (returns the sole head) when the
// folder is not forked.
func MaterializeFrontierManifest(ctx context.Context, sharedDir string) (manifestContents, error) {
	frontier, err := MultiheadFolderFrontier(ctx, sharedDir)
	if err != nil {
		return manifestContents{}, err
	}
	if len(frontier) == 0 {
		return manifestContents{}, fmt.Errorf("materialize frontier: no heads in %s", sharedDir)
	}

	union := make(map[hash.Hash]tableSpec)
	var order []hash.Hash
	for _, h := range frontier {
		for _, s := range append(append([]tableSpec(nil), h.appendix...), h.specs...) {
			if _, ok := union[s.name]; !ok {
				union[s.name] = s
				order = append(order, s.name)
			}
		}
	}
	specs := make([]tableSpec, 0, len(order))
	for _, n := range order {
		specs = append(specs, union[n])
	}

	contents := manifestContents{
		nbfVers: frontier[0].nbfVers,
		root:    frontier[0].root,
		gcGen:   frontier[0].gcGen,
		specs:   specs,
	}
	contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix, nil)

	if err := writeManifestFile(sharedDir, contents); err != nil {
		return manifestContents{}, err
	}
	return contents, nil
}

// writeManifestFile writes a manifest file into dir via temp + atomic rename.
func writeManifestFile(dir string, contents manifestContents) error {
	temp, err := tempfiles.MovableTempFileProvider.NewFile(dir, "nbs_manifest_")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	if err = writeManifest(temp, contents); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	return file.Rename(tempName, filepath.Join(dir, manifestFileName))
}
