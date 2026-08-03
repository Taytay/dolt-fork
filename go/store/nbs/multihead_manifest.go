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

// multiheadRoots is a CAS-free, multi-head, **id-free** replacement for the
// single mutable root that stock nbs advances with an optimistic
// compare-and-swap (see NomsBlockStore.Commit -> errLastRootMismatch, and
// updateWithChecker's `lastLock != upstream.lock` gate).
//
// Motivation and design: taytays_stuff/docs/research/
// multihead-on-proven-storage-decision.md and the ADR in
// experiments/multihead-dolt-nbs/. An earlier draft partitioned a *mutable*
// manifest by writer id (`writers/<writerId>/manifest`). That reintroduced
// an id-uniqueness assumption: if a machine + disk is cloned, both copies
// share the writer id and clobber each other's manifest (silently, on a
// synced folder). This version removes the assumption entirely.
//
// The state of the store is an append-only set of **content-addressed root
// records** under `roots/`. Each record names its parent record(s), forming
// a DAG; the current heads are the records no other record points at
// (`Roots`). There is no writer id anywhere:
//
//   - A record's name is a hash of its content (root + table specs +
//     parents), so two machines writing the *same* content produce the
//     *same* file (idempotent) and *different* content produces *different*
//     files (both survive). Collision is structurally impossible.
//   - A cloned disk is therefore not a hazard but an ordinary fork: the
//     clone appends a different record, the frontier becomes two heads, and
//     the existing multi-head machinery reconciles it later. No silent loss,
//     no id to duplicate.
//   - Table files stay shared and content-addressed in the store root, as
//     before.
//
// Note on the interface: this deliberately does NOT implement the
// single-root `manifest` interface, because that interface bakes in "one
// root" — the very assumption multi-head drops. Wiring it under the store
// (a per-handle tracked head, or the datas layer reading Roots directly) is
// the integration step; see the experiment BACKLOG.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

const rootsDirName = "roots"

// multiheadRoots is a handle on the append-only, content-addressed set of
// root records of a shared store. It carries no writer id, so any number of
// handles (including disk clones) may share one store safely.
type multiheadRoots struct {
	root string // shared store dir; table files and roots/ live here
}

func getMultiheadRoots(root string) (*multiheadRoots, error) {
	if err := os.MkdirAll(filepath.Join(root, rootsDirName), 0o777); err != nil {
		return nil, err
	}
	return &multiheadRoots{root: root}, nil
}

func (m *multiheadRoots) dir() string {
	return filepath.Join(m.root, rootsDirName)
}

// recordName is the content address of a record: a hash over the store root,
// its table specs, and its parents. Parents are part of identity (a node
// with different parents is a different DAG node); wall-clock time is NOT —
// keeping it out of the name is what preserves idempotency and clone-safety
// (see the ADR's discussion of metadata-in-names).
func recordName(contents manifestContents, parents []hash.Hash) hash.Hash {
	sorted := append([]hash.Hash(nil), parents...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i][:], sorted[j][:]) < 0
	})
	var extra []byte
	for _, p := range sorted {
		extra = append(extra, p[:]...)
	}
	return generateLockHash(contents.root, contents.specs, contents.appendix, extra)
}

// Publish appends a content-addressed root record naming |parents| and
// returns its content address. It is append-only and idempotent: publishing
// identical content is a no-op that returns the same address. No lock, no
// CAS, no writer id — safe on the dumbest store.
func (m *multiheadRoots) Publish(ctx context.Context, contents manifestContents, parents []hash.Hash) (hash.Hash, error) {
	if contents.lock.IsEmpty() {
		contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix, nil)
	}
	name := recordName(contents, parents)
	path := filepath.Join(m.dir(), name.String())
	if _, err := os.Stat(path); err == nil {
		return name, nil // already published: idempotent
	} else if !os.IsNotExist(err) {
		return hash.Hash{}, err
	}

	// Body: a parents line (comma-separated addresses) then the standard v5
	// manifest for the contents.
	sorted := append([]hash.Hash(nil), parents...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i][:], sorted[j][:]) < 0
	})
	var body bytes.Buffer
	for i, p := range sorted {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(p.String())
	}
	body.WriteByte('\n')
	if err := writeManifest(&body, contents); err != nil {
		return hash.Hash{}, err
	}

	// Content-addressed name => write once via temp + rename.
	temp, err := tempfiles.MovableTempFileProvider.NewFile(m.dir(), "nbs_root_")
	if err != nil {
		return hash.Hash{}, err
	}
	tempName := temp.Name()
	if _, err = temp.Write(body.Bytes()); err != nil {
		temp.Close()
		return hash.Hash{}, err
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return hash.Hash{}, err
	}
	if err = temp.Close(); err != nil {
		return hash.Hash{}, err
	}
	if err = file.Rename(tempName, path); err != nil {
		return hash.Hash{}, err
	}
	return name, nil
}

// getRecord parses one record file, returning its contents and parents.
func (m *multiheadRoots) getRecord(name hash.Hash) (manifestContents, []hash.Hash, error) {
	data, err := os.ReadFile(filepath.Join(m.dir(), name.String()))
	if err != nil {
		return manifestContents{}, nil, err
	}
	nl := bytes.IndexByte(data, '\n')
	if nl < 0 {
		return manifestContents{}, nil, ErrCorruptManifest
	}
	var parents []hash.Hash
	if line := strings.TrimSpace(string(data[:nl])); line != "" {
		for _, s := range strings.Split(line, ",") {
			p, ok := hash.MaybeParse(s)
			if !ok {
				return manifestContents{}, nil, ErrCorruptManifest
			}
			parents = append(parents, p)
		}
	}
	contents, err := parseManifest(bytes.NewReader(data[nl+1:]))
	if err != nil {
		return manifestContents{}, nil, err
	}
	return contents, parents, nil
}

// Roots returns the multi-head frontier: the contents of every record no
// other record names as a parent, de-duplicated by store root. One element
// means the writers agree; more than one is a live fork to reconcile later.
func (m *multiheadRoots) Roots(ctx context.Context) ([]manifestContents, error) {
	entries, err := os.ReadDir(m.dir())
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	type rec struct {
		contents manifestContents
		parents  []hash.Hash
	}
	recs := make(map[hash.Hash]rec)
	parented := make(map[hash.Hash]struct{})
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name, ok := hash.MaybeParse(e.Name())
		if !ok {
			continue // skip temp files and anything not a content address
		}
		contents, parents, err := m.getRecord(name)
		if err != nil {
			return nil, err
		}
		recs[name] = rec{contents, parents}
		for _, p := range parents {
			parented[p] = struct{}{}
		}
	}

	seenRoot := make(map[hash.Hash]struct{})
	var tips []manifestContents
	for name, r := range recs {
		if _, isParent := parented[name]; isParent {
			continue // superseded by a descendant record
		}
		if _, dup := seenRoot[r.contents.root]; dup {
			continue
		}
		seenRoot[r.contents.root] = struct{}{}
		tips = append(tips, r.contents)
	}
	return tips, nil
}
