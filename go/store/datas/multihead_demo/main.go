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

// Command multihead_demo is a runnable end-to-end demonstration of CAS-free,
// multi-head, multiwriter Dolt storage in a single local folder — the thing
// stock Dolt cannot do (its single-root manifest CAS rejects a divergent
// writer). It is backed entirely by the real Dolt store; nothing is faked.
//
// Usage:
//
//	go run ./store/datas/multihead_demo [folder]
//
// If no folder is given a temp dir is used and printed. The demo runs two
// phases:
//
//	Phase A — coordination-free storage: N writers, each its own store, publish
//	  divergent heads into ONE shared folder SIMULTANEOUSLY (no lock, no CAS).
//	  All N heads survive as a frontier discovered by LISTing roots/.
//
//	Phase B — versioned relational data: two writers commit divergent tables to
//	  one folder, producing a fork; a sticky provisional head keeps each writer's
//	  view stable (no flip-flop); the fork is reconciled with Dolt's real
//	  three-way merge into a single head.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const ref = "refs/heads/main"

// the demo table: pk id:int64 -> name:string
var (
	keyDesc = val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc})
	valDesc = val.NewTupleDescriptor(val.Type{Enc: val.StringEnc})
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	var base string
	if len(os.Args) > 1 {
		base = os.Args[1]
	} else {
		tmp, err := os.MkdirTemp("", "multihead-demo-")
		if err != nil {
			return err
		}
		base = tmp
	}
	fmt.Printf("working folder: %s\n", base)

	if err := phaseA(ctx, filepath.Join(base, "phaseA-storage")); err != nil {
		return fmt.Errorf("phase A: %w", err)
	}
	if err := phaseB(ctx, filepath.Join(base, "phaseB-relational")); err != nil {
		return fmt.Errorf("phase B: %w", err)
	}
	fmt.Println("\nDone. Two writers shared one folder, no coordination, no data lost.")
	return nil
}

// -------- Phase A: coordination-free concurrent storage --------

func phaseA(ctx context.Context, shared string) error {
	fmt.Println("\n== Phase A: N writers publish to one folder simultaneously (no lock, no CAS) ==")
	if err := os.MkdirAll(shared, 0o777); err != nil {
		return err
	}

	newStore := func(dir string) (*nbs.NomsBlockStore, error) {
		if err := os.MkdirAll(dir, 0o777); err != nil {
			return nil, err
		}
		return nbs.NewLocalStore(ctx, constants.FormatDefaultString, dir, 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
	}

	// Bootstrap a shared base head.
	stBase, err := newStore(filepath.Join(shared, "..", "phaseA-base"))
	if err != nil {
		return err
	}
	baseChunk := chunks.NewChunk([]byte("shared-base"))
	if err := stBase.Put(ctx, baseChunk, noAddrs); err != nil {
		return err
	}
	if _, err := stBase.Commit(ctx, baseChunk.Hash(), hash.Hash{}); err != nil {
		return err
	}
	rec0, err := stBase.PublishHeadTo(ctx, shared, nil)
	if err != nil {
		return err
	}
	_ = stBase.Close()
	fmt.Printf("  bootstrapped shared base, record %s\n", short(rec0))

	const N = 4
	dirs := make([]string, N)
	for i := range dirs {
		dirs[i] = filepath.Join(shared, "..", fmt.Sprintf("phaseA-w%d", i))
	}
	roots := make([]hash.Hash, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // all writers fire at once
			st, err := newStore(dirs[i])
			if err != nil {
				errs[i] = err
				return
			}
			defer st.Close()
			c := chunks.NewChunk([]byte(fmt.Sprintf("writer-%d-edit", i)))
			if err := st.Put(ctx, c, noAddrs); err != nil {
				errs[i] = err
				return
			}
			if _, err := st.Commit(ctx, c.Hash(), hash.Hash{}); err != nil {
				errs[i] = err
				return
			}
			roots[i] = c.Hash()
			if _, err := st.PublishHeadTo(ctx, shared, []hash.Hash{rec0}); err != nil {
				errs[i] = err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			return fmt.Errorf("writer %d: %w", i, e)
		}
	}

	frontier, err := nbs.MultiheadFolderFrontier(ctx, shared)
	if err != nil {
		return err
	}
	fmt.Printf("  %d writers published concurrently -> frontier has %d heads (none rejected, none clobbered)\n", N, len(frontier))
	if len(frontier) != N {
		return fmt.Errorf("expected %d heads, got %d", N, len(frontier))
	}
	fmt.Println("  (stock Dolt would have rejected all but one with a non-fast-forward CAS)")
	return nil
}

// -------- Phase B: versioned relational fork + real merge + sticky head --------

func phaseB(ctx context.Context, shared string) error {
	fmt.Println("\n== Phase B: two writers fork a table in one folder, reconciled by Dolt's real merge ==")
	if err := os.MkdirAll(shared, 0o777); err != nil {
		return err
	}

	open := func() (datas.Database, func() error, error) {
		st, err := nbs.NewLocalStore(ctx, constants.FormatDefaultString, shared, 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
		if err != nil {
			return nil, nil, err
		}
		db := datas.NewDatabase(st)
		if err := datas.EnableMultihead(db); err != nil {
			return nil, nil, err
		}
		return db, st.Close, nil
	}

	commit := func(db datas.Database, ds datas.Dataset, rows map[int64]string, parents ...hash.Hash) (hash.Hash, error) {
		m, err := buildTable(ctx, db, rows)
		if err != nil {
			return hash.Hash{}, err
		}
		ds2, err := db.Commit(ctx, ds, tree.ValueFromNode(m.Node()), datas.CommitOptions{Parents: parents, Meta: pinnedMeta()})
		if err != nil {
			return hash.Hash{}, err
		}
		addr, _ := ds2.MaybeHeadAddr()
		return addr, nil
	}

	// Session 0: bootstrap the base table.
	db0, close0, err := open()
	if err != nil {
		return err
	}
	ds0, err := db0.GetDataset(ctx, ref)
	if err != nil {
		return err
	}
	base, err := commit(db0, ds0, map[int64]string{1: "alice", 2: "bob"})
	if err != nil {
		return err
	}
	_ = close0()
	fmt.Printf("  base committed: {1:alice, 2:bob}  head=%s\n", short(base))

	// Writer A: adds a row (fast-forward child of base).
	dbA, closeA, err := open()
	if err != nil {
		return err
	}
	dsA, err := dbA.GetDataset(ctx, ref)
	if err != nil {
		return err
	}
	aHead, err := commit(dbA, dsA, map[int64]string{1: "alice", 2: "bob", 3: "carol"})
	if err != nil {
		return err
	}
	_ = closeA()
	fmt.Printf("  writer A: +{3:carol}                head=%s\n", short(aHead))

	// Writer B: had synced at base, edited offline -> a divergent child of base.
	dbB, closeB, err := open()
	if err != nil {
		return err
	}
	dsB, err := dbB.GetDataset(ctx, ref)
	if err != nil {
		return err
	}
	bHead, err := commit(dbB, dsB, map[int64]string{1: "alice", 2: "BOB", 4: "dave"}, base)
	if err != nil {
		return err
	}
	_ = closeB()
	fmt.Printf("  writer B: 2->BOB, +{4:dave}         head=%s (diverged from base)\n", short(bHead))

	// Reconciler opens the folder and sees the fork.
	db, closeR, err := open()
	if err != nil {
		return err
	}
	defer closeR()

	_, err = db.GetDataset(ctx, ref)
	fmt.Printf("  GetDataset on the shared ref -> %v\n", err)

	tips, err := datas.Tips(ctx, db, ref)
	if err != nil {
		return err
	}
	fmt.Printf("  frontier: %d heads %s\n", len(tips), shortAll(tips))

	// Sticky provisional head: writer A's view does not flip-flop to B's tip.
	aResolved, forked, err := datas.ResolveHead(ctx, db, ref, aHead)
	if err != nil {
		return err
	}
	fmt.Printf("  writer A's working head (sticky): %s  forked=%v  <- unchanged by B's tip\n", short(aResolved), forked)
	canon, _, err := datas.ResolveHead(ctx, db, ref, hash.Hash{})
	if err != nil {
		return err
	}
	fmt.Printf("  a fresh reader's canonical head:  %s  (deterministic, coordination-free)\n", short(canon))

	// Reconcile with Dolt's real three-way merge.
	var conflicts []int64
	collide := func(l, r tree.Diff) (tree.Diff, bool) {
		id, _ := keyDesc.GetInt64(0, val.Tuple(l.Key))
		conflicts = append(conflicts, id)
		return l, true // "ours" on conflict; none expected here
	}
	merged, err := datas.MergeTips(ctx, db, aHead, bHead, keyDesc, valDesc, collide)
	if err != nil {
		return err
	}
	mergedRows, err := readTable(ctx, merged)
	if err != nil {
		return err
	}
	fmt.Printf("  merged (real 3-way merge): %s  conflicts=%v\n", fmtRows(mergedRows), conflicts)

	mergeAddr, err := datas.AppendMapCommit(ctx, db, ref, merged, datas.CommitOptions{Parents: []hash.Hash{aHead, bHead}, Meta: pinnedMeta()})
	if err != nil {
		return err
	}
	tips, err = datas.Tips(ctx, db, ref)
	if err != nil {
		return err
	}
	fmt.Printf("  recorded merge %s -> frontier collapses to %d head\n", short(mergeAddr), len(tips))

	final, err := db.GetDataset(ctx, ref)
	if err != nil {
		return err
	}
	fa, _ := final.MaybeHeadAddr()
	fmt.Printf("  GetDataset now resolves cleanly to %s\n", short(fa))
	return nil
}

// -------- small table helpers (real prolly maps) --------

func buildTable(ctx context.Context, db datas.Database, rows map[int64]string) (prolly.Map, error) {
	ns, err := datas.NodeStore(db)
	if err != nil {
		return prolly.Map{}, err
	}
	ids := make([]int64, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	pool := ns.Pool()
	var tups []val.Tuple
	for _, id := range ids {
		kb := val.NewTupleBuilder(keyDesc, ns)
		kb.PutInt64(0, id)
		k, err := kb.Build(ctx, pool)
		if err != nil {
			return prolly.Map{}, err
		}
		vb := val.NewTupleBuilder(valDesc, ns)
		if err := vb.PutString(0, rows[id]); err != nil {
			return prolly.Map{}, err
		}
		v, err := vb.Build(ctx, pool)
		if err != nil {
			return prolly.Map{}, err
		}
		tups = append(tups, k, v)
	}
	return prolly.NewMapFromTuples(ctx, ns, keyDesc, valDesc, tups...)
}

func readTable(ctx context.Context, m prolly.Map) (map[int64]string, error) {
	it, err := m.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	out := map[int64]string{}
	for {
		k, v, err := it.Next(ctx)
		if err == io.EOF || k == nil {
			break
		}
		if err != nil {
			return nil, err
		}
		id, _ := keyDesc.GetInt64(0, k)
		name, _ := valDesc.GetString(0, v)
		out[id] = name
	}
	return out, nil
}

func pinnedMeta() *datas.CommitMeta {
	epoch := datas.CommitDateAt(time.UnixMilli(0))
	return &datas.CommitMeta{Author: datas.CommitIdent{Date: epoch}, Committer: datas.CommitIdent{Date: epoch}}
}

func noAddrs(c chunks.Chunk) chunks.InsertAddrsCb {
	return func(ctx context.Context, addrs hash.HashSet, exists chunks.PendingRefExists) error { return nil }
}

func short(h hash.Hash) string {
	s := h.String()
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

func shortAll(hs []hash.Hash) string {
	parts := make([]string, len(hs))
	for i, h := range hs {
		parts[i] = short(h)
	}
	return "[" + join(parts, " ") + "]"
}

func fmtRows(rows map[int64]string) string {
	ids := make([]int64, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d:%s", id, rows[id]))
	}
	return "{" + join(parts, ", ") + "}"
}

func join(parts []string, sep string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += sep
		}
		out += p
	}
	return out
}
