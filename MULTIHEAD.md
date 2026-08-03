# Multi-head nbs — plan & handoff

Self-contained brief for an agent working **in this fork** on CAS-free,
multi-head, clone-safe storage for Dolt. You do not need any other repo to
start, but the full design lives in `Taytay/taytays_stuff`
(`docs/research/multihead-on-proven-storage-decision.md`,
`experiments/multihead-dolt-nbs/ARCHITECTURE.md`, `ADR-clone-safety.md`) and
the correctness gate (`experiments/merge-conformance`) lives there too — see
"Correctness gate" below.

Work on branch **`claude/multihead-nbs`** (draft PR
https://github.com/Taytay/dolt-fork/pull/1).

## The goal in one paragraph

Stock nbs advances **one root via an optimistic compare-and-swap**
(`store.go` `NomsBlockStore.Commit` → `updateManifest` →
`if nbs.upstream.root != last { return errLastRootMismatch }`, plus the
manifest lock check; the dumb file remote enforces the same in
`file_manifest.go`). That single contended cell needs a CAS the store may
lack on S3, corrupts under concurrent writers on a synced folder, and rejects
non-fast-forward pushes. The goal is a **multi-head** store: writers/clones
publish content-addressed roots with **no coordination**, producing several
heads (a fork) reconciled later by Dolt's existing merge. This makes a dumb
`file://`/S3/Dropbox remote safe, offline-first, and clone-safe.

## What already exists (done, on this branch)

Four new files in `go/store/nbs/` (additive — the single-root path is
untouched):

- `multihead_manifest.go` — `multiheadRoots`: an append-only set of
  **content-addressed root records** under `roots/`, each naming its
  parent(s). `Publish` is append-only + idempotent (no lock, no CAS, **no
  writer id**); `Roots()` returns the frontier (tips of the record DAG),
  de-duped by store root. Table files stay shared and content-addressed.
- `multihead_store.go` — additive `NomsBlockStore.PublishHead(parents)` and
  `MultiheadRoots()`: record/enumerate heads without touching `Commit`.
- `multihead_manifest_test.go`, `multihead_store_test.go` — 4 tests.

### Build & test
```bash
cd go
go build ./store/nbs/
go vet ./store/nbs/
go test ./store/nbs/ -run TestMultihead -count=1 -v
# => ok: ClonedDiskBecomesAFork, PublishIsIdempotent,
#        MergeCollapsesFrontier, IntegratesWithRealStore
```
First build downloads the module deps (~1.5 GB, ~1–2 min). Go 1.24. The Go
module root is `go/`.

## Invariants you must NOT break

1. **No writer id anywhere.** Identity is the content hash. This is what makes
   it clone-safe (a cloned disk becomes another fork, never a silent
   overwrite). Do not reintroduce a per-writer mutable pointer. (`ADR-clone-safety`.)
2. **Supersession is causal, never temporal.** A record is superseded only by
   a descendant that names it as a parent — never by a "newer" timestamp.
   Concurrent records = a fork; keep both heads.
3. **Content-addressed names are pure content.** Do not put wall-clock time in
   a record name (it breaks idempotency). Time, if needed, is a non-authoritative
   body/sidecar hint.
4. **Don't regress the single-root path** until Step 1 deliberately replaces
   it; keep existing nbs tests green (`go test ./store/nbs/`).

## The plan, in order (exact targets verified @ this branch)

### Step 1 — make multi-head the primary commit path
Today `PublishHead`/`MultiheadRoots` sit *beside* the single-root commit.
Wire them in so a normal commit publishes into the `roots/` DAG and the store
exposes multiple heads.
- `go/store/nbs/store.go`: `Commit` (**:1561**) → `updateManifest` (**:1614**),
  where `errLastRootMismatch` (**:1607**, raised **:1616**) and the lock check
  (**:1710**) enforce single-root CAS. Route the commit through
  `multiheadRoots.Publish(upstream, parents)` instead of the CAS root advance,
  and give the store a way to report `Roots()` (a single `Root()` is
  ambiguous under a fork — return the sole tip, or an `ErrMultipleHeads` that
  tells the caller to reconcile).
- `multiheadRoots` deliberately does **not** implement the single-root
  `manifest` interface (that interface *is* the single-root assumption), so
  this is a store-construction change, not a manifest swap.

### Step 2 — datas frontier (a ref becomes a set of tips)
Let a push add a head instead of rejecting a non-fast-forward.
- `go/store/datas/database_common.go`:
  - `BuildNewCommit` (**:505**) — the lineage gate at **:522**
    (`!hasParentHash(opts, headAddr)` → `ErrMergeNeeded`, defined **:45**;
    `hasParentHash` is **:966**). Note `opts.Force` already bypasses it, so
    the gate is optional today.
  - `doCommit` (**:561**) — the ref-level optimistic CAS at **:567**
    (`if curr != datasetCurrentAddr { return ErrMergeNeeded }`), inside
    `db.update` over a `prolly.AddressMap`.
  - `FastForward` (**:340**), `SetHead` (**:202**) for reference.
- Change the root value's `prolly.AddressMap` from `datasetID -> commitAddr`
  to a **set of tips** (or writer-scoped keys), so `doCommit` appends a tip
  rather than erroring. Resolve-later reuses Dolt's existing three-way merge.
- **Validate against merge-conformance** (below) before/after.

### Step 3 — cross-writer GC
Stock conjoin/GC deletes shared table files and assumes one root. Make GC a
mark-sweep from the tips of the `roots/` DAG, and keep conjoin from deleting
another writer's shared table files. Consider the "writer-local,
never-reused monotone id → coordination-free GC" borrow from Quadrable (see
the decision note's Quadrable section).

## Correctness gate: merge-conformance

The portable suite that pins fork/edit/merge semantics is
`experiments/merge-conformance/` (the `mergeconf` Python package) in
`Taytay/taytays_stuff` (currently on PR #197). A Python spike
(`experiments/multihead-store/`) already passes it and is the reference for
the semantics. For Step 2, implement a `mergeconf.MergeDriver` against the
Dolt multi-head path and run its `CASES` + Hypothesis `check_history`. To use
it, attach `taytays_stuff` to your session (same owner) or copy the
`mergeconf` package in; the ducklake driver in that repo
(`experiments/ducklake-branch-merge/tests/test_conformance.py`) is a worked
example of wiring a real store to the suite.

## Environment notes

- Go module root: `go/`. Go 1.24.
- The fork's default `main` tracks `dolthub/dolt`; rebase periodically.
- Keep changes new-file-heavy where possible; Steps 1–2 necessarily edit
  existing files (`store.go`, `database_common.go`) — normal commits here, not
  patches.
