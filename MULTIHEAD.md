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

**Step 1 — nbs roots layer.** Four new files in `go/store/nbs/` (additive —
the single-root path is untouched):

- `multihead_manifest.go` — `multiheadRoots`: an append-only set of
  **content-addressed root records** under `roots/`, each naming its
  parent(s). `Publish` is append-only + idempotent (no lock, no CAS, **no
  writer id**); `Roots()` returns the frontier (tips of the record DAG),
  de-duped by store root. Table files stay shared and content-addressed.
- `multihead_store.go` — additive `NomsBlockStore.PublishHead(parents)` and
  `MultiheadRoots()`: record/enumerate heads without touching `Commit`.
- `multihead_manifest_test.go`, `multihead_store_test.go` — 4 tests.

**Step 2 — datas frontier (a ref becomes a set of tips).** Additive files in
`go/store/datas/` — the single-root `doCommit`/`BuildNewCommit` path is
untouched (`TestMultiheadDatasets_SingleRootPathUnaffected` guards it):

- `multihead_datasets.go` — `AppendCommit(ref, v, opts)` builds a commit with
  **no fast-forward/lineage gate** (it calls `newCommitForValue` directly, the
  Step-2 relaxation of `ErrMergeNeeded`) and records it as a tip;
  `RecordTip(ref, addr)` publishes an existing commit as a tip; `Tips(ref)`
  returns the frontier of that ref's commit DAG; `TipValue` reads a tip's
  committed value. Tips are **content-addressed sub-keys of the ref in the same
  root `prolly.AddressMap`** — so a ref holds a *set of tips* with no writer id
  and no CAS on the ref. (This realizes "a ref becomes a set of tips"
  *additively*, rather than changing the AddressMap's value type in place —
  which keeps every existing datas test green. See "A note on the design" below.)
- `multihead_datasets_test.go` — 6 tests: fork → two tips with no
  `ErrMergeNeeded` (the anti-CAS proof); fast-forward keeps one tip; a merge
  collapses the frontier; append/record idempotent; refs isolated; single-root
  path unaffected.
- `multihead_conf/main.go` — a small harness exposing the merge-conformance
  world over JSON, backed by the real datas layer, for the portable gate.

### Build & test
```bash
cd go
go build ./store/nbs/ ./store/datas/ ./store/datas/multihead_conf/
go vet ./store/nbs/ ./store/datas/
go test ./store/nbs/  -run TestMultihead         -count=1 -v   # Step 1: 4 tests
go test ./store/datas/ -run TestMultiheadDatasets -count=1 -v   # Step 2: 6 tests
```
First build downloads the module deps (~1.5 GB, ~1–2 min). Go 1.24. The Go
module root is `go/`. (The full `./store/nbs/` suite is heavy and can be killed
by a short CI timeout on its conjoin stress tests — unrelated to these changes;
use the `-run TestMultihead*` targets for a fast signal.)

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

### Step 2 — datas frontier (a ref becomes a set of tips) — DONE (additive), gate green
Implemented additively in `multihead_datasets.go` (see "What already exists").
A ref holds a set of tips via content-addressed sub-keys in the root
`prolly.AddressMap`; `AppendCommit` bypasses the lineage gate by calling
`newCommitForValue` directly, so a divergent commit adds a tip instead of
returning `ErrMergeNeeded`. **Validated against the portable merge-conformance
suite** (15 pinned CASES + Hypothesis differential, all policies) — see the
gate section below.

Reference points in the stock single-root path (still intact, for whoever does
Step 2b — making multi-head the *primary* commit path):
- `go/store/datas/database_common.go`:
  - `BuildNewCommit` (**:505**) — the lineage gate at **:522**
    (`!hasParentHash(opts, headAddr)` → `ErrMergeNeeded`, defined **:45**;
    `hasParentHash` is **:966**). `opts.Force` already bypasses it.
  - `doCommit` (**:561**) — the ref-level optimistic CAS at **:567**
    (`if curr != datasetCurrentAddr { return ErrMergeNeeded }`), inside
    `db.update` over a `prolly.AddressMap`.
  - `FastForward` (**:340**), `SetHead` (**:202**) for reference.

**A note on the design.** MULTIHEAD.md originally proposed changing the
AddressMap's *value type* from `commitAddr` to a set. That would ripple through
every dataset reader (`datasetFromMap`, refspec resolution, SQL, GC) and can't
be done without regressing the single-root path — violating invariant #4. The
additive realization (content-addressed sub-keys `<ref>\x00multihead-tip\x00<hash>`
in the *same* map) gives a ref a set of tips with no value-type change, no
writer id, and no CAS — and leaves every existing datas test green. Step 2b
(below) is where the *primary* commit path is switched over.

### Step 2b — make multi-head the primary datas commit path
`AppendCommit`/`Tips` are additive today (like `PublishHead` in Step 1). To make
a normal `Commit` publish a tip and expose multiple heads: route `doCommit`
through `recordTip` (drop the `curr != datasetCurrentAddr` gate) and teach
`datasetFromMap`/`GetDataset` to surface the frontier (a single `HeadAddr` is
ambiguous under a fork — return the sole tip or an `ErrMultipleHeads`). Also
wire **Dolt's real SQL row-merge engine** (`libraries/doltcore/merge`) into the
resolve path; the conformance harness currently uses a model three-way to
validate the frontier/resolve plumbing.

### Step 3 — cross-writer GC
Stock conjoin/GC deletes shared table files and assumes one root. Make GC a
mark-sweep from the tips of the `roots/` DAG, and keep conjoin from deleting
another writer's shared table files. Consider the "writer-local,
never-reused monotone id → coordination-free GC" borrow from Quadrable (see
the decision note's Quadrable section).

## Correctness gate: merge-conformance

The portable suite that pins fork/edit/merge semantics is
`experiments/merge-conformance/` (the `mergeconf` Python package) in
`Taytay/taytays_stuff` (currently on PR #197). **Step 2 is wired to it and
passes:** the `multihead_conf` harness here plus the driver in
`taytays_stuff/experiments/multihead-dolt-nbs/conformance/` run the real Dolt
multi-tip datas layer through all 15 pinned CASES and the Hypothesis
differential check (`fail`/`ours`/`theirs`/`record`).

```bash
cd go && go build -o /tmp/multihead_conf ./store/datas/multihead_conf
export MH_CONF_BIN=/tmp/multihead_conf
export PYTHONPATH=<taytays_stuff>/experiments/merge-conformance
cd <taytays_stuff>/experiments/multihead-dolt-nbs/conformance && python -m pytest -v
# => 15 CASES + 1 property test, all green
```

To use it, attach `taytays_stuff` to your session (same owner) or copy the
`mergeconf` package in; the ducklake driver in that repo
(`experiments/ducklake-branch-merge/tests/test_conformance.py`) is another
worked example. Note the harness's resolve step uses a model three-way
(mirroring `mergeconf/model.py`); Step 2b wires Dolt's real SQL merge engine.

## Environment notes

- Go module root: `go/`. Go 1.24.
- The fork's default `main` tracks `dolthub/dolt`; rebase periodically.
- Keep changes new-file-heavy where possible; Steps 1–2 necessarily edit
  existing files (`store.go`, `database_common.go`) — normal commits here, not
  patches.
