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

**Step 2b — reconcile a fork with Dolt's *real* three-way merge.** Additive
files in `go/store/datas/`:

- `multihead_merge.go` — `AppendMapCommit`/`TipMap` store & load a table as a
  real `prolly.Map` tip (committed value = the map's root node, so its chunks
  are referenced), and `MergeTips(ours, theirs, kd, vd, collide)` reconciles a
  two-tip fork with **`prolly.MergeMaps`** — the same tree-level three-way merge
  Dolt's SQL row merge is built on — using `FindCommonAncestor` for the base.
  The merge policy is the caller's `tree.CollisionFn`. `NodeStore(db)` exposes
  the store for building maps.
- `multihead_merge_test.go` — 4 tests: disjoint clean auto-merge, update/update
  under each policy, update-vs-delete, and a no-common-ancestor (empty base)
  merge — over real prolly maps, asserting merged contents + frontier collapse.
- `multihead_conf/main.go` now stores the table as a real `prolly.Map` and
  resolves merges via `MergeTips` (Dolt's real merge), replacing the model
  three-way. The portable `mergeconf` suite (15 CASES + Hypothesis differential,
  all policies) passes against it.

**Step 2c — multi-head as the PRIMARY commit path.** Additive files +
mode-gated branches in `go/store/datas/`:

- `multihead_primary.go` — `EnableMultihead(db)` turns a database's *ordinary*
  `Commit`/`GetDataset` API multi-head: `Commit` publishes the new commit as a
  tip with no fast-forward gate and no CAS on the ref (a divergent commit adds a
  head instead of `ErrMergeNeeded`; a sequential commit names the observed head,
  so the frontier fast-forwards back to one tip), and `GetDataset` resolves the
  ref's frontier — the sole tip, or `ErrMultipleHeads` when forked. The mode is
  **off by default**, so a database opened normally is byte-identical to stock
  (invariant #4). The branches in `database_common.go` (`Commit`,
  `datasetFromMap`) are one guarded `if db.multihead` each; `tips` was refactored
  to a reusable `frontierOf(am, ref)` so the primary path resolves against the
  exact root map it holds.
- `multihead_primary_test.go` — 4 tests: a fork through the *normal* `Commit`
  API leaves two tips with no `ErrMergeNeeded` and `GetDataset` returns
  `ErrMultipleHeads`; sequential commits fast-forward to one head; a reconcile
  collapses the fork and `GetDataset` resolves again; and the default (non-
  multihead) database still enforces the single-root CAS.

**SQL/relational merge engine — reconcile a whole RootValue with Dolt's real
`MergeRoots`.** `prolly.MergeMaps` (Step 2b) merges ONE keyed map; a real tip is
a whole `RootValue` (many tables, schemas, indexes, constraints). Composing the
per-map merge across a root is `libraries/doltcore/merge.MergeRoots` — what
`dolt merge` itself calls. `store/datas` cannot import `doltcore` (the layering
runs the other way), so this tier is validated one layer up:

- `go/libraries/doltcore/merge/multihead_reconcile_test.go` — frames `ours`/
  `theirs` as two tips of a forked ref and reconciles them with the **real
  `MergeRoots`**, over the fidelity `prolly.MergeMaps` alone cannot reach: 3
  tests — multiple tables with disjoint edits (clean auto-merge across the whole
  root), a **schema change** (add nullable column) on one side merged with row
  edits on the other, and a row-level **conflict** recorded into the merged
  root's conflict table. Reuses the package's own harness (`sch`/`tbl`/
  `verifyMerge`) plus a `mhRootWithTables` multi-table root builder.

**End-to-end: one shared folder, two writers, a head each, reconcile later.**
The demo the whole design is for — on a **real on-disk store**, not `TestStorage`:

- `go/store/nbs/multihead_e2e_test.go` — `TestMultihead_SharedFolderTwoWritersOneHeadEach`:
  one folder is the shared store; two writers each derive from a common base and
  `PublishHead` into the append-only, content-addressed `roots/` set over shared
  table files; the folder ends with **two heads discovered by LISTing `roots/`**,
  and both heads' chunks read back from the one shared store (nothing clobbered).
- `go/store/datas/multihead_e2e_test.go` — `TestMultihead_SharedFolderReconcileEndToEnd`:
  the relational end-to-end. Two writers commit divergently through the
  **primary multi-head `Commit`** API on a real shared folder (no
  `ErrMergeNeeded`); `GetDataset` reports the fork as `ErrMultipleHeads`; a
  reconciler three-way merges the tips with Dolt's real `prolly.MergeMaps` and
  records the merged tip, collapsing the fork to one head.
- **Concurrency honesty (for the two tests above):** they open the same folder
  in turn (a synced-folder model). Truly *simultaneous* lock-free writers are the
  next item.

**Simultaneous, lock-free writers — `roots/` as the coordination point.** The
follow-on that removes the take-turns caveat, so many writers publish into one
shared folder at once with no lock and no CAS:

- `go/store/nbs/multihead_folder.go` — `PublishHeadTo(ctx, sharedDir, parents)`
  publishes a store's committed head into a shared folder by (a) copying its
  content-addressed table files there (write-once temp+rename, idempotent) and
  (b) appending a `roots/` record — both lock-free, so concurrent writers never
  block or get CAS-rejected. `MultiheadFolderFrontier(sharedDir)` LISTs the
  frontier; `MaterializeFrontierManifest(sharedDir)` compacts the frontier's
  table specs into one manifest so a plain `NomsBlockStore` opened on the folder
  reads every head's chunks (the reconcile-read prelude — a single-reconciler
  step that may take the lock; the *writers* never do).
- `go/store/nbs/multihead_concurrent_test.go`:
  - `TestMultihead_ConcurrentWritersNoLock` — N=8 writers, each its own store,
    all fire off a barrier and `PublishHeadTo` one shared folder **at once**; the
    frontier is exactly the N heads and every head's chunks read back. Passes
    under `-race` (10×).
  - `TestMultihead_StockSingleRootRejectsConcurrentDivergent` — the contrast:
    two stock stores committing divergently off one base concurrently, and the
    single-root manifest CAS admits **exactly one**. This is the coordination
    `roots/` removes.
- **Why this is the real thing:** the file-manifest store permits concurrent
  *opens* (it coordinates at `Commit`, not at open); the only contended cell was
  the root CAS, and `PublishHeadTo` sidesteps it entirely by writing only
  content-addressed, write-once files. That is the S3/dumb-remote property: no
  locks, writers never coordinate, divergence is preserved as a frontier and
  reconciled later. **What still remains:** making this the *default* `Commit`
  path Dolt-wide (every reader taught to expect a frontier) — the store seam is
  now here (`PublishHeadTo` + `MultiheadFolderFrontier`).

**Choosing a working head without merging (no flip-flop).** A reader often wants
one head to proceed on while the fork is unresolved. `store/datas/multihead_head.go`:

- `ResolveHead(ctx, db, ref, preferred)` — **sticky**: if the caller's current
  head is still a tip it is kept (it does not move just because another writer's
  tip appeared — the flip-flop hazard); only once it is superseded does it fall
  back to the canonical tip. `forked` reports "unmerged work exists." A fresh
  reader (empty `preferred`) gets `CanonicalTip` — the lowest-hash tip, which
  every reader agrees on with no coordination. Neither pick merges or drops a
  tip, and neither supersedes by time (invariant #2). `Tips` remains the truth.
- `multihead_head_test.go` — proves a writer's head stays put as other tips
  arrive, and falls back only after its head is merged away.

**Runnable end-to-end demo.** `go run ./store/datas/multihead_demo [folder]` — a
real program (real store, real merge) that (A) fires N writers into one folder
simultaneously and shows all N heads survive, then (B) forks a table across two
writers, shows the sticky head, and reconciles with Dolt's real three-way merge
into a single head. This is "multiwriter versioned Dolt in a local folder,"
runnable today via the store API.

**CLI/SQL toggle — `DOLT_MULTIHEAD=1`.** An env var opens databases in
multi-head mode Dolt-wide, keeping stock behavior byte-identical when unset:

- `libraries/doltcore/dconfig/envvars.go` — `EnvMultihead = "DOLT_MULTIHEAD"`.
- `libraries/doltcore/doltdb/doltdb.go` — `LoadDoltDBWithParams` (and
  `DoltDBFromCS`) call `datas.EnableMultiheadResolve(db)` when the env var is
  set. This is the load path for on-disk/remote databases, so it covers the
  whole CLI/SQL stack from one place.
- `store/datas/multihead_primary.go` — `EnableMultiheadResolve` turns on
  **fork-tolerant reads**: `GetDataset` resolves a fork to the canonical
  (lowest-hash) head instead of `ErrMultipleHeads`, and `Datasets`/
  `DatasetsByRootHash` present each ref once (a `multiheadDatasetsMap` collapses
  the internal tip sub-keys so branch/tag enumeration never sees them). The CLI
  write path `CommitWithWorkingSet` gets a multi-head branch
  (`commitWithWorkingSetMultihead`) that records the commit as a tip (no head
  CAS) while updating the working set atomically.
- `libraries/doltcore/doltdb/doltdb.go` — `(*DoltDB).MultiheadTips(ref)` exposes
  the frontier; `libraries/doltcore/sqle/dfunctions/frontier.go` —
  `dolt_frontier()` SQL function surfaces the current branch's tips.
  `dolt_frontier('<ref>')` takes an optional ref argument, so a remote-tracking
  ref's frontier is inspectable after a fetch —
  `dolt_frontier('refs/remotes/origin/main')`.

Verified end-to-end against a freshly built `dolt` binary: `DOLT_MULTIHEAD=1
dolt init / sql / add / commit / log / status / branch` all work; a commit
records a frontier tip (`select dolt_frontier()` shows it); sequential commits
fast-forward. Tests: `store/datas` unit tests for resolve-mode reads
(`TestMultiheadResolve_*`), the collapsing enumeration, and the CLI write path
(`TestMultiheadCommitWithWorkingSet`, incl. a stale-head fork with no
`ErrMergeNeeded`).

**Multi-head fetch/push — two CLIs sharing a folder remote fork and reconcile.**
The follow-on that removes the toggle's last "known limit": with multi-head push
a *divergent* push FORKS the remote instead of being rejected as non-fast-forward,
and fetch brings the whole frontier home. Because `DOLT_MULTIHEAD` opens a
`file://` remote multi-head through the same `LoadDoltDBWithParams` hook, two
writers sharing one folder remote each land a head.

- `libraries/doltcore/doltdb/doltdb.go` — `(*DoltDB).RecordTip(ref, addr)` (the
  multi-head analogue of `SetHeadToCommit`/`FastForward`: append a tip, no CAS,
  no fast-forward gate) and `(*DoltDB).IsMultihead()`.
- `libraries/doltcore/env/actions/multihead_remotes.go` — `pushMultihead` and
  `fetchRefSpecsMultihead`. Push transfers the commit's chunk closure (unchanged
  `PullChunks`, content-addressed) then records it as a tip on the remote and on
  the local tracking ref — no fast-forward gate. Fetch pulls EVERY tip of each
  remote ref's frontier (`MultiheadTips`) and records them all on the tracking
  ref, so the other writer's fork becomes visible locally. The stock single-root
  path in `remotes.go` is untouched; these run only when the relevant
  `IsMultihead()` is true (branch points in `actions.Push` and
  `fetchRefSpecsWithDepth`). Chunk transfer is head-agnostic and reused as-is;
  only the ref-update discipline (a frontier of tips vs. one CAS'd head) differs.
- Tests: `libraries/doltcore/env/actions/multihead_remotes_test.go`
  (`TestPushMultihead_DivergentPushForks`: two children of one base pushed in turn
  leave the remote with a two-tip frontier, a child push collapses it, and a third
  writer's fetch mechanics bring the whole fork home) and
  `integration-tests/bats/multihead-remotes.bats` (the CLI story). Verified
  end-to-end against a built `dolt` binary: writer B's divergent `dolt push`
  succeeds where stock prints "non-fast-forward / integrate the remote changes
  before pushing again", and after `dolt fetch` writer A sees both tips in
  `dolt_frontier('refs/remotes/origin/main')`.

**Frontier-aware reconcile — collapse a fork into one head through the CLI.**
The other half of the round trip: after push/fetch produce a fork,
`dolt_reconcile()` three-way merges the frontier back into a single head with the
same engine `dolt merge` uses.

- `libraries/doltcore/merge/multihead_reconcile.go` — `ReconcileFrontier(ctx,
  ddb, branchRef, meta, eo)`: reads the ref's frontier (`MultiheadTips`), folds
  the tips through the real `merge.MergeRoots` (base = each tip's common ancestor,
  `GetCommitAncestor`), writes the merged `RootValue`, and records ONE commit
  naming every tip as a parent — so `datas.Tips` drops them and the frontier
  collapses to that single merged head. It reports conflict/violation counts; a
  conflicted reconcile still collapses (the conflicts are recorded in the merged
  commit, resolvable with `dolt conflicts` / `dolt_conflicts_resolve`). Lives in
  the `merge` package because it calls `MergeRoots` (which imports `doltdb`).
- `libraries/doltcore/sqle/dprocedures/dolt_reconcile.go` — the `dolt_reconcile()`
  stored procedure (the frontier analogue of `dolt merge`, callable as
  `dolt sql -q "call dolt_reconcile()"`). It reconciles the current branch's
  frontier and syncs the working set to the merged root. With an optional ref
  argument — `dolt_reconcile('refs/remotes/origin/main')` — it first folds that
  ref's frontier onto the current branch (via `RecordTip`), then reconciles: the
  "merge a fetched multi-head remote into my branch" flow. Returns
  `(hash, tips, conflicts, message)`.
- Tests: `libraries/doltcore/merge/multihead_reconcile_frontier_test.go`
  (`TestReconcileFrontier_DisjointCollapsesToOneHead` — a real forked branch
  collapses to one merged head with two parents, and a re-run is a no-op;
  `TestReconcileFrontier_ConflictRecordedStillCollapses` — a same-row conflict is
  recorded and the fork still collapses) and the reconcile cases in
  `integration-tests/bats/multihead-remotes.bats`. Verified end-to-end against a
  built `dolt` binary: two writers fork a folder remote, `dolt fetch` brings the
  fork, `call dolt_reconcile('refs/remotes/origin/main')` collapses `main` to one
  head whose rows are the union of both writers' edits and whose `dolt log` shows
  a two-parent merge commit; a conflicting fork collapses with `dolt_conflicts`
  populated (base/ours/theirs) for resolution.

**First-class `dolt reconcile` CLI verb.** Reconcile is a real subcommand, not
only a `call`. `dolt reconcile [<ref>]` is the frontier analogue of `dolt merge`:

- `go/cmd/dolt/commands/reconcile.go` — `ReconcileCmd`, registered in
  `doltcmd.go` right after `MergeCmd`. It wraps the `dolt_reconcile()` procedure
  (interpolating the optional ref as a bound parameter, `set
  @@dolt_force_transaction_commit = 1` so a conflicted reconcile sticks — the
  same pattern `dolt merge` uses) and renders the `(hash, tips, conflicts,
  message)` row: a no-op on a single head, "Reconciled N heads into a single
  head." otherwise, and it exits nonzero when the reconcile recorded conflicts
  (pointing at `dolt conflicts`), so scripts can tell a clean collapse from one
  that needs resolution.
- Verified end-to-end against a built `dolt`: two writers fork a folder remote,
  `dolt fetch`, then `dolt reconcile refs/remotes/origin/main` collapses `main`
  to one merged head (rows `1,10,20`, a two-parent merge commit in `dolt log`); a
  second `dolt reconcile` reports "nothing to reconcile"; a conflicting fork
  collapses, prints the CONFLICT line, and exits 1 with `dolt_conflicts` showing
  `t,1`. Bats: the `dolt reconcile` cases in `multihead-remotes.bats`.

**Frontier-aware GC — a live fork is a GC root.**
`go/libraries/doltcore/doltdb/doltdb.go` (`DoltDB.GC`) now adds every frontier
tip of every ref as a GC root, alongside the (collapsed) heads `Datasets`
enumerates:

- `datas.AllFrontierTips` (`store/datas/multihead_datasets.go`) →
  `DoltDB.MultiheadFrontierTips` returns each forked ref's whole frontier.
  `DoltDB.GC` inserts those tips into the same generation bucket as ordinary
  branch heads.
- The honest mechanics: a fork was *already* never collected — `ValueStore.GC`
  roots the raw manifest address map, which holds every tip as a content-
  addressed sub-key, so a divergent tip is reachable and retained regardless of
  the collapse `Datasets` does. The frontier roots exist for correct
  GENERATIONAL placement (a fork's non-canonical tips classified with the branch
  heads in the old generation rather than lingering as new-generation data that
  is re-walked every GC) and to make the "a live fork is a GC root" invariant
  explicit at the `DoltDB.GC` seam rather than an implicit consequence of a deep
  nbs detail.
- Tests: `libraries/doltcore/doltdb/multihead_gc_test.go`
  (`TestMultiheadGCKeepsFrontier` — a forked branch survives GC with both tips'
  unique table data intact while an unreferenced commit is collected) and the
  `gc keeps a fork's whole frontier` case in `multihead-remotes.bats`. Verified
  end-to-end: `dolt gc` on a still-forked tracking ref leaves both tips, and the
  fork still reconciles cleanly afterward.

**Known limits (the honest edges).** Push/fetch, reconcile, and GC are all
frontier-aware, so the full round trip — two `dolt` CLIs fork a folder remote,
`dolt reconcile` collapses the fork to one merged head, `dolt gc` keeps a live
fork intact — works from the CLI. What is still NOT wired: `dolt merge <branch>`
/ `dolt pull` themselves keep the stock single-root path (reconcile is its own
`dolt reconcile` verb / `dolt_reconcile()` procedure, deliberately not entangled
with the heavily-used merge path); `dolt_reconcile` folds >2 tips against the
canonical tip's ancestor (octopus-style approximation — a true two-writer fork is
exact); tags are single-headed on the wire. Truly *simultaneous* pushes to one
folder serialize at the remote's nbs root-map (both survive as tips, none
rejected); the fully lock-free deposit is the `store/nbs` `roots/` path
(`PublishHeadTo`), not yet wired under push. The toggle remains experimental.

### Build & test
```bash
cd go
go build ./store/nbs/ ./store/datas/ ./store/datas/multihead_conf/
go vet ./store/nbs/ ./store/datas/
go test ./store/nbs/  -run TestMultihead                     -count=1 -v  # Step 1 + e2e + concurrent: 7
go test ./store/nbs/  -run TestMultihead_ConcurrentWritersNoLock -race -count=10  # lock-free proof
go test ./store/datas/ -count=1                                           # Steps 2/2b/2c + e2e + full suite (invariant #4)
go test ./store/datas/ -run 'TestMultiheadDatasets|TestReconcile|TestMultiheadPrimary|TestMultihead_SharedFolder' -count=1 -v  # 15
# SQL/relational tier (needs libicu-dev for the cgo icu-regex dep):
go test ./libraries/doltcore/merge/ -run TestMultiheadReconcile -count=1 -v  # 3
# Multi-head fetch/push (needs libicu-dev):
go test ./libraries/doltcore/env/actions/ -run TestPushMultihead -count=1 -v  # divergent push forks; fetch brings the frontier
# Frontier-aware reconcile (needs libicu-dev):
go test ./libraries/doltcore/merge/ -run TestReconcileFrontier -count=1 -v  # fork collapses to one merged head; conflict still collapses
# Frontier-aware GC (needs libicu-dev):
go test ./libraries/doltcore/doltdb/ -run 'TestMultiheadGCKeepsFrontier|TestGarbageCollection' -count=1 -v  # a fork survives GC; stock GC unchanged
# First-class dolt reconcile subcommand builds into the CLI:
go build -o /tmp/dolt ./cmd/dolt && /tmp/dolt reconcile --help
# CLI story (needs the bats harness + a built dolt on PATH):
#   integration-tests/bats/multihead-remotes.bats  (push/fetch fork + dolt reconcile + gc round trip)
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

### Step 2b — reconcile with Dolt's real three-way merge — DONE (tree-level), gate green
Implemented in `multihead_merge.go` (see "What already exists"). A fork is
collapsed by `MergeTips`, which runs **`prolly.MergeMaps`** — Dolt's real
tree-level three-way merge, the same one the SQL row merge is built on — over
the tips' committed prolly maps against their `FindCommonAncestor` base. The
conformance harness now stores real prolly maps and resolves via `MergeTips`,
so the portable suite validates against Dolt's actual merge, not a model. This
retires the "model three-way" caveat for the row/tuple merge.

**Full relational fidelity — DONE at the RootValue level (via `MergeRoots`),
gate green.** The remaining fidelity beyond one keyed map — schema merge,
multiple tables, secondary indexes, FK/constraint validation, and Dolt's
conflict-recording tables — is exactly `libraries/doltcore/merge.MergeRoots`
(what `dolt merge` calls). `multihead_reconcile_test.go` (see "What already
exists") reconciles two multi-head tips through the real `MergeRoots` over
multiple tables, a schema change, and a recorded conflict. `store/datas` can't
import `doltcore` (layering), so this validation correctly lives in the
`doltcore/merge` package. **What still remains:** end-to-end wiring of a
multi-head *tip* (a datas commit whose value is a `RootValue`) → `MergeRoots` →
record the merged `RootValue` as the collapsing tip, driven through the running
doltdb/SQL session (a `dolt merge` that reads the frontier). The engine tier is
proven; the plumbing that hands multi-head tips to it is the next integration.

### Step 2c — make multi-head the primary commit path — DONE (mode-gated), gate green
Implemented in `multihead_primary.go` (see "What already exists"). A database
switched with `EnableMultihead(db)` has a multi-head *primary* path: the ordinary
`Commit` publishes a tip with no lineage gate and no `curr != datasetCurrentAddr`
CAS (a divergent commit adds a head; a sequential one fast-forwards), and
`GetDataset` surfaces the frontier — the sole tip, or `ErrMultipleHeads` under a
fork. `Commit` and `datasetFromMap` each branch on a single `if db.multihead`;
`tips` was refactored into a reusable `frontierOf(am, ref)` the primary path
shares.

**Why mode-gated, not a Dolt-wide flip.** The single-root CAS is assumed by
essentially every reader, refspec resolver, SQL path, and the GC; flipping it
unconditionally would regress the whole suite (invariant #4). Mode-gating makes
multi-head a genuine, first-class *primary* path — a normal `Commit` is multi-tip
when the mode is on — while a database opened normally stays byte-identical to
stock (`TestMultiheadPrimary_DefaultModeKeepsCAS` guards this). Flipping the
global default (teaching *every* reader to expect a frontier, plus the nbs half
in Step 1) remains the larger follow-on; `EnableMultihead` is the seam it hangs
on.

### Step 3 — cross-writer GC
Stock conjoin/GC deletes shared table files and assumes one root. Make GC a
mark-sweep from the tips of the `roots/` DAG, and keep conjoin from deleting
another writer's shared table files. Consider the "writer-local,
never-reused monotone id → coordination-free GC" borrow from Quadrable (see
the decision note's Quadrable section).

## Correctness gate: merge-conformance

The portable suite that pins fork/edit/merge semantics is
`experiments/merge-conformance/` (the `mergeconf` Python package) in
`Taytay/taytays_stuff` (currently on PR #197). **Steps 2 and 2b are wired to it
and pass:** the `multihead_conf` harness here plus the driver in
`taytays_stuff/experiments/multihead-dolt-nbs/conformance/` run the real Dolt
multi-tip datas layer — storing real `prolly.Map`s and reconciling forks with
`prolly.MergeMaps` (Dolt's real merge) — through all 15 pinned CASES and the
Hypothesis differential check (`fail`/`ours`/`theirs`/`record`).

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
worked example. The harness's resolve step uses Dolt's real tree-level merge
(`prolly.MergeMaps`). The full relational merge engine
(`libraries/doltcore/merge.MergeRoots` — schema/multi-table/constraints) is
exercised separately by `multihead_reconcile_test.go` in that package (it needs
the doltdb/SQL stack, which `store/datas` cannot import). What is left is
end-to-end plumbing (multi-head tip → `MergeRoots` → recorded merged tip through
a running doltdb session), not the engine itself.

## Environment notes

- Go module root: `go/`. Go 1.24.
- The fork's default `main` tracks `dolthub/dolt`; rebase periodically.
- Keep changes new-file-heavy where possible; Steps 1–2 necessarily edit
  existing files (`store.go`, `database_common.go`) — normal commits here, not
  patches.
