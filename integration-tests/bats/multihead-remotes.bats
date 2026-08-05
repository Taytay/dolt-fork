#!/usr/bin/env bats
# Multi-head fetch/push (DOLT_MULTIHEAD). Two writers share a file:// folder
# remote; each pushes divergent work off a common base. In multi-head mode the
# second push FORKS the remote (a frontier of tips) instead of being rejected as
# non-fast-forward, and `dolt fetch` brings every tip into the tracking ref.
# The stock (env unset) path still fast-forward-rejects — invariant #4.
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    cd $BATS_TMPDIR
    cd dolt-repo-$$
}

teardown() {
    assert_feature_version
    teardown_common
}

# Seed the current repo with a base commit and push it to a fresh file remote.
seed_and_push_base() {
    dolt sql -q "create table t (id int primary key, v int)"
    dolt sql -q "insert into t values (1, 100)"
    dolt add .
    dolt commit -m "base"
    mkdir remotedir
    dolt remote add origin file://remotedir
    dolt push origin main
}

@test "multihead-remotes: divergent push forks the remote instead of being rejected" {
    export DOLT_MULTIHEAD=1

    seed_and_push_base

    # A second writer clones the base.
    mkdir clones
    cd clones
    dolt clone file://../remotedir b
    cd ..

    # Writer A commits and pushes (remote advances to A2).
    dolt sql -q "insert into t values (10, 10)"
    dolt commit -am "A: change"
    run dolt push origin main
    [ "$status" -eq 0 ]

    # Writer B commits divergently off the base and pushes. In multi-head mode
    # this is ACCEPTED (a fork), not rejected as non-fast-forward.
    cd clones/b
    dolt sql -q "insert into t values (20, 20)"
    dolt commit -am "B: change"
    run dolt push origin main
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "non-fast-forward" ]] || false
    cd ../..

    # Writer A fetches: the remote-tracking ref now holds BOTH tips (the fork).
    run dolt fetch origin
    [ "$status" -eq 0 ]
    run dolt sql -q "select dolt_frontier('refs/remotes/origin/main') as f" -r csv
    [ "$status" -eq 0 ]
    # Two comma-separated 32-char tip hashes.
    [[ "${lines[1]}" =~ ^[0-9a-v]{32},[0-9a-v]{32}$ ]] || false
}

@test "multihead-remotes: sequential pushes fast-forward (one tip)" {
    export DOLT_MULTIHEAD=1

    seed_and_push_base

    dolt sql -q "insert into t values (2, 200)"
    dolt commit -am "second"
    run dolt push origin main
    [ "$status" -eq 0 ]

    dolt sql -q "insert into t values (3, 300)"
    dolt commit -am "third"
    run dolt push origin main
    [ "$status" -eq 0 ]

    # A sequential chain names its predecessor as parent, so the remote frontier
    # collapses back to a single tip.
    run dolt fetch origin
    [ "$status" -eq 0 ]
    run dolt sql -q "select dolt_frontier('refs/remotes/origin/main') as f" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ ^[0-9a-v]{32}$ ]] || false
}

@test "multihead-remotes: reconcile collapses a fetched fork into one merged head" {
    export DOLT_MULTIHEAD=1

    seed_and_push_base

    # A second writer clones, commits divergently, and pushes (forks the remote).
    mkdir clones
    cd clones
    dolt clone file://../remotedir b
    cd b
    dolt sql -q "insert into t values (20, 20)"
    dolt commit -am "B: add row 20"
    dolt push origin main
    cd ../..

    # Writer A commits and pushes its own side, then fetches the fork.
    dolt sql -q "insert into t values (10, 10)"
    dolt commit -am "A: add row 10"
    dolt push origin main
    dolt fetch origin

    # Reconcile the fetched multi-head remote into main.
    run dolt sql -q "call dolt_reconcile('refs/remotes/origin/main')"
    [ "$status" -eq 0 ]

    # main is now a single head again.
    run dolt sql -q "select dolt_frontier() as f" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ ^[0-9a-v]{32}$ ]] || false

    # The merged head carries BOTH writers' rows.
    run dolt sql -q "select id from t order by id" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "10" ]] || false
    [[ "$output" =~ "20" ]] || false

    # It is a real merge commit (two parents).
    run dolt log -n 1 --parents
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge:" ]] || false
}

@test "multihead-remotes: reconcile is a no-op on a single-head branch" {
    export DOLT_MULTIHEAD=1
    dolt sql -q "create table t (id int primary key, v int)"
    dolt sql -q "insert into t values (1, 1)"
    dolt add .
    dolt commit -m "base"

    run dolt sql -q "call dolt_reconcile()" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to reconcile" ]] || false
}

@test "multihead-remotes: stock mode still rejects a non-fast-forward push" {
    # No DOLT_MULTIHEAD: the single-root fast-forward discipline is unchanged.
    seed_and_push_base

    mkdir clones
    cd clones
    dolt clone file://../remotedir b
    cd ..

    dolt sql -q "insert into t values (10, 10)"
    dolt commit -am "A: change"
    dolt push origin main

    cd clones/b
    dolt sql -q "insert into t values (20, 20)"
    dolt commit -am "B: change"
    run dolt push origin main
    [ "$status" -ne 0 ]
    [[ "$output" =~ "non-fast-forward" ]] || false
}
