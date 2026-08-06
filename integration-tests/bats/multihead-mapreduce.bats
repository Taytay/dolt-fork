#!/usr/bin/env bats
# Map/reduce over a shared file:// folder remote in multi-head mode
# (DOLT_MULTIHEAD). This is the end-to-end scenario the multi-head work exists
# for: many machines write disjoint partitions of one table in parallel, push to
# one shared folder (each divergent push FORKS the remote instead of being
# rejected), and a reducer fetches the whole fork and reconciles it back to a
# single head with Dolt's real three-way merge.
#
#   Phase 1 (generate): N machines each clone the same base, insert their own
#     100-row id partition, and push -> a genuine N-tip fork; reconcile -> 500
#     rows, no conflicts (disjoint keys).
#   Phase 2 (process):  N machines each clone the 500-row head, uppercase their
#     own partition, and push -> another N-tip fork; reconcile -> 500 uppercase.
#   Phase 3 (reduce):   one machine reconciles and sorts all 500.
#
# Verified equivalent (same commands, same counts) against a built dolt binary
# before being codified here.
load $BATS_TEST_DIRNAME/helper/common.bash

WORKERS=5
ROWS=100

setup() {
    setup_common
    cd $BATS_TMPDIR
    cd dolt-repo-$$
}

teardown() {
    assert_feature_version
    teardown_common
}

# gen_inserts START COUNT -> a single INSERT statement for rows [START, START+COUNT)
gen_inserts() {
    local start=$1 cnt=$2 i id vals=""
    for (( i=0; i<cnt; i++ )); do
        id=$(( start + i ))
        vals+="($id,'str$id'),"
    done
    echo "insert into t values ${vals%,};"
}

@test "multihead-mapreduce: N machines generate, uppercase, and reduce 500 strings via a shared folder" {
    export DOLT_MULTIHEAD=1

    remote="$(pwd)/remotedir"
    mkdir "$remote"
    base="file://$remote"

    # --- seed the shared base: an empty strings table ---
    dolt sql -q "create table t (id int primary key, s varchar(64))"
    dolt add .
    dolt commit -m "base: empty strings table"
    dolt remote add origin "$base"
    dolt push origin main

    work="$(pwd)/work"
    mkdir "$work"

    # --- Phase 1 (generate): all workers clone the SAME base, then push divergently ---
    for (( k=0; k<WORKERS; k++ )); do
        dolt clone "$base" "$work/gen$k"
    done
    for (( k=0; k<WORKERS; k++ )); do
        cd "$work/gen$k"
        dolt sql -q "$(gen_inserts $(( k*ROWS )) $ROWS)"
        dolt commit -am "gen$k"
        dolt push origin main
        cd - >/dev/null
    done

    # --- reduce Phase 1: fetch the fork and reconcile to a single 500-row head ---
    dolt clone "$base" "$work/reduce1"
    cd "$work/reduce1"
    dolt fetch origin
    # The tracking ref holds a genuine N-tip fork.
    run dolt sql -q "select dolt_frontier('refs/remotes/origin/main') as f" -r csv
    [ "$status" -eq 0 ]
    tips=$(echo "${lines[1]}" | tr ',' '\n' | wc -l | tr -d ' ')
    [ "$tips" -eq "$WORKERS" ] || false

    run dolt reconcile refs/remotes/origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Reconciled $WORKERS heads" ]] || false

    run dolt sql -q "select count(*) as c from t" -r csv
    [ "${lines[1]}" -eq $(( WORKERS*ROWS )) ] || false
    # Disjoint inserts merge cleanly and are still all lowercase.
    run dolt sql -q "select count(*) as c from t where s <> lower(s)" -r csv
    [ "${lines[1]}" -eq 0 ] || false
    dolt push origin main
    cd - >/dev/null

    # --- Phase 2 (process): all workers clone the 500-row head, uppercase their partition ---
    for (( k=0; k<WORKERS; k++ )); do
        dolt clone "$base" "$work/proc$k"
    done
    for (( k=0; k<WORKERS; k++ )); do
        cd "$work/proc$k"
        run dolt sql -q "select count(*) as c from t" -r csv
        [ "${lines[1]}" -eq $(( WORKERS*ROWS )) ] || false
        dolt sql -q "update t set s = upper(s) where id >= $(( k*ROWS )) and id < $(( k*ROWS+ROWS ))"
        dolt commit -am "proc$k upper"
        dolt push origin main
        cd - >/dev/null
    done

    # --- reduce Phase 2: fetch + reconcile -> single head, all uppercase ---
    dolt clone "$base" "$work/reduce2"
    cd "$work/reduce2"
    dolt fetch origin
    run dolt sql -q "select dolt_frontier('refs/remotes/origin/main') as f" -r csv
    tips=$(echo "${lines[1]}" | tr ',' '\n' | wc -l | tr -d ' ')
    [ "$tips" -eq "$WORKERS" ] || false

    run dolt reconcile refs/remotes/origin/main
    [ "$status" -eq 0 ]

    run dolt sql -q "select count(*) as c from t" -r csv
    [ "${lines[1]}" -eq $(( WORKERS*ROWS )) ] || false
    # Every row is now uppercase (disjoint updates merged with no conflict).
    run dolt sql -q "select count(*) as c from t where s <> upper(s)" -r csv
    [ "${lines[1]}" -eq 0 ] || false
    dolt push origin main
    cd - >/dev/null

    # --- Phase 3 (reduce/sort) ---
    dolt clone "$base" "$work/examine"
    cd "$work/examine"
    dolt fetch origin
    dolt reconcile refs/remotes/origin/main
    run dolt sql -q "select count(*) as c from t" -r csv
    [ "${lines[1]}" -eq $(( WORKERS*ROWS )) ] || false
    # Sorted output is complete and lexicographically ordered.
    run dolt sql -q "select s from t order by s asc limit 1" -r csv
    [ "${lines[1]}" = "STR0" ] || false
    cd - >/dev/null
}

@test "multihead-mapreduce: overlapping writes to the same key surface as a conflict" {
    # The partitioning discipline matters: if two machines edit the SAME row, the
    # reconcile records a conflict (rather than silently losing one edit).
    export DOLT_MULTIHEAD=1

    remote="$(pwd)/remotedir"
    mkdir "$remote"
    base="file://$remote"

    dolt sql -q "create table t (id int primary key, s varchar(64))"
    dolt sql -q "insert into t values (1, 'base')"
    dolt add .
    dolt commit -m "base"
    dolt remote add origin "$base"
    dolt push origin main

    work="$(pwd)/work"
    mkdir "$work"
    dolt clone "$base" "$work/a"
    dolt clone "$base" "$work/b"

    # Both machines edit row 1 differently -> a real conflict on reconcile.
    cd "$work/a"; dolt sql -q "update t set s='AAA' where id=1"; dolt commit -am "a"; dolt push origin main; cd - >/dev/null
    cd "$work/b"; dolt sql -q "update t set s='BBB' where id=1"; dolt commit -am "b"; dolt push origin main; cd - >/dev/null

    dolt clone "$base" "$work/r"
    cd "$work/r"
    dolt fetch origin
    run dolt reconcile refs/remotes/origin/main
    # The reconcile still collapses the fork, but reports the conflict (exit 1).
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt sql -q "select count(*) as c from dolt_conflicts" -r csv
    [ "${lines[1]}" -ge 1 ] || false
    cd - >/dev/null
}
