// Copyright 2024 Dolthub, Inc.
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

// Command multihead_conf is a thin harness that exposes the merge-conformance
// world (one keyed table, one main lineage, one branch) over a line-oriented
// JSON protocol on stdin/stdout, backed by the REAL Dolt multi-tip datas layer.
// A Python mergeconf.MergeDriver drives it as a subprocess so the portable
// suite (pinned CASES + Hypothesis check_history) runs against actual Dolt
// commits, the multi-head frontier, and — as of Step 2b — Dolt's real
// tree-level three-way merge.
//
// main and branch are two tips of ONE ref. The table (pk id:int64; value
// v:string, n:int64 nullable) is stored as a real prolly.Map committed with
// datas.AppendMapCommit; apply() records a divergent commit as a tip (no CAS,
// no ErrMergeNeeded); merge() reconciles the two tips with datas.MergeTips,
// which runs prolly.MergeMaps (the same tree merge Dolt's SQL row merge is
// built on) over the tips' maps against their common-ancestor commit. The
// policy is expressed as the merge's CollisionFn.
//
// Protocol: one JSON request object per line; one JSON response per line.
//
//	{"cmd":"setup","seed":{"1":["a",null]}}      -> {"ok":true}
//	{"cmd":"fork"}                               -> {"ok":true}
//	{"cmd":"apply","side":"main","ops":[...]}    -> {"ok":true}
//	{"cmd":"merge","policy":"fail"}              -> {"ok":true,"applied":...,
//	                                                 "conflicts":[k],
//	                                                 "resolutions":{"k":row|null}}
//	{"cmd":"state","side":"main"}                -> {"ok":true,"state":{...}}
//	{"cmd":"snapshot","side":"main"}             -> {"ok":true,"token":"<addr>"}
//	{"cmd":"state_at","side":"main","token":".."}-> {"ok":true,"state":{...}}
//	{"cmd":"close"}                              -> {"ok":true} then exit
//
// A row is [v string, n int|null]; state maps stringified id -> row.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const ref = "refs/heads/main"

// The conformance table schema.
var (
	keyDesc = val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc})
	valDesc = val.NewTupleDescriptor(
		val.Type{Enc: val.StringEnc},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)
)

// row is a table row value part: (v, n) with n nullable. A missing key in a
// state means the row is absent.
type row struct {
	V string
	N *int
}

type state map[int]row

func (s state) toWire() map[string][]interface{} {
	m := make(map[string][]interface{}, len(s))
	for k, r := range s {
		m[fmt.Sprintf("%d", k)] = rowToWire(r)
	}
	return m
}

func rowToWire(r row) []interface{} {
	var n interface{}
	if r.N != nil {
		n = *r.N
	}
	return []interface{}{r.V, n}
}

func rowFromWire(raw []interface{}) (row, error) {
	if len(raw) != 2 {
		return row{}, fmt.Errorf("row must have 2 elements, got %d", len(raw))
	}
	v, ok := raw[0].(string)
	if !ok {
		return row{}, fmt.Errorf("row[0] must be a string, got %T", raw[0])
	}
	r := row{V: v}
	if raw[1] != nil {
		f, ok := raw[1].(float64) // JSON numbers decode to float64
		if !ok {
			return row{}, fmt.Errorf("row[1] must be a number or null, got %T", raw[1])
		}
		n := int(f)
		r.N = &n
	}
	return r, nil
}

func stateFromWire(m map[string][]interface{}) (state, error) {
	s := make(state, len(m))
	for k, raw := range m {
		var id int
		if _, err := fmt.Sscanf(k, "%d", &id); err != nil {
			return nil, fmt.Errorf("bad key %q: %w", k, err)
		}
		r, err := rowFromWire(raw)
		if err != nil {
			return nil, err
		}
		s[id] = r
	}
	return s, nil
}

// harness holds the live store and the three tracked heads.
type harness struct {
	ctx    context.Context
	db     datas.Database
	ns     tree.NodeStore
	fork   hash.Hash
	main   hash.Hash
	branch hash.Hash
}

func newHarness() (*harness, error) {
	storage := &chunks.TestStorage{}
	db := datas.NewDatabase(storage.NewViewWithDefaultFormat())
	ns, err := datas.NodeStore(db)
	if err != nil {
		return nil, err
	}
	return &harness{ctx: context.Background(), db: db, ns: ns}, nil
}

// mapFromState builds a real prolly.Map (id-sorted) for a table state.
func (h *harness) mapFromState(s state) (prolly.Map, error) {
	ids := make([]int, 0, len(s))
	for k := range s {
		ids = append(ids, k)
	}
	sort.Ints(ids)

	pool := h.ns.Pool()
	var tups []val.Tuple
	for _, id := range ids {
		kb := val.NewTupleBuilder(keyDesc, h.ns)
		kb.PutInt64(0, int64(id))
		k, err := kb.Build(h.ctx, pool)
		if err != nil {
			return prolly.Map{}, err
		}
		vb := val.NewTupleBuilder(valDesc, h.ns)
		if err := vb.PutString(0, s[id].V); err != nil {
			return prolly.Map{}, err
		}
		if s[id].N != nil {
			vb.PutInt64(1, int64(*s[id].N))
		}
		v, err := vb.Build(h.ctx, pool)
		if err != nil {
			return prolly.Map{}, err
		}
		tups = append(tups, k, v)
	}
	return prolly.NewMapFromTuples(h.ctx, h.ns, keyDesc, valDesc, tups...)
}

func (h *harness) mapToState(m prolly.Map) (state, error) {
	it, err := m.IterAll(h.ctx)
	if err != nil {
		return nil, err
	}
	out := state{}
	for {
		k, v, err := it.Next(h.ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if k == nil {
			break
		}
		id, _ := keyDesc.GetInt64(0, k)
		vs, _ := valDesc.GetString(0, v)
		var np *int
		if n64, ok := valDesc.GetInt64(1, v); ok {
			n := int(n64)
			np = &n
		}
		out[int(id)] = row{V: vs, N: np}
	}
	return out, nil
}

func (h *harness) commit(s state, parents ...hash.Hash) (hash.Hash, error) {
	m, err := h.mapFromState(s)
	if err != nil {
		return hash.Hash{}, err
	}
	epoch := datas.CommitDateAt(time.UnixMilli(0))
	meta := &datas.CommitMeta{Author: datas.CommitIdent{Date: epoch}, Committer: datas.CommitIdent{Date: epoch}}
	return datas.AppendMapCommit(h.ctx, h.db, ref, m, datas.CommitOptions{Parents: parents, Meta: meta})
}

func (h *harness) stateAt(addr hash.Hash) (state, error) {
	if addr.IsEmpty() {
		return state{}, nil
	}
	m, err := datas.TipMap(h.ctx, h.db, addr, keyDesc, valDesc)
	if err != nil {
		return nil, err
	}
	return h.mapToState(m)
}

func (h *harness) head(side string) (hash.Hash, error) {
	switch side {
	case "main":
		return h.main, nil
	case "branch":
		return h.branch, nil
	default:
		return hash.Hash{}, fmt.Errorf("unknown side %q", side)
	}
}

type request struct {
	Cmd    string                   `json:"cmd"`
	Seed   map[string][]interface{} `json:"seed"`
	Side   string                   `json:"side"`
	Ops    [][]interface{}          `json:"ops"`
	Policy string                   `json:"policy"`
	Token  string                   `json:"token"`
}

func (h *harness) handle(req request) (map[string]interface{}, error) {
	switch req.Cmd {
	case "setup":
		seed, err := stateFromWire(req.Seed)
		if err != nil {
			return nil, err
		}
		addr, err := h.commit(seed)
		if err != nil {
			return nil, err
		}
		h.main, h.branch, h.fork = addr, addr, addr
		return map[string]interface{}{"ok": true}, nil

	case "fork":
		h.branch = h.main
		h.fork = h.main
		return map[string]interface{}{"ok": true}, nil

	case "apply":
		headAddr, err := h.head(req.Side)
		if err != nil {
			return nil, err
		}
		cur, err := h.stateAt(headAddr)
		if err != nil {
			return nil, err
		}
		next, err := applyOps(cur, req.Ops)
		if err != nil {
			return nil, err
		}
		newAddr, err := h.commit(next, headAddr)
		if err != nil {
			return nil, err
		}
		if req.Side == "main" {
			h.main = newAddr
		} else {
			h.branch = newAddr
		}
		return map[string]interface{}{"ok": true}, nil

	case "merge":
		return h.merge(req.Policy)

	case "state":
		headAddr, err := h.head(req.Side)
		if err != nil {
			return nil, err
		}
		s, err := h.stateAt(headAddr)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"ok": true, "state": s.toWire()}, nil

	case "snapshot":
		headAddr, err := h.head(req.Side)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"ok": true, "token": headAddr.String()}, nil

	case "state_at":
		addr, ok := hash.MaybeParse(req.Token)
		if !ok {
			return nil, fmt.Errorf("bad token %q", req.Token)
		}
		s, err := h.stateAt(addr)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"ok": true, "state": s.toWire()}, nil

	default:
		return nil, fmt.Errorf("unknown cmd %q", req.Cmd)
	}
}

func (h *harness) merge(policy string) (map[string]interface{}, error) {
	// Identical heads (both sides no-op, or convergent to the same commit):
	// nothing to reconcile, main already holds the answer.
	if h.main == h.branch {
		return map[string]interface{}{
			"ok": true, "applied": true, "conflicts_known": true,
			"conflicts": []int{}, "resolutions": map[string]interface{}{},
		}, nil
	}

	var conflicts []int
	collide := func(l, r tree.Diff) (tree.Diff, bool) {
		if l.Type == r.Type && bytes.Equal(l.To, r.To) {
			return l, true // convergent edit: not a conflict
		}
		id, _ := keyDesc.GetInt64(0, val.Tuple(l.Key))
		conflicts = append(conflicts, int(id))
		if policy == "theirs" {
			return r, true
		}
		return l, true // ours / record / fail keep ours
	}

	merged, err := datas.MergeTips(h.ctx, h.db, h.main, h.branch, keyDesc, valDesc, collide)
	if err != nil {
		return nil, err
	}
	sort.Ints(conflicts)

	// policy "fail" refuses the whole merge on any conflict (all-or-nothing):
	// leave main untouched, do not commit.
	if policy == "fail" && len(conflicts) > 0 {
		return map[string]interface{}{
			"ok": true, "applied": false, "conflicts_known": true, "conflicts": conflicts,
		}, nil
	}

	mergeAddr, err := h.commit2(merged, h.main, h.branch)
	if err != nil {
		return nil, err
	}
	h.main = mergeAddr

	mergedState, err := h.mapToState(merged)
	if err != nil {
		return nil, err
	}
	res := map[string]interface{}{}
	for _, id := range conflicts {
		if r, ok := mergedState[id]; ok {
			res[fmt.Sprintf("%d", id)] = rowToWire(r)
		} else {
			res[fmt.Sprintf("%d", id)] = nil
		}
	}
	return map[string]interface{}{
		"ok": true, "applied": true, "conflicts_known": true,
		"conflicts": conflicts, "resolutions": res,
	}, nil
}

// commit2 commits an already-merged prolly.Map as the collapsing tip.
func (h *harness) commit2(m prolly.Map, parents ...hash.Hash) (hash.Hash, error) {
	epoch := datas.CommitDateAt(time.UnixMilli(0))
	meta := &datas.CommitMeta{Author: datas.CommitIdent{Date: epoch}, Committer: datas.CommitIdent{Date: epoch}}
	return datas.AppendMapCommit(h.ctx, h.db, ref, m, datas.CommitOptions{Parents: parents, Meta: meta})
}

func applyOps(s state, ops [][]interface{}) (state, error) {
	next := make(state, len(s))
	for k, v := range s {
		next[k] = v
	}
	for _, op := range ops {
		if len(op) == 0 {
			return nil, fmt.Errorf("empty op")
		}
		kind, ok := op[0].(string)
		if !ok {
			return nil, fmt.Errorf("op[0] must be a string")
		}
		switch kind {
		case "insert", "update":
			if len(op) != 4 {
				return nil, fmt.Errorf("%s needs id,v,n", kind)
			}
			id := int(op[1].(float64))
			r, err := rowFromWire(op[2:])
			if err != nil {
				return nil, err
			}
			next[id] = r
		case "delete":
			if len(op) != 2 {
				return nil, fmt.Errorf("delete needs id")
			}
			delete(next, int(op[1].(float64)))
		default:
			return nil, fmt.Errorf("unknown op %q", kind)
		}
	}
	return next, nil
}

func main() {
	h, err := newHarness()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	emit := func(resp map[string]interface{}) {
		b, _ := json.Marshal(resp)
		out.Write(b)
		out.WriteByte('\n')
		out.Flush()
	}

	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			emit(map[string]interface{}{"ok": false, "error": err.Error()})
			continue
		}
		if req.Cmd == "close" {
			emit(map[string]interface{}{"ok": true})
			return
		}
		resp, err := h.handle(req)
		if err != nil {
			emit(map[string]interface{}{"ok": false, "error": err.Error()})
			continue
		}
		emit(resp)
	}
}
