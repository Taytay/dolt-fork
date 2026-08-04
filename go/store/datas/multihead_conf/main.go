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
// JSON protocol on stdin/stdout, backed by the REAL Dolt multi-tip datas layer
// (datas.AppendCommit / datas.Tips). A Python mergeconf.MergeDriver drives it
// as a subprocess so the portable suite (pinned CASES + Hypothesis
// check_history) runs against actual Dolt commits and the multi-head frontier.
//
// main and branch are two tips of ONE ref: apply() records a divergent commit
// as a tip (no CAS, no ErrMergeNeeded); merge() reads the two tips' states and
// resolves them with a three-way merge that mirrors the conformance model
// (experiments/merge-conformance/mergeconf/model.py). Storage, fork, tips, and
// history all go through the real datas layer; the three-way policy logic is
// the model (wiring Dolt's SQL row-merge engine is a later integration).
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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const ref = "refs/heads/main"

// row is a table row value part: (v, n) with n nullable. A missing key in a
// state means the row is absent.
type row struct {
	V string
	N *int
}

func (r row) equal(o row) bool {
	if r.V != o.V {
		return false
	}
	if (r.N == nil) != (o.N == nil) {
		return false
	}
	return r.N == nil || *r.N == *o.N
}

type state map[int]row

// wire form: a row is [v, n]; state is {"id": [v, n]}.
func (s state) toWire() map[string][]interface{} {
	m := make(map[string][]interface{}, len(s))
	for k, r := range s {
		var n interface{}
		if r.N != nil {
			n = *r.N
		}
		m[fmt.Sprintf("%d", k)] = []interface{}{r.V, n}
	}
	return m
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

// stateToJSON canonicalizes a state to deterministic bytes (id-sorted triples)
// so identical states commit to identical values.
func stateToJSON(s state) []byte {
	ids := make([]int, 0, len(s))
	for k := range s {
		ids = append(ids, k)
	}
	sort.Ints(ids)
	triples := make([][]interface{}, 0, len(ids))
	for _, id := range ids {
		r := s[id]
		var n interface{}
		if r.N != nil {
			n = *r.N
		}
		triples = append(triples, []interface{}{id, r.V, n})
	}
	b, _ := json.Marshal(triples)
	return b
}

func stateFromJSON(b []byte) (state, error) {
	var triples [][]interface{}
	if err := json.Unmarshal(b, &triples); err != nil {
		return nil, err
	}
	s := make(state, len(triples))
	for _, tr := range triples {
		if len(tr) != 3 {
			return nil, fmt.Errorf("triple must have 3 elements")
		}
		id := int(tr[0].(float64))
		r, err := rowFromWire(tr[1:])
		if err != nil {
			return nil, err
		}
		s[id] = r
	}
	return s, nil
}

// threeWay mirrors experiments/merge-conformance/mergeconf/model.py exactly.
// Per key, with base b, ours o, theirs t (absent = missing):
//   - t == b            -> keep o
//   - o == b || o == t  -> take t
//   - otherwise         -> conflict, resolved by policy
//
// Returns the merged state and the sorted list of conflicting keys.
func threeWay(base, ours, theirs state, policy string) (state, []int, map[int]*row) {
	keys := map[int]struct{}{}
	for k := range base {
		keys[k] = struct{}{}
	}
	for k := range ours {
		keys[k] = struct{}{}
	}
	for k := range theirs {
		keys[k] = struct{}{}
	}

	present := func(s state, k int) (row, bool) { r, ok := s[k]; return r, ok }
	eq := func(s1 state, k1 int, s2 state, k2 int) bool {
		r1, ok1 := present(s1, k1)
		r2, ok2 := present(s2, k2)
		if ok1 != ok2 {
			return false
		}
		return !ok1 || r1.equal(r2)
	}

	result := state{}
	var conflicts []int
	resolutions := map[int]*row{}
	for k := range keys {
		o, oOK := present(ours, k)
		t, tOK := present(theirs, k)

		var merged row
		var mergedOK bool
		switch {
		case eq(theirs, k, base, k): // theirs unchanged -> keep ours
			merged, mergedOK = o, oOK
		case eq(ours, k, base, k) || eq(ours, k, theirs, k): // ours unchanged / convergent -> take theirs
			merged, mergedOK = t, tOK
		default: // both changed differently -> conflict
			conflicts = append(conflicts, k)
			switch policy {
			case "theirs":
				merged, mergedOK = t, tOK
			default: // ours, record, fail all keep ours
				merged, mergedOK = o, oOK
			}
			if mergedOK {
				rr := merged
				resolutions[k] = &rr
			} else {
				resolutions[k] = nil
			}
		}
		if mergedOK {
			result[k] = merged
		}
	}
	sort.Ints(conflicts)
	return result, conflicts, resolutions
}

// harness holds the live store and the three tracked heads.
type harness struct {
	ctx    context.Context
	db     datas.Database
	fork   hash.Hash
	main   hash.Hash
	branch hash.Hash
}

func newHarness() *harness {
	storage := &chunks.TestStorage{}
	return &harness{
		ctx: context.Background(),
		db:  datas.NewDatabase(storage.NewViewWithDefaultFormat()),
	}
}

func (h *harness) commit(s state, parents ...hash.Hash) (hash.Hash, error) {
	// Pinned dates keep commits deterministic; distinct parents/values still
	// yield distinct commits, which is what a real fork needs.
	epoch := datas.CommitDateAt(time.UnixMilli(0))
	meta := &datas.CommitMeta{Author: datas.CommitIdent{Date: epoch}, Committer: datas.CommitIdent{Date: epoch}}
	return datas.AppendCommit(h.ctx, h.db, ref, types.String(stateToJSON(s)),
		datas.CommitOptions{Parents: parents, Meta: meta})
}

func (h *harness) stateAt(addr hash.Hash) (state, error) {
	if addr.IsEmpty() {
		return state{}, nil
	}
	v, err := datas.TipValue(h.ctx, h.db, addr)
	if err != nil {
		return nil, err
	}
	return stateFromJSON([]byte(v.(types.String)))
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
		base, err := h.stateAt(h.fork)
		if err != nil {
			return nil, err
		}
		ours, err := h.stateAt(h.main)
		if err != nil {
			return nil, err
		}
		theirs, err := h.stateAt(h.branch)
		if err != nil {
			return nil, err
		}
		merged, conflicts, resolutions := threeWay(base, ours, theirs, req.Policy)

		// policy "fail" with any conflict refuses the whole merge and touches
		// nothing (all-or-nothing).
		if req.Policy == "fail" && len(conflicts) > 0 {
			return map[string]interface{}{
				"ok":              true,
				"applied":         false,
				"conflicts":       conflicts,
				"conflicts_known": true,
			}, nil
		}

		// Append the merge as a new tip naming both heads; this collapses the
		// frontier (verified separately by Tips in the Go unit tests).
		mergeAddr, err := h.commit(merged, h.main, h.branch)
		if err != nil {
			return nil, err
		}
		h.main = mergeAddr

		res := map[string]interface{}{}
		for k, r := range resolutions {
			if r == nil {
				res[fmt.Sprintf("%d", k)] = nil
			} else {
				var n interface{}
				if r.N != nil {
					n = *r.N
				}
				res[fmt.Sprintf("%d", k)] = []interface{}{r.V, n}
			}
		}
		return map[string]interface{}{
			"ok":              true,
			"applied":         true,
			"conflicts":       conflicts,
			"conflicts_known": true,
			"resolutions":     res,
		}, nil

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
	h := newHarness()
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
