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

// Choosing a single working head from a multi-head frontier WITHOUT merging.
//
// The authoritative state of a multi-head ref is the set returned by Tips. But a
// reader often wants to proceed on one head and reconcile later (a working copy
// has a checked-out head; `git status` tells you there is divergence to merge).
// This file provides that provisional pick without ever dropping a tip.
//
// The naive pick — "lowest-hash tip" — is coordination-free but NON-MONOTONIC: a
// later writer whose tip sorts lower silently changes "the" head under a reader
// that never acted (the flip-flop hazard). ResolveHead fixes that with a STICKY
// rule: a caller passes the head it is currently on, and keeps it as long as it
// is still a tip; only when its head has been superseded (merged away) does it
// fall back to the canonical lowest-hash tip. So a head moves only when the
// caller itself commits or reconciles — never merely because another tip
// appeared — while a fresh reader with no prior head still gets a deterministic,
// coordination-free answer.
//
// Neither pick is a merge and neither is authoritative: it never deletes the
// other tips, and it never supersedes by wall-clock time (invariant #2 —
// supersession is causal, never temporal). The forked bool is the "you have
// unmerged work" signal; the fix is MergeTips, not a smarter guess.
package datas

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
)

// CanonicalTip returns the deterministic, coordination-free pick from a frontier
// — the lowest tip in the sorted order Tips already returns — or the empty hash
// for an empty frontier. Every reader of the same frontier agrees on it with no
// shared state. It is provisional and may change as tips arrive; prefer
// ResolveHead when you hold a current head and want stability.
func CanonicalTip(tips []hash.Hash) hash.Hash {
	if len(tips) == 0 {
		return hash.Hash{}
	}
	return tips[0] // Tips is sorted by content hash, so tips[0] is canonical
}

// ResolveHead picks a single working head for ref without merging. If |preferred|
// (the head the caller is currently on; pass the empty hash if none) is still on
// the frontier, it is kept — a sticky choice that does not flip-flop as other
// tips come and go. Otherwise the canonical tip is returned. |forked| reports
// whether the frontier has more than one tip, i.e. there is unmerged work to
// reconcile with MergeTips. A ref with no tips returns the empty hash, false.
func ResolveHead(ctx context.Context, db Database, ref string, preferred hash.Hash) (head hash.Hash, forked bool, err error) {
	tips, err := Tips(ctx, db, ref)
	if err != nil {
		return hash.Hash{}, false, err
	}
	if len(tips) == 0 {
		return hash.Hash{}, false, nil
	}
	forked = len(tips) > 1
	if !preferred.IsEmpty() {
		for _, t := range tips {
			if t == preferred {
				return preferred, forked, nil // sticky: keep the caller's head
			}
		}
	}
	return CanonicalTip(tips), forked, nil
}
