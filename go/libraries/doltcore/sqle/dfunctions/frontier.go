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

package dfunctions

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltFrontierFuncName = "dolt_frontier"

// DoltFrontierFunc returns the multi-head frontier of the current branch as a
// comma-separated list of commit tip hashes: empty for a normal (single-head)
// branch, one hash when writers agree, several when there is an unresolved fork
// to reconcile. It is meaningful only when the database was opened in multi-head
// mode (DOLT_MULTIHEAD); on a stock single-root database it returns the empty
// string. It is the SQL surface over (*doltdb.DoltDB).MultiheadTips.
type DoltFrontierFunc struct{}

// NewDoltFrontierFunc creates a new DoltFrontierFunc expression.
func NewDoltFrontierFunc(ctx *sql.Context) sql.Expression {
	return &DoltFrontierFunc{}
}

// Eval implements the Expression interface.
func (f *DoltFrontierFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()
	if dbName == "" {
		return nil, nil
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	ddb, ok := dSess.GetDoltDB(ctx, dbName)
	if !ok {
		return nil, nil
	}

	branchRef, err := dSess.CWBHeadRef(ctx, dbName)
	if err == doltdb.ErrOperationNotSupportedInDetachedHead {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	tips, err := ddb.MultiheadTips(ctx, branchRef.String())
	if err != nil {
		return nil, err
	}

	parts := make([]string, len(tips))
	for i, t := range tips {
		parts[i] = t.String()
	}
	return strings.Join(parts, ","), nil
}

// String implements the Stringer interface.
func (f *DoltFrontierFunc) String() string { return "DOLT_FRONTIER()" }

// IsNullable implements the Expression interface.
func (f *DoltFrontierFunc) IsNullable(ctx *sql.Context) bool { return false }

// Resolved implements the Expression interface.
func (*DoltFrontierFunc) Resolved() bool { return true }

// Type implements the Expression interface.
func (f *DoltFrontierFunc) Type(ctx *sql.Context) sql.Type { return types.Text }

// Children implements the Expression interface.
func (*DoltFrontierFunc) Children() []sql.Expression { return nil }

// WithChildren implements the Expression interface.
func (f *DoltFrontierFunc) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return NewDoltFrontierFunc(ctx), nil
}
