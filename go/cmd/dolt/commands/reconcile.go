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

package commands

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var reconcileDocs = cli.CommandDocumentationContent{
	ShortDesc: "Collapse a multi-head frontier into a single head.",
	LongDesc: `Reconciles the current branch's multi-head frontier — the set of divergent tips produced when two writers commit off a common base and share a folder remote (see multi-head mode, {{.EmphasisLeft}}DOLT_MULTIHEAD{{.EmphasisRight}}) — back into one head, using the same three-way merge engine as {{.EmphasisLeft}}dolt merge{{.EmphasisRight}}.

Where {{.EmphasisLeft}}dolt merge {{.LessThan}}branch{{.GreaterThan}}{{.EmphasisRight}} joins another branch into the current one, {{.EmphasisLeft}}dolt reconcile{{.EmphasisRight}} folds the current branch's own divergent tips together and records a single merge commit that names every tip as a parent, so the branch has one head again.

With a ref argument — {{.EmphasisLeft}}dolt reconcile refs/remotes/origin/main{{.EmphasisRight}} — it first folds that ref's frontier onto the current branch and then reconciles. This is the "merge a fetched multi-head remote into my branch" flow: after {{.EmphasisLeft}}dolt fetch{{.EmphasisRight}} brings a forked remote's tips into the tracking ref, this merges them in and collapses the fork to one head.

If a reconcile results in conflicts they are recorded exactly as a conflicted {{.EmphasisLeft}}dolt merge{{.EmphasisRight}} would leave them; use {{.EmphasisLeft}}dolt conflicts{{.EmphasisRight}} to investigate and resolve them.

Reconcile is meaningful only in multi-head mode; on a single-head branch it is a no-op.`,
	Synopsis: []string{
		"[{{.LessThan}}ref{{.GreaterThan}}]",
	},
}

type ReconcileCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command.
func (cmd ReconcileCmd) Name() string {
	return "reconcile"
}

// Description returns a description of the command.
func (cmd ReconcileCmd) Description() string {
	return reconcileDocs.ShortDesc
}

func (cmd ReconcileCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(reconcileDocs, ap)
}

func (cmd ReconcileCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"ref", "An optional ref whose frontier is folded onto the current branch before reconciling (e.g. a remote-tracking ref after `dolt fetch`)."})
	return ap
}

// EventType returns the type of the event to log.
func (cmd ReconcileCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_MERGE
}

// Exec executes the command.
func (cmd ReconcileCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, reconcileDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if queryist.IsRemote {
		cli.Println(fmt.Sprintf(cli.RemoteUnsupportedMsg, commandStr))
		return 1
	}

	// A reconcile that merges divergent tips can create conflicts; let it stick
	// so the result is resolvable with `dolt conflicts` (mirrors `dolt merge`).
	_, _, _, err = queryist.Queryist.Query(queryist.Context, "set @@dolt_force_transaction_commit = 1")
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	query, err := constructInterpolatedDoltReconcileQuery(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	_, rowIter, _, err := queryist.Queryist.Query(queryist.Context, query)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	rows, err := sql.RowIterToRows(queryist.Context, rowIter)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if len(rows) != 1 {
		cli.Println("Runtime error: reconcile returned an unexpected number of rows:", len(rows))
		return 1
	}

	return printReconcileResult(rows[0])
}

// constructInterpolatedDoltReconcileQuery builds the interpolated CALL for the
// dolt_reconcile() stored procedure, passing the optional ref argument through
// as a bound parameter to prevent sql injection.
func constructInterpolatedDoltReconcileQuery(apr *argparser.ArgParseResults) (string, error) {
	if apr.NArg() == 0 {
		return "CALL DOLT_RECONCILE()", nil
	}
	return dbr.InterpolateForDialect("CALL DOLT_RECONCILE(?)", []interface{}{apr.Arg(0)}, dialect.MySQL)
}

// printReconcileResult renders the (hash, tips, conflicts, message) row that
// dolt_reconcile() returns and picks an exit code: nonzero when the reconcile
// recorded conflicts, so scripts can tell a clean collapse from one that needs
// resolution.
func printReconcileResult(row sql.Row) int {
	tips, _ := getInt64ColAsInt64(row[1])
	conflicts, _ := getInt64ColAsInt64(row[2])

	message := ""
	if row[3] != nil {
		if m, ok := row[3].(string); ok {
			message = m
		}
	}

	if tips <= 1 {
		// No-op (single head) or nothing to do: report the message as-is.
		if message != "" {
			cli.Println(message)
		} else {
			cli.Println("nothing to reconcile: the branch has a single head")
		}
		return 0
	}

	cli.Printf("Reconciled %d heads into a single head.\n", tips)
	if hash, ok := row[0].(string); ok && hash != "" {
		cli.Println("New head:", hash)
	}

	if conflicts > 0 {
		cli.Println("CONFLICT (content): the reconcile recorded conflicts.")
		cli.Println("Use 'dolt conflicts' to investigate and resolve them, then commit the result.")
		return 1
	}

	return 0
}
