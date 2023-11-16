/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package semantics

import "vitess.io/vitess/go/vt/sqlparser"

// TableAnalyze analyzes the parsed query.
func TableAnalyze(statement sqlparser.Statement, currentDb string, si SchemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(currentDb, newSchemaInfo(si))

	// Analysis for initial scope
	err := analyzer.tableAnalyze(statement)
	if err != nil {
		return nil, err
	}

	// Creation of the semantic table
	semTable := analyzer.newSemTable(statement, si.ConnCollation())

	return semTable, nil
}

func (a *analyzer) tableAnalyze(statement sqlparser.Statement) error {
	_ = sqlparser.Rewrite(statement, a.tableAnalyzeDown, a.tableAnalyzeUp)
	return a.err
}

func (a *analyzer) tableAnalyzeDown(cursor *sqlparser.Cursor) bool {
	// If we have an error we keep on going down the tree without checking for anything else
	// this way we can abort when we come back up.
	if !a.shouldContinue() {
		return true
	}

	if err := a.scoper.down(cursor); err != nil {
		a.setError(err)
		return true
	}
	if err := a.checkForInvalidConstructs(cursor); err != nil {
		a.setError(err)
		return true
	}
	// log any warn in rewriting.
	a.warning = a.rewriter.warning

	a.noteQuerySignature(cursor.Node())

	a.enterProjection(cursor)
	// this is the visitor going down the tree. Returning false here would just not visit the children
	// to the current node, but that is not what we want if we have encountered an error.
	// In order to abort the whole visitation, we have to return true here and then return false in the `analyzeUp` method
	return true
}

func (a *analyzer) tableAnalyzeUp(cursor *sqlparser.Cursor) bool {
	if !a.shouldContinue() {
		return false
	}

	if err := a.scoper.up(cursor); err != nil {
		a.setError(err)
		return false
	}
	if err := a.tables.up(cursor); err != nil {
		a.setError(err)
		return false
	}

	if err := a.binder.up(cursor); err != nil {
		a.setError(err)
		return true
	}

	if err := a.typer.up(cursor); err != nil {
		a.setError(err)
		return false
	}

	a.leaveProjection(cursor)
	return a.shouldContinue()
}
