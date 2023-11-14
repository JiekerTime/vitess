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

package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func buildOtherReadAndAdmin(sql string, vschema plancontext.VSchema) (*planResult, error) {
	destination, keyspace, _, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}

	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql, // This is original sql query to be passed as the parser can provide partial ddl AST.
		SingleShardOnly:   true,
	}), nil
}

// explain support split table
func buildOtherReadAndAdminForSplitTable(stmt sqlparser.ExplainStmt, mapForSplitTable map[string]string, destination key.Destination, keyspace *vindexes.Keyspace) (*planResult, error) {
	cloneStmt := sqlparser.DeepCloneStatement(stmt.Statement)
	sqlparser.RewriteSplitTableName(cloneStmt, mapForSplitTable)
	stmt.Statement = cloneStmt
	sql := sqlparser.String(&stmt)
	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql, //This is original sql query to be passed as the parser can provide partial ddl AST.
		SingleShardOnly:   true,
	}), nil
}

// get split table information
func getSplitTableInfo(stmt sqlparser.ExplainStmt, vschema plancontext.VSchema) (key.Destination, *vindexes.Keyspace, map[string]string, bool, error) {
	destination, keyspace, _, err := vschema.TargetDestination("")
	if err != nil {
		return nil, nil, nil, false, err
	}

	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}

	semTable, _ := semantics.Analyze(stmt.Statement, ksName, vschema)
	found := false
	mapForSplitTable := make(map[string]string)
	if semTable.Tables != nil {
		for _, info := range semTable.Tables {
			if realTable, ok := info.(*semantics.RealTable); ok {
				tableName := realTable.Table.Name.String()
				splitTableConfig, err := vschema.FindSplitTable(ksName, tableName)
				if err != nil {
					continue
				}
				mapForSplitTable[tableName] = splitTableConfig.ActualTableList[0].ActualTableName
				found = true
			}
		}
	}

	return destination, keyspace, mapForSplitTable, found, nil
}
