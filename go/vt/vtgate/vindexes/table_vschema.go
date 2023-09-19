/*
Copyright 2019 The Vitess Authors.

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

package vindexes

import (
	"strconv"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type SplitTable struct {
	LogicTableName    sqlparser.IdentifierCS `json:"logic_table_name,omitempty"`
	TableVindex       string                 `json:"table_vindex,omitempty"`
	TableVindexColumn []*TableVindexColumn   `json:"table_vindex_column,omitempty"`
	TableCount        int32                  `json:"table_count,omitempty"`
	ActualTables      []*ActualTable
}

type TableVindexColumn struct {
	Index      int32                  `json:"index"`
	Column     sqlparser.IdentifierCI `json:"column"`
	ColumnType querypb.Type           `json:"column_type"`
}

// FindSplitTableOrVindex finds a table or a Vindex by name using Find and FindVindex.
func (vschema *VSchema) FindSplitTableOrVindex(keyspace, tableName string) (*LogicTableConfig, Vindex, error) {
	tables, err := vschema.FindSplitTable(keyspace, tableName)
	if err != nil {
		return nil, nil, err
	}
	if tables != nil {
		return tables, nil, nil
	}
	v, err := vschema.FindSplitTableVindex(keyspace, tableName)
	if err != nil {
		return nil, nil, err
	}
	if v != nil {
		return nil, v, nil
	}
	return nil, nil, NotFoundError{TableName: tableName}
}

// FindSplitTableVindex finds a split table  vindex by name. If a keyspace is specified, only
// split table vindexes from that keyspace are searched. If no kesypace is specified, then a split table
// vindex is returned only if its name is unique across all keyspaces. The function
// returns an error only if the split table vindex name is ambiguous.
func (vschema *VSchema) FindSplitTableVindex(keyspace, name string) (Vindex, error) {
	if keyspace == "" {
		vindex, ok := vschema.uniqueVindexes[name]
		if vindex == nil && ok {
			return nil, vterrors.Errorf(
				vtrpcpb.Code_FAILED_PRECONDITION,
				"ambiguous split table vindex reference: %s",
				name,
			)
		}
		return vindex, nil
	}
	splitTableVindex, ok := vschema.Keyspaces[keyspace].SplitTableVindexes[name]
	if !ok {
		return nil, vterrors.VT05003(keyspace)
	}
	return splitTableVindex, nil
}

// logicToActualTable split table  logic table -> all actual tables list
func logicToActualTable(logicTableName string, tableIndex int, table *LogicTableConfig) error {
	if len(logicTableName) == 0 {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"logic to actual table name failed table name is:%v",
			logicTableName,
		)
	}
	if tableIndex < 0 || int(table.TableCount) < tableIndex {
		return vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"logic to actual table name failed '%v' for table index: %v TableCount: %v",
			logicTableName,
			tableIndex,
			table.TableCount,
		)
	}
	splitTableIndex := "_" + strconv.Itoa(tableIndex)
	position := len(logicTableName)
	if logicTableName[0] == '`' && logicTableName[len(logicTableName)-1] == '`' {
		position = len(logicTableName) - 1
	}
	var builder strings.Builder
	builder.WriteString(logicTableName[:position])
	builder.WriteString(splitTableIndex)
	builder.WriteString(logicTableName[position:])
	table.ActualTableList = append(table.ActualTableList, ActualTable{ActualTableName: builder.String(), Index: tableIndex})
	return nil
}

func buildSplitTables(ks *vschemapb.Keyspace, vschema *VSchema, ksvschema *KeyspaceSchema) error {
	for vname, vindexInfo := range ks.SplittableVindexes {
		vindex, err := CreateVindex(vindexInfo.Type, vname, vindexInfo.Params)
		if err != nil {
			return err
		}

		// If the keyspace requires explicit routing, don't include its indexes
		// in global routing.
		if !ks.RequireExplicitRouting {
			if _, ok := vschema.uniqueVindexes[vname]; ok {
				vschema.uniqueVindexes[vname] = nil
			} else {
				vschema.uniqueVindexes[vname] = vindex
			}
		}
		ksvschema.SplitTableVindexes[vname] = vindex
	}
	for tname, table := range ks.SplittableTables {
		t := &LogicTableConfig{
			LogicTableName: tname,
			TableVindex:    ksvschema.SplitTableVindexes[table.TableVindex],
			TableCount:     table.TableCount,
		}
		// Initialize Columns.
		colNames := make(map[string]bool)
		for _, col := range table.TableVindexColumn {
			name := sqlparser.NewIdentifierCI(col.Column)
			if colNames[name.Lowered()] {
				return vterrors.Errorf(
					vtrpcpb.Code_INVALID_ARGUMENT,
					"duplicate column name '%v' for table: %s",
					name,
					tname,
				)
			}
			colNames[name.Lowered()] = true
			t.TableIndexColumn = append(t.TableIndexColumn, &TableColumn{Column: col.Column, Index: col.Index, ColumnType: col.ColumnType})
		}
		for tableIndex := int32(0); tableIndex < t.TableCount; tableIndex++ {
			if err := logicToActualTable(t.LogicTableName, int(tableIndex), t); err != nil {
				return err
			}
		}

		// Add the table to the map entries.
		ksvschema.SplitTableTables[tname] = t
	}
	return nil
}

func (ks *KeyspaceSchema) findSplitTable(
	tableName string,
) *LogicTableConfig {
	table := ks.SplitTableTables[tableName]
	if table != nil {
		return table
	}
	return nil
}

func (vschema *VSchema) FindSplitTable(
	keyspace,
	tableName string,
) (*LogicTableConfig, error) {
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return nil, vterrors.VT05003(keyspace)
	}
	table := ks.findSplitTable(tableName)
	if table == nil {
		return nil, vterrors.VT05004(tableName)
	}
	return table, nil
}

func (vschema *VSchema) FindActualTable(
	keyspace,
	logicTableName string,
) (*LogicTableConfig, error) {
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return nil, vterrors.VT05003(keyspace)
	}
	table := ks.findSplitTable(logicTableName)
	if table == nil {
		return nil, vterrors.VT05004(logicTableName)
	}
	//table.
	//return table, nil
	return nil, nil
}
