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
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/hack"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	TableIndexTypeList      = "split_table_list"
	TableIndexTypeRangeMM   = "split_table_range_mm"
	RangeMMTableCount       = 12
	TableIndexTypeRangeMMDD = "split_table_range_mmdd"
	RangeMMDDTableCount     = 366
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

// LogicToActualTable split table  logic table -> all actual tables list
func LogicToActualTable(logicTableName string, tableIndex int, tableIndexType string, table *LogicTableConfig) (string, error) {
	if len(logicTableName) == 0 {
		return "", vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"logic to actual table name failed table name is:%v",
			logicTableName,
		)
	}
	if tableIndex < 0 || int(table.TableCount) < tableIndex {
		return "", vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT,
			"logic to actual table name failed '%v' for table index: %v TableCount: %v",
			logicTableName,
			tableIndex,
			table.TableCount,
		)
	}
	tableNameIndexStr := ""
	switch tableIndexType {
	case TableIndexTypeList:
		for _, value := range table.Params {
			if value.Index == tableIndex {
				tableNameIndexStr = value.Name
			}
		}
	default:
		tableNameIndexStr = strconv.Itoa(tableIndex)
	}
	splitTableIndex := "_" + tableNameIndexStr
	position := len(logicTableName)
	if logicTableName[0] == '`' && logicTableName[len(logicTableName)-1] == '`' {
		position = len(logicTableName) - 1
	}
	var builder strings.Builder
	builder.WriteString(logicTableName[:position])
	builder.WriteString(splitTableIndex)
	builder.WriteString(logicTableName[position:])
	actualTable := builder.String()
	table.ActualTableList = append(table.ActualTableList, ActualTable{ActualTableName: actualTable, Index: tableIndex})
	return actualTable, nil
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
		data := make(map[string][]string)
		list := make(map[string]*TableParams)
		if _, ok := ks.SplittableVindexes[table.TableVindex]; !ok {
			break
		}
		switch ks.SplittableVindexes[table.TableVindex].Type {
		case TableIndexTypeRangeMM:
			table.TableCount = RangeMMTableCount
		case TableIndexTypeRangeMMDD:
			table.TableCount = RangeMMDDTableCount
		case TableIndexTypeList:

			err := json.Unmarshal(hack.StringBytes(table.Params["json"]), &data)
			if err != nil {
				return err
			}
			var actualTableList []string
			for key, value := range data {
				actualTableList = append(actualTableList, key)
				for _, listValue := range value {
					if _, ok := list[listValue]; ok {
						return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, " Unable to configure duplicate data:%v", listValue)
					}
					list[listValue] = &TableParams{Name: key}
				}
			}
			sort.Strings(actualTableList)
			for key, actualTable := range list {
				for idx, actualTableTag := range actualTableList {
					if actualTable.Name == actualTableTag {
						list[key].Index = idx
					}
				}
			}
			table.TableCount = int32(len(actualTableList))
		}
		t := &LogicTableConfig{
			LogicTableName: tname,
			TableVindex:    ksvschema.SplitTableVindexes[table.TableVindex],
			TableCount:     table.TableCount,
			Params:         list,
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
			t.TableIndexColumn = append(t.TableIndexColumn, &TableColumn{Column: sqlparser.NewIdentifierCI(col.Column), Index: col.Index, ColumnType: col.ColumnType})
		}
		for tableIndex := int32(0); tableIndex < t.TableCount; tableIndex++ {

			actualTable, err := LogicToActualTable(t.LogicTableName, int(tableIndex), ks.SplittableVindexes[table.TableVindex].Type, t)
			if err != nil {
				return err
			}
			ksvschema.SplitTableActualTables[actualTable] = struct{}{}
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

func (ks *KeyspaceSchema) findSplitAllTables() (tables map[string]*LogicTableConfig) {
	return ks.SplitTableTables
}

func (ks *KeyspaceSchema) findAllTables() (tables map[string]*Table) {
	return ks.Tables
}

func (vschema *VSchema) FindAllTables(keyspace string) (map[string]*LogicTableConfig, map[string]*Table, error) {
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return nil, nil, vterrors.VT05003(keyspace)
	}
	splitTables := ks.findSplitAllTables()
	tables := ks.findAllTables()
	return splitTables, tables, nil
}

func (ks *KeyspaceSchema) isSplitTableActualTable(
	tableName string,
) bool {
	_, exists := ks.SplitTableActualTables[tableName]
	return exists
}

func (vschema *VSchema) IsSplitTableActualTable(keyspace, tableName string) (bool, error) {
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return false, vterrors.VT05003(keyspace)
	}
	res := ks.isSplitTableActualTable(tableName)

	return res, nil
}
