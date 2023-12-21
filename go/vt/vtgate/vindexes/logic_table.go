package vindexes

import (
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

type SplitTableMap map[string]*LogicTableConfig

type LogicTableConfig struct {
	LogicTableName     string                  `json:"logic_table_name,omitempty"`
	TableVindex        Vindex                  `json:"table_vindex,omitempty"`
	TableCount         int32                   `json:"table_count,omitempty"`
	TableIndexColumn   []*TableColumn          `json:"table_vindex_column,omitempty"`
	Params             map[string]*TableParams `json:"params,omitempty"`
	ActualTableList    []ActualTable
	SequenceColumnName string
}

type ActualTable struct {
	ActualTableName string
	Index           int
}
type TableParams struct {
	Name  string
	Index int
}

type TableColumn struct {
	Column     sqlparser.IdentifierCI `json:"column"`
	ColumnType query.Type             `json:"column_type"`
	Index      int32                  `json:"index"`
}

// GetFirstActualTableMap Gets the first table mapping of the split table
// It is used in FieldQuery
func GetFirstActualTableMap(logicTable SplitTableMap) map[string]string {
	firstActualTable := make(map[string]string)
	for key, value := range logicTable {
		firstActualTable[key] = value.ActualTableList[0].ActualTableName
	}
	return firstActualTable
}

// GetSingleFirstActualTableMap Gets the first table mapping of the split table  for single table
func GetSingleFirstActualTableMap(logicTable *LogicTableConfig) map[string]string {
	firstActualTable := make(map[string]string)
	firstActualTable[logicTable.LogicTableName] = logicTable.ActualTableList[0].ActualTableName
	return firstActualTable
}
