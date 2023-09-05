package tableindexes

import (
	"vitess.io/vitess/go/vt/proto/query"
)

type SplitTableMap map[string]LogicTableConfig

type LogicTableConfig struct {
	LogicTableName     string    `json:"logic_table_name,omitempty"`
	TableVindex        string    `json:"table_vindex,omitempty"`
	TableCount         int32     `json:"table_count,omitempty"`
	TableIndexColumn   []*Column `json:"table_vindex_column,omitempty"`
	ActualTableList    []ActualTable
	TableIndexRule     TableIndexRule
	SequenceColumnName string
}

type ActualTable struct {
	ActualTableName string
	Index           int
}

type Column struct {
	Column     string     `json:"column"`
	ColumnType query.Type `json:"column_type"`
	Index      int32      `json:"index"`
}

// GetFirstActualTableMap Gets the first table mapping of the splitable
// It is used in filedquery
func GetFirstActualTableMap(logicTable SplitTableMap) map[string]string {
	firstActualTable := make(map[string]string)
	for key, value := range logicTable {
		firstActualTable[key] = value.ActualTableList[0].ActualTableName
	}
	return firstActualTable
}
