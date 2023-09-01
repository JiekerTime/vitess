package tableindexes

import "vitess.io/vitess/go/vt/proto/query"

type SplitTableMap map[string]LogicTableConfig

type LogicTableConfig struct {
	LogicTableName     string
	ActualTableList    []ActualTable
	TableIndexColumn   *Column
	TableIndexRule     TableIndexRule
	SequenceColumnName string
}

type ActualTable struct {
	ActualTableName string
	Index           int
}

type Column struct {
	ColumnName string
	ColType    query.Type
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
