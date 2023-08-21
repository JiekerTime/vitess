package tableindexes

import "vitess.io/vitess/go/vt/proto/query"

type LogicTableConfig struct {
	LogicTableName   string
	ActualTableCount uint64
	ActualTableList  []ActualTable

	TableIndexColumn Column

	TableIndexRule TableIndexRule

	SequenceColumnName string
}

type ActualTable struct {
	ActualTableName string

	Index int
}

type Column struct {
	ColumnName string

	ColType query.Type
}
