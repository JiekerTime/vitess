package tableindexes

import "vitess.io/vitess/go/vt/proto/query"

type LogicTable struct {
	LogicTableName string

	ActualTableList []ActualTable

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
