package plancontext

type LogicTableConfig struct {
	LogicTable         string
	ActualTableExprs   string
	ShardingColumnName string
	ShardingColumnType string
	ShardingAlgorithms string
	SequenceColumnName string
}
