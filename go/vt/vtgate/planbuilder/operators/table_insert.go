package operators

import (
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ ops.Operator = (*TableInsert)(nil)

type TableInsert struct {
	TableColVindexes *vindexes.LogicTableConfig

	//这里分片是个三维数组，因为分片的元数据一个表可以有多个ColVindexes，分表的元数据没有兼容这个所以定义为二维数组
	TableVindexValues [][]evalengine.Expr

	TableVindexValueOffset []int

	// Insert using select query will have select plan as input operator for the insert operation.
	Input ops.Operator
	noColumns
	noPredicates
}

func (t *TableInsert) Clone(inputs []ops.Operator) ops.Operator {
	//TODO implement me
	panic("implement me")
}

func (t *TableInsert) Inputs() []ops.Operator {
	if t.Input == nil {
		return nil
	}
	return []ops.Operator{t.Input}
}

func (t *TableInsert) SetInputs(operators []ops.Operator) {
	//TODO implement me
	panic("implement me")
}

func (t *TableInsert) Description() ops.OpDescription {
	//TODO implement me
	panic("implement me")
}

func (t *TableInsert) ShortDescription() string {
	//TODO implement me
	panic("implement me")
}

func (t *TableInsert) GetOrdering() ([]ops.OrderBy, error) {
	panic("does not expect insert operator to receive get ordering call")
}
