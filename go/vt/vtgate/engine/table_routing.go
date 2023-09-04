package engine

import (
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

type TableRoutingParameters struct {
	// Opcode is the execution opcode.
	Opcode Opcode

	LogicTable tableindexes.SplitTableMap

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr
}
