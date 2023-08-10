package engine

import (
	"encoding/json"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

type TableOpCode int

const (
	TableEqualUnique = TableOpCode(iota)

	TableIn

	TableScatter
)

var tableOpName = map[TableOpCode]string{
	TableEqualUnique: "TableEqualUnique",
	TableIn:          "TableIn",
	TableScatter:     "TableScatter",
}

// MarshalJSON serializes the Opcode as a JSON string.
// It's used for testing and diagnostics.
func (code TableOpCode) MarshalJSON() ([]byte, error) {
	return json.Marshal(tableOpName[code])
}

// String returns a string presentation of this opcode
func (code TableOpCode) String() string {
	return tableOpName[code]
}

type TableRoutingParameters struct {
	// Opcode is the execution opcode.
	Opcode TableOpCode

	tableIndex tableindexes.TableIndex

	logicTables []tableindexes.LogicTable

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr
}
