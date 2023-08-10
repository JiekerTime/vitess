package planbuilder

import (
	"testing"
	oprewriters "vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
)

func TestTableOne(t *testing.T) {
	oprewriters.DebugOperatorTree = true
	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/table_schema.json", true),
	}

	testFile(t, "table_onecase.json", "", vschema, false)
}
