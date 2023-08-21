package engine

import (
	"context"

	"testing"
	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func init() {
	// We require MySQL 8.0 collations for the comparisons in the tests
	mySQLVersion := "8.0.0"
	servenv.SetMySQLServerVersionForTest(mySQLVersion)
	collationEnv = collations.NewEnvironment(mySQLVersion)
}
func TestTableSelectEqualUnique(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	sel := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.TableSingleColumn)

	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}
	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}
