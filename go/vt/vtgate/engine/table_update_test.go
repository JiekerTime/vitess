package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestTableUpdateEqual(t *testing.T) {
	updStmt, _, _ := sqlparser.Parse2("update t_user set a = 1 where col = 5")
	upd := &TableUpdate{
		TableDML: &TableDML{
			AST:   updStmt,
			Table: []*vindexes.Table{{Name: sqlparser.NewIdentifierCS("t_user")}},
			ShardRouteParam: &RoutingParameters{
				Opcode: Scatter,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
			},
			TableRouteParam: &TableRoutingParameters{
				TableOpcode: EqualUnique,
				LogicTable:  getTestLogicTableConfig("", nil, nil),
				TableValues: []evalengine.Expr{evalengine.NewLiteralInt(5)},
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: update t_user_1 set a = 1 where col = 5 {} ks.20-: update t_user_1 set a = 1 where col = 5 {} true false`,
	})

	// Failure case
	upd.TableRouteParam.TableValues = []evalengine.Expr{evalengine.NewBindVar("aa", sqltypes.Unknown, collations.Unknown)}
	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `query arguments missing for aa`)
}

func TestTableUpdateEqual2(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	updStmt, _, _ := sqlparser.Parse2("update t_user set a = 1 where col =99 and id=1")
	upd := &TableUpdate{
		TableDML: &TableDML{
			AST:   updStmt,
			Table: []*vindexes.Table{{Name: sqlparser.NewIdentifierCS("t_user")}},
			ShardRouteParam: &RoutingParameters{
				Opcode: EqualUnique,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			TableRouteParam: &TableRoutingParameters{
				TableOpcode: EqualUnique,
				LogicTable:  getTestLogicTableConfig("", nil, nil),
				TableValues: []evalengine.Expr{evalengine.NewLiteralInt(99)},
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: update t_user_2 set a = 1 where col = 99 and id = 1 {} true true`,
	})
}

func TestTableUpdateScatter(t *testing.T) {
	updStmt, _, _ := sqlparser.Parse2("update t_user set a = 1 ")
	upd := &TableUpdate{
		TableDML: &TableDML{
			AST:   updStmt,
			Table: []*vindexes.Table{{Name: sqlparser.NewIdentifierCS("t_user")}},
			ShardRouteParam: &RoutingParameters{
				Opcode: Scatter,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
			},
			TableRouteParam: &TableRoutingParameters{
				TableOpcode: Scatter,
				LogicTable:  getTestLogicTableConfig("", nil, nil),
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: update t_user_1 set a = 1 {} ks.20-: update t_user_1 set a = 1 {} true false`,
		`ExecuteMultiShard ks.-20: update t_user_2 set a = 1 {} ks.20-: update t_user_2 set a = 1 {} true false`,
	})
}
