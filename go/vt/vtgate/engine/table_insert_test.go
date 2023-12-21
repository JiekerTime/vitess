package engine

import (
	"context"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestInsertTableShardedSimple(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}},
					},
				},
				SplittableVindexes: map[string]*vschemapb.Vindex{
					"split_table_binaryhash": {
						Type: "split_table_binaryhash",
					},
				},
				SplittableTables: map[string]*vschemapb.SplitTable{
					"t1": {
						TableVindex:       "split_table_binaryhash",
						TableCount:        10,
						TableVindexColumn: []*vschemapb.TableVindexColumn{{Index: 0, Column: "col", ColumnType: sqltypes.Int32}}},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	// A single row insert should be autocommitted
	ins := &Insert{
		Opcode:   InsertTableSharded,
		Ignore:   false,
		Keyspace: ks.Keyspace,
		VindexValues: [][][]evalengine.Expr{{
			// colVindex columns: id
			{
				evalengine.NewLiteralInt(1),
			},
		}},
		Prefix: "prefix ",
		Mid: sqlparser.Values{
			{&sqlparser.Argument{Name: "_id_0", Type: sqltypes.Int64}},
		},
		Suffix:           " suffix",
		TableColVindexes: ks.SplitTableTables["t1"],
		TableVindexValues: [][]evalengine.Expr{
			// colVindex columns: id
			{
				evalengine.NewLiteralInt(2),
			},
		},
	}
	ins.ColVindexes = append(ins.ColVindexes, ks.Tables["t1"].ColumnVindexes...)

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Based on shardForKsid, values returned will be 20-.
		`ResolveDestinations sharded [value:"0"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteBatchMultiShard sharded.20-: prefix t1_7(:_id_0 /* INT64 */) suffix {_id_0: type:INT64 value:"1"} true true`,
	})

	// Multiple rows are not autocommitted by default
	ins = &Insert{
		Opcode:   InsertTableSharded,
		Ignore:   false,
		Keyspace: ks.Keyspace,
		VindexValues: [][][]evalengine.Expr{{
			// colVindex columns: id
			// 3 rows.
			{
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}},
		Prefix: "prefix ",
		Mid: sqlparser.Values{
			{&sqlparser.Argument{Name: "_id_0", Type: sqltypes.Int64}},
			{&sqlparser.Argument{Name: "_id_1", Type: sqltypes.Int64}},
			{&sqlparser.Argument{Name: "_id_2", Type: sqltypes.Int64}},
		},
		Suffix:           " suffix",
		TableColVindexes: ks.SplitTableTables["t1"],
		TableVindexValues: [][]evalengine.Expr{
			{
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		},
	}
	vc = newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	ins.ColVindexes = append(ins.ColVindexes, ks.Tables["t1"].ColumnVindexes...)

	_, err = ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}

	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteBatchMultiShard sharded.20-: prefix t1_0(:_id_2 /* INT64 */) suffix {_id_0: type:INT64 value:"1" _id_2: type:INT64 value:"3"} sharded.-20: prefix t1_7(:_id_1 /* INT64 */) suffix {_id_1: type:INT64 value:"2"} true false`,
		`ExecuteBatchMultiShard sharded.20-: prefix t1_2(:_id_0 /* INT64 */) suffix {_id_0: type:INT64 value:"1" _id_2: type:INT64 value:"3"} true false`,
	})

}

func TestInsertTableShardedGenerate(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}},
					},
				},
				SplittableVindexes: map[string]*vschemapb.Vindex{
					"split_table_binaryhash": {
						Type: "split_table_binaryhash",
					},
				},
				SplittableTables: map[string]*vschemapb.SplitTable{
					"t1": {
						TableVindex:       "split_table_binaryhash",
						TableCount:        10,
						TableVindexColumn: []*vschemapb.TableVindexColumn{{Index: 0, Column: "col", ColumnType: sqltypes.Int32}}},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := &Insert{
		Opcode:   InsertTableSharded,
		Ignore:   false,
		Keyspace: ks.Keyspace,
		VindexValues: [][][]evalengine.Expr{{
			// colVindex columns: id
			// 3 rows.
			{
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}},
		Prefix: "prefix ",
		Mid: sqlparser.Values{
			{&sqlparser.Argument{Name: "_id_0", Type: sqltypes.Int64}},
			{&sqlparser.Argument{Name: "_id_1", Type: sqltypes.Int64}},
			{&sqlparser.Argument{Name: "_id_2", Type: sqltypes.Int64}},
		},
		Suffix:           " suffix",
		TableColVindexes: ks.SplitTableTables["t1"],
		TableVindexValues: [][]evalengine.Expr{
			{
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		},
	}
	ins.ColVindexes = append(ins.ColVindexes, ks.Tables["t1"].ColumnVindexes...)

	ins.Generate = &Generate{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks2",
			Sharded: false,
		},
		Query: "dummy_generate",
		Values: evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(1),
			evalengine.NullExpr,
			evalengine.NewLiteralInt(2),
		),
	}

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"nextval",
				"int64",
			),
			"2",
		),
		{InsertID: 1},
	}

	result, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone dummy_generate n: type:INT64 value:"1" ks2 -20`,
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteBatchMultiShard sharded.20-: prefix t1_0(:_id_2 /* INT64 */) suffix {_id_0: type:INT64 value:"1" _id_2: type:INT64 value:"3"} sharded.-20: prefix t1_7(:_id_1 /* INT64 */) suffix {_id_1: type:INT64 value:"2"} true false`,
		`ExecuteBatchMultiShard sharded.20-: prefix t1_2(:_id_0 /* INT64 */) suffix {_id_0: type:INT64 value:"1" _id_2: type:INT64 value:"3"} true false`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerateFromValues.
	expectResult(t, "Execute", result, &sqltypes.Result{InsertID: 2})
}
