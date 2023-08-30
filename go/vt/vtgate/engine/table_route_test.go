package engine

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

func TestGetTableQueries(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		logicTb  tableindexes.LogicTableConfig
		bv       map[string]*querypb.BindVariable
		expected []*querypb.BoundQuery
	}{
		{
			name:  "Update query",
			query: `UPDATE my_table SET my_column = 'new_value' WHERE id = 1`,
			logicTb: tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `UPDATE my_actual_table_1 SET my_column = 'new_value' WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `UPDATE my_actual_table_2 SET my_column = 'new_value' WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Delete query",
			query: `DELETE FROM my_table WHERE id = 1`,
			logicTb: tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `DELETE FROM my_actual_table_1 WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `DELETE FROM my_actual_table_2 WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Insert query",
			query: `INSERT INTO my_table (my_column) VALUES ('new_value')`,
			logicTb: tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `INSERT INTO my_actual_table_1(my_column) VALUES ('new_value')`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `INSERT INTO my_actual_table_2(my_column) VALUES ('new_value')`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with table alias",
			query: `SELECT * FROM my_table AS t WHERE t.id = 1`,
			logicTb: tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `SELECT * FROM my_actual_table_1 AS t WHERE t.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM my_actual_table_2 AS t WHERE t.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with table name",
			query: `SELECT * FROM my_table WHERE id = 1`,
			logicTb: tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `SELECT * FROM my_actual_table_1 WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM my_actual_table_2 WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(test.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			actual, err := getTableQueries(stmt, test.logicTb, test.bv)
			if err != nil {
				t.Fatalf(err.Error())
			}

			if len(actual) != len(test.expected) {
				t.Errorf("Unexpected result. Expected: %v, Actual: %v", test.expected, actual)
			}

			for i, query := range test.expected {
				if !strings.EqualFold(query.Sql, actual[i].Sql) {
					t.Errorf("Unexpected result. Expected: %v, Actual: %v", test.expected, actual)
				}
			}
		})
	}
}

func TestRewriteQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		act      tableindexes.ActualTable
		expected string
	}{
		{
			name:  "Update query",
			query: `UPDATE my_table SET my_column = 'new_value' WHERE id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table",
			},
			expected: `UPDATE my_actual_table SET my_column = 'new_value' WHERE id = 1`,
		},
		{
			name:  "Delete query",
			query: `DELETE FROM my_table WHERE id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table",
			},
			expected: `DELETE FROM my_actual_table WHERE id = 1`,
		},
		{
			name:  "Insert query",
			query: `INSERT INTO my_table (my_column)VALUES ('new_value')`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table",
			},
			expected: `INSERT INTO my_actual_table(my_column) VALUES ('new_value')`,
		},
		{
			name:  "Select query with table alias",
			query: `SELECT * FROM my_table AS t WHERE t.id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `SELECT * FROM my_actual_table_1 AS t WHERE t.id = 1`,
		},
		{
			name:  "Select query with table name",
			query: `SELECT * FROM my_table WHERE id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `SELECT * FROM my_actual_table_1 WHERE id = 1`,
		},
		// TODO: support joining
		{
			name:  "join query with table name",
			query: `select id from t_unshard join my_table`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `select id from t_unshard join my_actual_table_1`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(test.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			actual, err := rewriteQuery(stmt, test.act, "my_table")
			if err != nil {
				t.Fatalf("Failed to rewrite query: %v", err)
			}

			if !strings.EqualFold(actual, test.expected) {
				t.Errorf("Unexpected result. Expected: %s, Actual: %s", test.expected, actual)
			}
		})
	}
}

func TestResultMerge(t *testing.T) {
	resultSlice := []sqltypes.Result{
		{
			Fields: []*querypb.Field{
				// 定义字段
				{
					Name:  "id",
					Type:  sqltypes.Int64,
					Table: "test_001",
				},
				{
					Name:  "name",
					Type:  sqltypes.VarChar,
					Table: "test_002",
				},
			},
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				// 定义行数据
				{
					sqltypes.NewInt64(1),
					sqltypes.NewVarChar("John"),
				},
				{
					sqltypes.NewInt64(2),
					sqltypes.NewVarChar("Jane"),
				},
			},
		},

		{
			Fields: []*querypb.Field{
				// 定义字段
				{
					Name:  "id",
					Type:  sqltypes.Int64,
					Table: "test_003",
				},
				{
					Name:  "name",
					Type:  sqltypes.VarChar,
					Table: "test_004",
				},
			},
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				// 定义行数据
				{
					sqltypes.NewInt64(3),
					sqltypes.NewVarChar("Sto"),
				},
				{
					sqltypes.NewInt64(4),
					sqltypes.NewVarChar("Uve"),
				},
			},
		},
	}

	wantResult := &sqltypes.Result{

		Fields: []*querypb.Field{
			// 定义字段
			{
				Name:  "id",
				Type:  sqltypes.Int64,
				Table: "test",
			},
			{
				Name:  "name",
				Type:  sqltypes.VarChar,
				Table: "test",
			},
		},
		RowsAffected: 4,
		Rows: [][]sqltypes.Value{
			// 定义行数据
			{
				sqltypes.NewInt64(1),
				sqltypes.NewVarChar("John"),
			},
			{
				sqltypes.NewInt64(2),
				sqltypes.NewVarChar("Jane"),
			},
			// 定义行数据
			{
				sqltypes.NewInt64(3),
				sqltypes.NewVarChar("Sto"),
			},
			{
				sqltypes.NewInt64(4),
				sqltypes.NewVarChar("Uve"),
			},
		},
	}

	finalResult, _ := resultMerge("test", resultSlice)

	if !finalResult.Equal(wantResult) {
		t.Errorf("merge error !")
	}

}

func printResult(result sqltypes.Result) {
	// 打印字段名称
	for _, field := range result.Fields {
		fmt.Printf("%s(%s)\t", field.Name, field.Table)
	}

	fmt.Println()

	// 打印行数据
	for _, row := range result.Rows {
		for _, value := range row {
			fmt.Printf("%s\t", value.String())
		}
		fmt.Println()
	}
}

func TestTableRouteGetFields(t *testing.T) {

	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           1,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           2,
			},
		},
		TableIndexColumn: tableindexes.Column{ColumnName: "f1", ColType: querypb.Type_VARCHAR},
	}

	logicTableMap := make(map[string]tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: Scatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	Values := []evalengine.Expr{
		evalengine.TupleExpr{
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(4),
		},
	}

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			Opcode:     Scatter,
			LogicTable: logicTableMap,
			Values:     Values,
		},
	}

	resultSlice := make([]*sqltypes.Result, 0)

	result1 := &sqltypes.Result{

		Fields: []*querypb.Field{
			// 定义字段
			{
				Name: "id",
				Type: sqltypes.Int64,
			},
			{
				Name: "name",
				Type: sqltypes.VarChar,
			},
		},
	}

	resultSlice = append(resultSlice, result1)

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: resultSlice,
	}

	got, err := TableRoute.GetFields(context.Background(), vc, map[string]*querypb.BindVariable{})

	require.NoError(t, err)
	if !got.Equal(result1) {
		t.Errorf("l.GetFields:\n%v, want\n%v", got, result1)
	}

}

func TestTableRouteTryExecute(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table": "lkp",
		"f1":    "f1",
		"f2":    "f2",
	})
	sel := NewRoute(
		Equal,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           1,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           2,
			},
		},
		TableIndexColumn: tableindexes.Column{ColumnName: "f1", ColType: querypb.Type_VARCHAR},
	}

	logicTableMap := make(map[string]tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: Scatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	Values := []evalengine.Expr{
		evalengine.TupleExpr{
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(4),
		},
	}

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			Opcode:     Scatter,
			LogicTable: logicTableMap,
			Values:     Values,
		},
	}

	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRoute.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: select f1, f2 from lkp_1 {} ks.20-: select f1, f2 from lkp_2 {} false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

}
