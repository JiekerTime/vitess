package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestGetTableQueries(t *testing.T) {
	tests := []struct {
		name               string
		query              string
		logicTb            *vindexes.LogicTableConfig
		bv                 map[string]*querypb.BindVariable
		expected           []*querypb.BoundQuery
		actualTableNameMap map[string][]vindexes.ActualTable
	}{
		{
			name:  "Select query with table alias",
			query: `SELECT * FROM my_table AS t WHERE t.id = 1`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		},
		{
			name:  "Select query with table name",
			query: `SELECT * FROM my_table WHERE id = 1`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		},
		{
			name:  "Select query with subquery",
			query: "SELECT * FROM (SELECT * FROM my_table) AS t WHERE t.id = 1",
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
					Sql:           `SELECT * FROM (SELECT * FROM my_actual_table_1) AS t WHERE t.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM (SELECT * FROM my_actual_table_2) AS t WHERE t.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 WHERE t2.id = 1`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 WHERE t2.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_2) AS t1) AS t2 WHERE t2.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and aliases",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 WHERE t3.id = 1`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_2 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 JOIN my_table AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 JOIN my_actual_table_1 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_2) AS t1) AS t2 JOIN my_actual_table_2 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases",
			query: `SELECT t1.id, t2.name FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
					Sql:           "SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           "SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_actual_table_2 AS t1) AS t2) AS t3 JOIN my_actual_table_2 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
					Sql:           "SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           "SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_actual_table_2 AS t1) AS t2) AS t3 JOIN my_actual_table_2 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		}, {
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
					Sql:           "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2) as t3 join my_actual_table_1 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_actual_table_2 as t1 limit 5, 10) as t2) as t3 join my_actual_table_2 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset and subquery with limit and offset",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2 LIMIT 10 OFFSET 5) AS t3 LIMIT 10 OFFSET 5) AS t4 JOIN my_table AS t5 ON t4.id = t5.id WHERE t4.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			logicTb: &vindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []vindexes.ActualTable{
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
					Sql:           "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2 limit 5, 10) as t3 limit 5, 10) as t4 join my_actual_table_1 as t5 on t4.id = t5.id where t4.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from (select * from my_actual_table_2 as t1 limit 5, 10) as t2 limit 5, 10) as t3 limit 5, 10) as t4 join my_actual_table_2 as t5 on t4.id = t5.id where t4.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
			actualTableNameMap: map[string][]vindexes.ActualTable{
				"my_table": {
					{ActualTableName: "my_actual_table_1", Index: 0},
					{ActualTableName: "my_actual_table_2", Index: 1},
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

			actual, err := getTableQueries(stmt, test.bv, test.actualTableNameMap)
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
		act      string
		expected string
	}{
		{
			name:     "Select query with table alias",
			query:    `SELECT * FROM my_table AS t WHERE t.id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM my_actual_table_1 AS t WHERE t.id = 1`,
		},
		{
			name:     "Select query with table name",
			query:    `SELECT * FROM my_table WHERE id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM my_actual_table_1 WHERE id = 1`,
		},
		{
			name:     "Select query with subquery",
			query:    "SELECT * FROM (SELECT * FROM my_table) AS t WHERE t.id = 1",
			act:      "my_actual_table_1",
			expected: `SELECT * FROM (SELECT * FROM my_actual_table_1) AS t WHERE t.id = 1`,
		},
		{
			name:     "Select query with multiple subqueries",
			query:    `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 WHERE t2.id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 WHERE t2.id = 1`,
		},
		{
			name:     "Select query with multiple subqueries and aliases",
			query:    `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 WHERE t3.id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
		},
		{
			name:     "Select query with multiple subqueries and table aliases",
			query:    `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 JOIN my_table AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 JOIN my_actual_table_1 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
		},
		{
			name:     "Select query with multiple subqueries and table aliases and column aliases",
			query:    `SELECT t1.id, t2.name FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1`,
			act:      "my_actual_table_1",
			expected: "SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
		},
		{
			name:     "Select query with multiple subqueries and table aliases and column aliases and functions",
			query:    `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name`,
			act:      "my_actual_table_1",
			expected: "SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
		}, {
			name:     "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset",
			query:    `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			act:      "my_actual_table_1",
			expected: "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2) as t3 join my_actual_table_1 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
		},
		{
			name:     "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset and subquery with limit and offset",
			query:    `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2 LIMIT 10 OFFSET 5) AS t3 LIMIT 10 OFFSET 5) AS t4 JOIN my_table AS t5 ON t4.id = t5.id WHERE t4.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			act:      "my_actual_table_1",
			expected: "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2 limit 5, 10) as t3 limit 5, 10) as t4 join my_actual_table_1 as t5 on t4.id = t5.id where t4.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
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

func TestTableRouteGetFields(t *testing.T) {

	logicTable := vindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []vindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("f1"), ColumnType: querypb.Type_VARCHAR}},
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = &logicTable

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
		FieldQuery:      "select f1, f2 from lkp",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			TableOpcode: Scatter,
			LogicTable:  logicTableMap,
			TableValues: Values,
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

func TestTableRouteSelectScatter(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)
	logicTable := &vindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []vindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}},
		TableVindex:      vindex,
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: Scatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			TableOpcode: Scatter,
			LogicTable:  logicTableMap,
			TableValues: []evalengine.Expr{
				evalengine.NewLiteralInt(1),
			},
		},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRoute.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteBatchMultiShard ks.-20: select f1, f2 from lkp_1 {} ks.20-: select f1, f2 from lkp_1 {} false false`,
		`ExecuteBatchMultiShard ks.-20: select f1, f2 from lkp_2 {} ks.20-: select f1, f2 from lkp_2 {} false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

}

func TestTableRouteSelectEqualUnique(t *testing.T) {

	selvIndex, _ := vindexes.NewHash("", nil)
	sel := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	sel.Vindex = selvIndex.(vindexes.SingleColumn)
	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)
	logicTable := &vindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []vindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}},
		TableVindex:      vindex,
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: EqualUnique,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Values: []evalengine.Expr{
			evalengine.NewLiteralInt(1),
		},
		Vindex: selvIndex.(vindexes.SingleColumn),
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			TableOpcode: EqualUnique,
			LogicTable:  logicTableMap,
			TableValues: []evalengine.Expr{
				evalengine.NewLiteralInt(1),
			},
		},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRoute.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteBatchMultiShard ks.-20: select f1, f2 from lkp_1 {} false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

}

func TestTableRouteSelectEqual(t *testing.T) {

	selvIndex, _ := vindexes.NewLookup("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
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

	sel.Vindex = selvIndex.(vindexes.SingleColumn)

	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)
	logicTable := &vindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []vindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("f1"), ColumnType: querypb.Type_VARCHAR}},
		TableVindex:      vindex,
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: Equal,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Values: []evalengine.Expr{
			evalengine.NewLiteralInt(1),
		},
		Vindex: selvIndex.(vindexes.SingleColumn),
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			TableOpcode: Equal,
			LogicTable:  logicTableMap,
			TableValues: []evalengine.Expr{
				evalengine.NewLiteralInt(1),
			},
		},
	}

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFieldsWithTableName(
			"id",
			"int64",
			"lkp",
		),
		"1",
	)

	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"fromc|toc",
					"int64|varbinary",
				),
				"1|\x00",
				"1|\x80",
			),
			wantResult,
		},
	}
	result, err := TableRoute.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceIDs(00,80)`,
		`ExecuteBatchMultiShard ks.-20: select f1, f2 from lkp_1 {} ks.20-: select f1, f2 from lkp_1 {} false false`,
	})
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestTableRouteSelectIN(t *testing.T) {

	selvIndex, _ := vindexes.NewHash("", nil)
	sel := NewRoute(
		IN,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	sel.Vindex = selvIndex.(vindexes.SingleColumn)
	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)
	logicTable := &vindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []vindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}},
		TableVindex:      vindex,
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: IN,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Values: []evalengine.Expr{
			evalengine.TupleExpr{
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(4),
			},
		},
		Vindex: selvIndex.(vindexes.SingleColumn),
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			TableOpcode: IN,
			LogicTable:  logicTableMap,
			TableValues: []evalengine.Expr{
				evalengine.TupleExpr{
					evalengine.NewLiteralInt(1),
					evalengine.NewLiteralInt(2),
					evalengine.NewLiteralInt(4),
				},
			},
		},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRoute.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"4"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`ExecuteBatchMultiShard ks.-20: select f1, f2 from lkp_1 {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"4"}} false false`,
		`ExecuteBatchMultiShard ks.-20: select f1, f2 from lkp_2 {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"4"}} false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

}

func TestSortTableList(t *testing.T) {

	actualTableNameMap := map[string][]vindexes.ActualTable{
		"my_table": {{ActualTableName: "my_actual_table_1", Index: 1}, {ActualTableName: "my_actual_table_0", Index: 0}, {ActualTableName: "my_actual_table_3", Index: 3}},
	}

	SortTableList(actualTableNameMap)

	for _, table := range actualTableNameMap {
		print(table)
	}
}

func TestTableRouteSort(t *testing.T) {
	shardRouteParam := &RoutingParameters{
		Opcode: Unsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
	}
	tableIndexColumn := []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}}
	tableName := "t_user"
	sel := newTestTableRoute(shardRouteParam, tableName, tableIndexColumn, Scatter)
	sqlStmt, _, _ := sqlparser.Parse2("select id from t_user order by id")
	sel.Query = sqlStmt
	sel.FieldQuery = "select col1 from t_user where 1 != 1"
	sel.OrderBy = []OrderByParams{{
		Col:             0,
		WeightStringCol: -1,
	}}

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"int64",
				),
				"1",
				"1",
				"3",
				"2",
			),
		},
	}

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		"1",
		"1",
		"2",
		"3",
	)
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	expectResult(t, "sel.Execute", result, wantResult)

	sel.OrderBy[0].Desc = true
	vc.Rewind()
	result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	wantResult = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		"3",
		"2",
		"1",
		"1",
	)
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestTableRouteSortTruncate(t *testing.T) {
	shardRouteParam := &RoutingParameters{
		Opcode: Unsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
	}
	tableIndexColumn := []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}}
	tableName := "t_user"
	sel := newTestTableRoute(shardRouteParam, tableName, tableIndexColumn, Scatter)
	sqlStmt, _, _ := sqlparser.Parse2("dummy_select")
	sel.Query = sqlStmt
	sel.FieldQuery = "dummy_select_field"
	sel.OrderBy = []OrderByParams{{
		Col: 0,
	}}
	sel.TruncateColumnCount = 1

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|col",
					"int64|int64",
				),
				"1|1",
				"1|1",
				"3|1",
				"2|1",
			),
		},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		"1",
		"1",
		"2",
		"3",
	)
	expectResult(t, "sel.Execute", result, wantResult)
}

func newTestTableRoute(shardRouteParam *RoutingParameters, tableName string, tableIndexColumn []*vindexes.TableColumn, tableOpcode Opcode) *TableRoute {
	return &TableRoute{
		TableName:       tableName,
		ShardRouteParam: shardRouteParam,
		TableRouteParam: &TableRoutingParameters{
			TableOpcode: tableOpcode,
			LogicTable:  getTestLogicTableConfig(tableName, tableIndexColumn, nil),
		},
	}
}

func getTestLogicTableConfig(tableName string, tableIndexColumn []*vindexes.TableColumn, tableVindex vindexes.Vindex) vindexes.SplitTableMap {
	if len(tableIndexColumn) == 0 {
		tableIndexColumn = []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}}
		tableName = "t_user"
		tableVindex, _ = vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTable := vindexes.LogicTableConfig{
		LogicTableName: tableName,
		ActualTableList: []vindexes.ActualTable{
			{
				ActualTableName: tableName + "_1",
				Index:           0,
			},
			{
				ActualTableName: tableName + "_2",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: tableIndexColumn,
		TableVindex:      tableVindex,
	}
	logicTableMap[tableName] = &logicTable
	return logicTableMap
}
