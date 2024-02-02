package engine

import (
	"bufio"
	"context"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestTokenRewriteMethod(t *testing.T) {

	tests := []struct {
		name            string
		query           string
		bvs             map[string]*querypb.BindVariable
		logicalTables   []string
		exceptedQueries []string
	}{
		{
			name:  "Simple query",
			query: "select id from t_user",
			exceptedQueries: []string{
				"select id from t_user_0",
				"select id from t_user_1",
			},
			logicalTables: []string{"t_user"},
			bvs:           map[string]*querypb.BindVariable{},
		},

		{
			name:          "Select query with table alias",
			query:         `SELECT * FROM my_table AS t WHERE t.id = 1`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				`SELECT * FROM my_table_0 AS t WHERE t.id = 1`,
				`SELECT * FROM my_table_1 AS t WHERE t.id = 1`,
			},
		},
		{
			name:          "Select query with table name",
			query:         `SELECT * FROM my_table WHERE id = 1`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				`SELECT * FROM my_table_0 WHERE id = 1`,
				`SELECT * FROM my_table_1 WHERE id = 1`,
			},
		},
		{
			name:          "Select query with subquery",
			query:         "SELECT * FROM (SELECT * FROM my_table) AS t WHERE t.id = 1",
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				`SELECT * FROM (SELECT * FROM my_table_0) AS t WHERE t.id = 1`,
				`SELECT * FROM (SELECT * FROM my_table_1) AS t WHERE t.id = 1`,
			},
		},
		{
			name:          "Select query with multiple subqueries",
			query:         `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 WHERE t2.id = 1`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				`SELECT * FROM (SELECT * FROM (SELECT * FROM my_table_0) AS t1) AS t2 WHERE t2.id = 1`,
				`SELECT * FROM (SELECT * FROM (SELECT * FROM my_table_1) AS t1) AS t2 WHERE t2.id = 1`,
			},
		},
		{
			name:          "Select query with multiple subqueries and aliases",
			query:         `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 WHERE t3.id = 1`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				`SELECT * FROM (SELECT * FROM (SELECT * FROM my_table_0 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
				`SELECT * FROM (SELECT * FROM (SELECT * FROM my_table_1 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
			},
		},
		{
			name:          "Select query with multiple subqueries and table aliases",
			query:         `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 JOIN my_table AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				`SELECT * FROM (SELECT * FROM (SELECT * FROM my_table_0) AS t1) AS t2 JOIN my_table_0 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
				`SELECT * FROM (SELECT * FROM (SELECT * FROM my_table_1) AS t1) AS t2 JOIN my_table_1 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
			},
		},
		{
			name:          "Select query with multiple subqueries and table aliases and column aliases",
			query:         `SELECT t1.id, t2.name FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				"SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_table_0 AS t1) AS t2) AS t3 JOIN my_table_0 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
				"SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_table_1 AS t1) AS t2) AS t3 JOIN my_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
			},
		},
		{
			name:          "Select query with multiple subqueries and table aliases and column aliases and functions",
			query:         `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				"SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table_0 AS t1) AS t2) AS t3 JOIN my_table_0 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
				"SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table_1 AS t1) AS t2) AS t3 JOIN my_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
			},
		},
		{
			name:          "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset",
			query:         `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				"select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_table_0 as t1 limit 5, 10) as t2) as t3 join my_table_0 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
				"select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_table_1 as t1 limit 5, 10) as t2) as t3 join my_table_1 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
			},
		},
		{
			name:          "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset and subquery with limit and offset",
			query:         `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2 LIMIT 10 OFFSET 5) AS t3 LIMIT 10 OFFSET 5) AS t4 JOIN my_table AS t5 ON t4.id = t5.id WHERE t4.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			logicalTables: []string{"my_table"},
			bvs:           map[string]*querypb.BindVariable{},
			exceptedQueries: []string{
				"select t1.id, t2.`name`, max(t3.age) from (select * from (select * from (select * from my_table_0 as t1 limit 5, 10) as t2 limit 5, 10) as t3 limit 5, 10) as t4 join my_table_0 as t5 on t4.id = t5.id where t4.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
				"select t1.id, t2.`name`, max(t3.age) from (select * from (select * from (select * from my_table_1 as t1 limit 5, 10) as t2 limit 5, 10) as t3 limit 5, 10) as t4 join my_table_1 as t5 on t4.id = t5.id where t4.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
			},
		},
		{
			name:  "Multi tables",
			query: "select * from t_user join t_order",
			exceptedQueries: []string{
				"select * from t_user_0 join t_order_0",
				"select * from t_user_1 join t_order_1",
			},
			logicalTables: []string{"t_user", "t_order"},
			bvs:           map[string]*querypb.BindVariable{},
		},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, _, _ := sqlparser.Parse2(test.query)
			tableRoute := getTableRoute(stmt, test.logicalTables)
			actualTableMap, _ := tableRoute.TableRouteParam.findTableRoute(context.Background(), vc, test.bvs)
			queries, _ := tableRoute.TableRouteParam.getTableQueries(tableRoute.Query, test.bvs, actualTableMap)

			if len(queries) != len(test.exceptedQueries) {
				t.Errorf("Unexpected result count. Expected: %v, Actual: %v", len(test.exceptedQueries), len(queries))
			}

			for i, query := range queries {
				if !strings.EqualFold(query.Sql, test.exceptedQueries[i]) {
					t.Errorf("Unexpected result. Expected: %v, Actual: %v", test.exceptedQueries[i], query.Sql)
				}
			}
		})
	}
}

func BenchmarkSingleDQLStmtTokenRewriteMethodWithCache(b *testing.B) {
	queries := loadQueries(b, "rewrite_dql_queries.txt")
	for _, query := range queries {
		stmt, _ := sqlparser.Parse(query)

		b.Run("tokenRewrite-"+query, func(b *testing.B) {
			tableRoute := getTableRoute(stmt, []string{"t_user"})

			b.ResetTimer()
			b.ReportAllocs()

			_, _ = tableRoute.TableRouteParam.getTableQueries(tableRoute.Query, map[string]*querypb.BindVariable{}, map[string][]vindexes.ActualTable{
				"t_user": {
					{
						ActualTableName: "t_user_0",
						Index:           0,
					},
					{
						ActualTableName: "t_user_1",
						Index:           1,
					},
				},
			})
		})
	}
}

func BenchmarkSingleDMLStmtTokenRewriteMethodWithCache(b *testing.B) {
	queries := loadQueries(b, "rewrite_dml_queries.txt")
	for _, query := range queries {
		stmt, _ := sqlparser.Parse(query)
		tableDML := getTableDML(stmt, []string{"t_user"})

		b.Run("tokenRewrite-"+query, func(b *testing.B) {

			b.ResetTimer()
			b.ReportAllocs()

			_, _ = tableDML.TableRouteParam.getTableQueries(tableDML.AST, map[string]*querypb.BindVariable{}, map[string][]vindexes.ActualTable{
				"t_user": {
					{
						ActualTableName: "t_user_0",
						Index:           0,
					},
					{
						ActualTableName: "t_user_1",
						Index:           1,
					},
				},
			})
		})
	}
}

func BenchmarkBenchDQLStmtsTokenRewriteMethodWithCache(b *testing.B) {
	queries := loadQueries(b, "rewrite_dql_queries.txt")
	for _, query := range queries {
		stmt, _ := sqlparser.Parse(query)

		b.Run("tokenRewrite-"+query, func(b *testing.B) {
			tableRoute := getTableRoute(stmt, []string{"t_user"})

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < 100000; i++ {
				_, _ = tableRoute.TableRouteParam.getTableQueries(tableRoute.Query, map[string]*querypb.BindVariable{}, map[string][]vindexes.ActualTable{
					"t_user": {
						{
							ActualTableName: "t_user_0",
							Index:           0,
						},
						{
							ActualTableName: "t_user_1",
							Index:           1,
						},
					},
				})
			}
		})
	}
}

func BenchmarkBenchDMLStmtsTokenRewriteMethodWithCache(b *testing.B) {
	queries := loadQueries(b, "rewrite_dml_queries.txt")
	for _, query := range queries {
		stmt, _ := sqlparser.Parse(query)
		tableDML := getTableDML(stmt, []string{"t_user"})

		b.Run("tokenRewrite-"+query, func(b *testing.B) {

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < 100000; i++ {
				_, _ = tableDML.TableRouteParam.getTableQueries(tableDML.AST, map[string]*querypb.BindVariable{}, map[string][]vindexes.ActualTable{
					"t_user": {
						{
							ActualTableName: "t_user_0",
							Index:           0,
						},
						{
							ActualTableName: "t_user_1",
							Index:           1,
						},
					},
				})
			}
		})
	}
}

func getTableRoute(stmt sqlparser.Statement, logicalTables []string) TableRoute {
	vindex, _ := vindexes.CreateVindex("split_table_binaryhash", "split_table_binaryhash", nil)

	logicTableMap := make(map[string]*vindexes.LogicTableConfig, len(logicalTables))

	for _, logicTable := range logicalTables {
		logicTableMap[logicTable] = &vindexes.LogicTableConfig{
			LogicTableName: logicTable,
			ActualTableList: []vindexes.ActualTable{
				{
					ActualTableName: logicTable + "_0",
					Index:           0,
				},
				{
					ActualTableName: logicTable + "_1",
					Index:           1,
				},
			},
			TableCount:       2,
			TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}},
			TableVindex:      vindex,
		}
	}

	routingParameters := &RoutingParameters{
		Opcode: Scatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
	}

	return TableRoute{
		Query:           stmt,
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			TableOpcode: Scatter,
			LogicTable:  logicTableMap,
		},
	}
}

func getTableDML(stmt sqlparser.Statement, logicalTables []string) TableDML {
	vindex, _ := vindexes.CreateVindex("split_table_binaryhash", "split_table_binaryhash", nil)

	logicTableMap := make(map[string]*vindexes.LogicTableConfig, len(logicalTables))

	for _, logicTable := range logicalTables {
		logicTableMap[logicTable] = &vindexes.LogicTableConfig{
			LogicTableName: logicTable,
			ActualTableList: []vindexes.ActualTable{
				{
					ActualTableName: logicTable + "_0",
					Index:           0,
				},
				{
					ActualTableName: logicTable + "_1",
					Index:           1,
				},
			},
			TableCount:       2,
			TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}},
			TableVindex:      vindex,
		}
	}

	routingParameters := &RoutingParameters{
		Opcode: Scatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
	}

	return TableDML{
		TableNames:      logicalTables,
		AST:             stmt,
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			TableOpcode: Scatter,
			LogicTable:  logicTableMap,
			TableValues: []evalengine.Expr{
				evalengine.NewLiteralInt(1),
			},
		},
	}
}

func loadQueries(t testing.TB, filename string) (queries []string) {
	file, err := os.Open(path.Join("testdata", filename))
	require.NoError(t, err)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	read := file

	scanner := bufio.NewScanner(read)
	for scanner.Scan() {
		queries = append(queries, scanner.Text())
	}
	return queries
}
