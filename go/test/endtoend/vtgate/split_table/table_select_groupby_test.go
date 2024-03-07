package split_table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableGroupBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c) values (1, 'a', 'aaa', 1, false,1,2,3),(2, 'b', 'bbb', 2, false,2,3,4),(3, 'c', 'ccc', 3, false,3,4,5)")
	mcmp.Exec("insert into t_3(f_shard,f_table) values (1,2),(1,2),(2,3)")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id, count(*) k from t_user group by id")

	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user group by col")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col, count(*) k from t_user group by id,col")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id,f_key, count(*) k from t_user group by id,f_key")

	mcmp.ExecWithColumnCompareAndNotEmpty("select col,f_key, count(*) k from t_user group by col,f_key")

	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, count(*) from t_user group by a, b")

	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, count(*) from t_user group by 2, 1")

	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, count(*) from t_user group by b, a")

	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user group by 1")

	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, count(*) from t_user group by 1, 2, 3 order by 1, 2, 3")

	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, count(*) from t_user group by 1, 2, 3 order by a, b, c")

	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, count(*) from t_user group by 3, 2, 1 order by  b, a, c")

	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, count(*) from t_user group by 3, 2, 1 order by 1 desc, 3 desc, b")

	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1 as a from t_user where t_user.id = 3 group by a collate utf8_general_ci")

	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) k from t_user group by col order by null, k")

	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) k from t_user group by col order by null")

	mcmp.ExecWithColumnCompareAndNotEmpty("select lower(textcol2) as v, count(*) from t_user group by v")

	mcmp.ExecWithColumnCompareAndNotEmpty("select textcol2, count(*) from t_user group by a ")

	//结果不一样
	//mcmp.ExecWithColumnCompareAndNotEmpty("select ascii(textcol2) as a, count(*) from t_user group by a")

	mcmp.ExecWithColumnCompareAndNotEmpty("select group_concat(f_key order by col asc), id from t_user group by id, col")
	mcmp.ExecWithColumnCompareAndNotEmpty("select f_table, count(*) from t_3 group by f_table order by f_table+1")
	//结果不一样待修复
	//mcmp.ExecWithColumnCompareAndNotEmpty("select id,f_table from t_3 group by id,f_table order by id+1,f_table+1")

	mcmp.ExecWithColumnCompareAndNotEmpty("select a from t_user group by a+1")

	mcmp.ExecWithColumnCompareAndNotEmpty("select col, a, id from t_user group by col, a, id, id, a, col")

	mcmp.ExecWithColumnCompareAndNotEmpty("select f_table, f_table, count(*) from t_3 group by f_table")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by id,col")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by id")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by b, id")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by id, c")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user where id = 1 group by c")

	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user where id = 1 group by col")

	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.intcol FROM t_user GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")

	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.intcol FROM t_user where id = 1 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")

	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.intcol FROM t_user where col = 'a' GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")

	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.intcol FROM t_user where col = 'a' and id = 1 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")

	//VT03005: cannot group on 'sum(id)' (errno 1056) (sqlstate 42000) during query: select sum(id) as id,col from t_user group by b, id
	//mcmp.ExecWithColumnCompareAndNotEmpty("select sum(id) as id,col from t_user group by b, id")
	//   VT03005: cannot group on 'count(id)' (errno 1056) (sqlstate 42000) during query: select count(id) as id,col from t_user group by b, id
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(id) as id,col from t_user group by b, id")

	//VT03005: cannot group on 'max(id)' (errno 1056) (sqlstate 42000) during query: select max(id) as id,col from t_user group by b, id
	//mcmp.ExecWithColumnCompareAndNotEmpty("select max(id) as id,col from t_user group by b, id")
	//   VT03005: cannot group on 'min(id)' (errno 1056) (sqlstate 42000) during query: select min(id) as id,col from t_user group by b, id
	//mcmp.ExecWithColumnCompareAndNotEmpty("select min(id) as id,col from t_user group by b, id")

	//mcmp.ExecWithColumnCompareAndNotEmpty("select avg(id) as id,col from t_user group by b, id")

}

// table_aggr_cases.json
func TestTableAggrCases(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo) values (18, '405', 'aaa', 2, true,2,4,5,120,240)")
	// scatter aggregate with non-aggregate expressions.
	// Vitess Results:
	// [NULL INT64(1)]
	// MySQL Results:
	// [INT64(18) INT64(1)]
	// 分片结果返回的顺序问题
	_, err := mcmp.ExecAndIgnore("select id, count(*) from t_user")
	require.NoError(t, err)

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (1,  '3',    'aaa', 1, false, 1,  2, 3, 100, 200, 'abc', 'aaa', 'axa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (2,  '3',    'bbb', 2, false, 2,  3, 4, 103, 200, 'abc', 'aaa', 'axa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (3,  'abc',  'ccc', 3, true,  3,  4, 5, 100, 200, 'abc', 'bbb', 'bxb')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (4,  'abc',  'ccc', 3, true,  13, 4, 5, 100, 200, 'abc', 'bbb', 'bxb')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (5,  '12',   'ccc', 3, true,  13, 4, 5, 103, 200, 'abc', 'bbb', 'bxb')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (6,  '2',    'aaa', 1, true,  10, 2, 3, 100, 300, 2    , 'bbb', 'bxb')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (7,  '1024', 'bbb', 2, false, 20, 3, 4, 100, 300, 3    , 'bbb', 'bxb')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (8,  '1024', 'ccc', 3, false, 30, 4, 5, 102, 300, 4    , 'bbb', 'bxb')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (9,  '1024', 'aaa', 1, false, 14, 2, 3, 100, 300, 2    , 'bbb', 'bxb')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (10, '1024', 'aaa', 1, false, 15, 2, 3, 100, 300, 2    , 'bbb', 'bxb')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (11, '12',   'aaa', 1, true,  6,  2, 3, 100, 300, 2    , 'ccc', 'cxc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (12, '1024', 'aaa', 1, false, 2,  2, 3, 100, 300, 2    , 'ccc', 'cxc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (13, '1024', 'aaa', 1, false, 3,  2, 3, 100, 300, 2    , 'ccc', 'cxc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (14, '123',  'aaa', 1, false, 2,  2, 3, 100, 300, 'abc', 'ccc', 'cxc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (15, '1024', 'aaa', 1, false, 2,  2, 3, 100, 300, 2    , 'ccc', 'cxc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (123,   '1', 'aaa', 1, false, 2,  2, 3, 100, 300, 2    , 'ccc', 'cxc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (1019, '45', 'aaa', 1, false, 1,  2, 3, 100, 300, 2    , 'ccc', 'cxc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (1020, '45', 'aaa', 1, false, 2,  2, 3, 100, 300, 2    , 'aaa', 'axa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (1021, '45', 'aaa', 1, false, 3,  2, 3, 100, 300, 2    , 'aaa', 'axa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (1320, '45', 'aaa', 1, false, 2,  2, 3, 100, 300, 2    , 'aaa', 'axa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (1536, '45', 'aaa', 1, false, 2,  2, 3, 100, 300, 2    , 'aaa', 'axa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, val1, val2) values (1024, '45', 'aaa', 1, false, 1,  2, 3, 100, 300, 2    , 'aaa', 'axa')")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (100, 101, 101, 45, '45', 200),(200, 102, 102, 1024, '45', 200),(300, 103, 103, 1024, 'bbb', 200),(400, 104, 104, 3, '1024', 200),(500, 105, 105, 3, 'ada', 300)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (130, 101, 101, 45, '45', 200),(250, 102, 102, 1024, '1024', 200),(370, 103, 103, 1024, 'ccc', 300),(489, 104, 104, 3, '12', 300),(520, 105, 105, 3, 'axa', 300)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (131, 1024, 101, 45, '45', 200),(251, 102, 102, 1024, '1024', 200),(371, 103, 103, 1024, 'ccc', 300),(490, 104, 104, 3, '12', 300),(521, 105, 105, 3, 'axa', 300)")

	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (101, 11, 'aaa', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (121, 10, 'aaa', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (131, 12, 'bbb', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (141, 13, 'bbb', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (161, 12, 'ccc', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (1010, 11, 'aaa', 10, 300)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (1213, 10, 'aaa', 10, 300)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (1314, 12, 'bbb', 10, 300)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (1415, 13, 'bbb', 10, 300)")

	// scatter aggregate order by null
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user order by null")
	// scatter aggregate symtab lookup error
	mcmp.AssertContainsError("select id, b as id, count(*) from t_user order by id", "Column 'id' in field list is ambiguous")
	// scatter aggregate group by select col
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user group by col")
	// scatter aggregate multiple group by (columns)
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, count(*) from t_user group by a, b")
	// scatter aggregate multiple group by (numbers)
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, count(*) from t_user group by 2, 1")
	// scatter aggregate group by aggregate function
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) b from t_user group by b")
	// scatter aggregate multiple group by columns inverse order
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, count(*) from t_user group by b, a")
	// scatter aggregate group by column number
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user group by 1")
	// scatter aggregate group by invalid column number
	mcmp.AssertContainsError("select col from t_user group by 2", "Unknown column '2' in 'group clause'")
	// scatter aggregate with numbered order by columns
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, d, count(*) from t_user group by 1, 2, 3 order by 1, 2, 3")
	// scatter aggregate with named order by columns
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, d, count(*) from t_user group by 1, 2, 3 order by a, b, c")
	// scatter aggregate with jumbled order by columns
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, d, count(*) from t_user group by 1, 2, 3, 4 order by d, b, a, c")
	// scatter aggregate with jumbled group by and order by columns
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, d, count(*) from t_user group by 3, 2, 1, 4 order by d, b, a, c")
	// scatter aggregate with some descending order by cols
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, b, c, count(*) from t_user group by 3, 2, 1 order by 1 desc, 3 desc, b")
	// invalid order by column numner for scatter
	mcmp.AssertContainsError("select col, count(*) from t_user group by col order by 5 limit 10", "Unknown column '5' in 'order clause'")
	// aggregate with limit
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) from t_user group by col limit 10")
	// Group by with collate operator
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1 as a from t_user where t_user.id = 5 group by a collate utf8_general_ci")
	// Group by invalid column number (code is duplicated from symab).
	// results mismatched.
	//        Vitess Results:
	//        [INT64(8)]
	//        MySQL Results:
	//        [INT64(1)]
	//mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user group by 1.1")
	// Group by out of range column number (code is duplicated from symab).
	mcmp.AssertContainsError("select id from t_user group by 2", "Unknown column '2' in 'group clause'")
	// aggregate query with order by aggregate column along with NULL
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) k from t_user group by col order by null, k")
	// aggregate query with order by NULL
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) k from t_user group by col order by null")
	// weight_string addition to group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select lower(f_key) as v, count(*) from t_user group by v")
	// weight_string addition to group by when also there in order by
	mcmp.ExecWithColumnCompareAndNotEmpty("select char_length(f_key) as a123, count(*) from t_user group by a order by a123")
	// group by column alias
	mcmp.ExecWithColumnCompareAndNotEmpty("select ascii(f_key) as a123, count(*) from t_user group by a123")
	// group_concat on single shards
	// expected: []string{"group_concat(f_int order by name)", "id"}
	// actual  : []string{"group_concat(f_int order by `name` asc)", "id"}
	// column names do not match - the expected values are what mysql produced
	// mcmp.ExecWithColumnCompareAndNotEmpty("select group_concat(f_int order by name), id from t_user group by id, col")
	mcmp.Exec("select group_concat(f_int order by name), id from t_user group by id, col")
	mcmp.ExecWithColumnCompareAndNotEmpty("select group_concat(f_int order by `name` asc), id from t_user group by id, col")
	// Scatter order by is complex with aggregates in select
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) from t_user group by col order by col+1")
	// scatter aggregate complex order by
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by id,col order by id+1,col+1")
	// select expression does not directly depend on grouping expression
	mcmp.ExecWithColumnCompareAndNotEmpty("select a from t_user group by a+1")
	// redundant group by columns are not added
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, predef1, id from t_user group by col, predef1, id, id, predef1, col")
	// using a grouping column multiple times should be OK
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, col, count(*) from t_user group by col")
	// use vindex and table index group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by id,col")
	// use unique vindex group by, split table plan generate Aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by id")
	// use unique vindex group by, split table plan generate Aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by b, id")
	// use unique vindex group by, split table plan generate Aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user group by id, c")
	// EqualUnique Select, split table plan generate Aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user where id = 1024 group by c")
	// EqualUnique Select, use table index group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user where id = 1024 group by col")
	// EqualUnique Select, use table index group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col from t_user where id = 1024 group by c, col")
	// Group By X Order By X
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.intcol FROM t_user GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")
	// Group By X Order By X, vindex EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.intcol FROM t_user where id = 1536 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")
	// Group By X Order By X, table index EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.intcol FROM t_user where col = 45 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")
	// Group By X Order By X, vindex EqualUnique, table index EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.intcol FROM t_user where col = 45 and id = 1320 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")
	// count(*) spread across join
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// sum spread across join
	mcmp.ExecWithColumnCompareAndNotEmpty("select sum(t_user.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// count spread across join
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(t_user.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// max spread across join
	// expected: []string{"max(t_user.col)"}
	// actual  : []string{"max(t_user_0.col)"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompareAndNotEmpty("select max(t_user.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	mcmp.Exec("select max(t_user.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// min spread across join RHS
	// expected: []string{"min(t_user_extra.col)"}
	// actual  : []string{"min(t_user_extra_0.col)"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompareAndNotEmpty("select min(t_user_extra.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	mcmp.Exec("select min(t_user_extra.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// Grouping on join
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.a from t_user join t_user_extra group by t_user.a")
	// Aggregates and joins
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user join t_user_extra")
	// Aggregate on join
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.a, count(*) from t_user join t_user_extra group by t_user.a")
	// Aggregate on other table in join
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.a, count(t_user_extra.extra_id) from t_user join t_user_extra group by t_user.a")
	// group by and ',' joins with condition
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col from t_user join t_user_extra on t_user_extra.col = t_user.col group by t_user.id")
	// Aggregation on column from inner side in a left join query
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(u.id) from t_user u left join t_user_extra ue on u.col = ue.col")
	// Aggregation on outer side in a left join query
	// Vitess Results:
	// [NULL]
	// MySQL Results:
	// [INT64(0)]
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(ue.id) from t_user u left join t_user_extra ue on u.col = ue.col")
	// inner join with scalar aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user join t_music on t_user.foo = t_music.bar")
	// left outer join with scalar aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user left join t_music on t_user.foo = t_music.bar")
	// inner join with left grouping
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user left join t_music on t_user.foo = t_music.bar group by t_user.col")
	// inner join with right grouping
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user left join t_music on t_user.foo = t_music.bar group by t_music.col")
	// left outer join with left grouping
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user left join t_music on t_user.foo = t_music.bar group by t_user.col")
	// left outer join with right grouping
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user left join t_music on t_user.foo = t_music.bar group by t_music.col")
	// 3 table inner join with scalar aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user join t_music on t_user.foo = t_music.bar join t_user_extra on t_user.foo = t_user_extra.baz")
	// 3 table with mixed join with scalar aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user left join t_music on t_user.foo = t_music.bar join t_user_extra on t_user.foo = t_user_extra.baz")
	// ordering have less column than grouping columns, grouping gets rearranged as order by and missing columns gets added to ordering
	mcmp.ExecWithColumnCompareAndNotEmpty("select u.col, u.intcol, count(*) from t_user u join t_music group by 1,2 order by 2")
	// multiple count star and a count with 3 table join
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*), count(*), count(u.col) from t_user u, t_user u2, t_user_extra ue")
	// interleaving grouping, aggregation and join with min, max columns
	// expected: []string{"col", "min(t_user_extra.baz)", "f_key", "max(t_user_extra.bar)"}
	// actual  : []string{"col", "min(t_user_extra_0.baz)", "f_key", "max(t_user_extra_0.bar)"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col, min(t_user_extra.baz), t_user.f_key, max(t_user_extra.bar) from t_user join t_user_extra on t_user.col = t_user_extra.bar group by t_user.col, t_user.f_key")
	mcmp.ExecAndNotEmpty("select t_user.col, min(t_user_extra.baz), t_user.f_key, max(t_user_extra.bar) from t_user join t_user_extra on t_user.col = t_user_extra.bar group by t_user.col, t_user.f_key")
	// count with distinct no unique vindex
	mcmp.ExecWithColumnCompareAndNotEmpty("select col1, count(distinct col2) from t_user group by col1")
	// count with distinct no unique vindex and no group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(distinct col2) from t_user")
	// count with distinct no unique vindex, count expression aliased
	mcmp.ExecWithColumnCompareAndNotEmpty("select col1, count(distinct col2) c2 from t_user group by col1")
	// sum with distinct no unique vindex
	mcmp.ExecWithColumnCompareAndNotEmpty("select col1, sum(distinct col2) from t_user group by col1")
	// min with distinct no unique vindex. distinct is ignored.
	mcmp.ExecWithColumnCompareAndNotEmpty("select col1, min(distinct col2) from t_user group by col1")
	// order by count distinct
	mcmp.ExecWithColumnCompareAndNotEmpty("select col1, count(distinct col2) k from t_user group by col1 order by k")
	// distinct and aggregate functions missing group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct a, count(*) from t_user")
	// distinct and aggregate functions
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct a, count(*) from t_user group by a")
	// do not use distinct when using only aggregates and no group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct count(*) from t_user")
	// multiple distinct functions with grouping.
	mcmp.ExecWithColumnCompareAndNotEmpty("select col1, count(distinct col2), sum(distinct col2) from t_user group by col1")
	// distinct on text column with collation
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(distinct textcol1) from t_user group by col")
	// we have to track the order of distinct aggregation expressions
	mcmp.ExecWithColumnCompareAndNotEmpty("select val2, count(distinct val1), count(*) from t_user group by val2")
	// multiple distinct aggregations on the same column is allowed
	mcmp.ExecWithColumnCompareAndNotEmpty("select val1, count(distinct val2), sum(distinct val2) from t_user group by val1")
	// multiple distinct aggregations on the same column in different positions
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(distinct val2), val1, count(*), sum(distinct val2) from t_user group by val1")
	// scalar aggregates with min, max, sum distinct and count distinct using collations
	mcmp.ExecWithColumnCompareAndNotEmpty("select min(textcol1), max(textcol2), sum(distinct textcol1), count(distinct textcol1) from t_user")
	// grouping aggregates with mi, max, sum distinct and count distinct using collations
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, min(textcol1), max(textcol2), sum(distinct textcol1), count(distinct textcol1) from t_user group by col")
	// Column and Literal equality filter on scatter aggregates
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) ac from t_user having ac = 10") Should NOT be empty, but was []
	// Equality filtering with column and string literal on scatter aggregates
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) ac from t_user having ac = '1'") Should NOT be empty, but was []
	// Column and Literal not equal filter on scatter aggregates
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) ac from t_user having ac != 10")
	// Not equal filter with column and string literal on scatter aggregates
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) ac from t_user having ac != '1'")
	// Greater than filter on scatter aggregates
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) ac from t_user having ac > 10")
	// Less than filter on scatter aggregates
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) ac from t_user having ac < 10") Should NOT be empty, but was []
	// Less Equal filter on scatter aggregates
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) ac from t_user having ac <= 10") Should NOT be empty, but was []
	// Less Equal filter on scatter with grouping
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) ac from t_user group by col having ac <= 10")
	// We should be able to find grouping keys on ordered aggregates
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) as ac, val1 from t_user group by val1 having ac = 1.00")
	// aggregation filtering by having on a route with no group by with non-unique vindex filter
	// MySQL不支持,报错Unknown column 'name' in 'having clause'  但是Vitess支持了
	_, err = mcmp.ExecAndIgnore("select 1 from t_user having count(id) = 10 and name = 'a'")
	require.NoError(t, err)
	//mcmp.ExecWithColumnCompareAndNotEmpty("select 1,name from t_user having count(id) = 21 and name = 'abc'")
	//mcmp.ExecAndNotEmpty("select 1,name from t_user having count(id) = 21 and name = 'abc'") Should NOT be empty, but was []
	// aggregation filtering by having on a route with no group by
	// expected: []string{"1"}
	// actual  : []string{":vtg1 /* INT64 */"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompareAndNotEmpty("select 1 from t_user having count(id) = 22")
	//mcmp.ExecAndNotEmpty("select 1 from t_user having count(id) = 22") Should NOT be empty, but was []
	// find aggregation expression and use column offset in filter
	mcmp.ExecWithColumnCompareAndNotEmpty("select foo, count(*) from t_user group by foo having count(*) = 5")
	// find aggregation expression and use column offset in filter times two
	//mcmp.ExecWithColumnCompareAndNotEmpty("select foo, sum(foo), sum(b) from t_user group by foo having sum(foo)+sum(b) = 42") Should NOT be empty, but was []
	// find aggregation expression and use column offset in filter times three
	//mcmp.ExecWithColumnCompareAndNotEmpty("select foo, sum(foo) as fooSum, sum(b) as barSum from t_user group by foo having fooSum+sum(b) = 42") Should NOT be empty, but was []
	// having should be able to add new aggregation expressions in having
	mcmp.ExecWithColumnCompareAndNotEmpty("select foo from t_user group by foo having count(*) = 5")
	// Can't inline derived table when it has HAVING with aggregation function
	//mcmp.ExecWithColumnCompareAndNotEmpty("select * from (select id from t_user having count(*) = 22) s") Should NOT be empty, but was []
	// order by inside derived tables can be ignored
	mcmp.ExecAndNotEmpty("select col from (select t_user.col, t_user_extra.extra_id from t_user join t_user_extra on t_user.id = t_user_extra.user_id order by t_user_extra.extra_id) a")
	// when pushing predicates into derived tables, make sure to put them in HAVING when they contain aggregations
	//_, err = mcmp.ExecAndIgnore("select t1.portalId, t1.flowId from (select portalId, flowId, count(*) as count from t_user_extra where localDate > :v1 group by user_id, flowId order by null) as t1 where count >= :v2")
	//require.ErrorContains(t, err, "VT12001: unsupported: unable to use: *sqlparser.DerivedTable in split table")
	// Cannot have more than one aggr(distinct...
	_, err = mcmp.ExecAndIgnore("select count(distinct a), count(distinct b) from t_user")
	require.ErrorContains(t, err, "VT12001: unsupported: only one DISTINCT aggregation is allowed in a SELECT: count(distinct b)")
	// using two distinct columns - min with distinct vindex, sum with distinct without vindex
	mcmp.ExecWithColumnCompareAndNotEmpty("select col1, min(distinct id), sum(distinct c) from t_user group by col1")
	// select count(distinct user_id, name) from user
	_, err = mcmp.ExecAndIgnore("select count(distinct a, name) from t_user")
	require.ErrorContains(t, err, "VT03001: aggregate functions take a single argument 'count(distinct a, `name`)'")
	// count with distinct group by unique vindex
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, count(distinct col) from t_user group by id")
	// count with distinct unique vindex
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(distinct id) from t_user group by col")
	// scatter aggregate using distinctdistinct
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct col from t_user")
	// scatter aggregate using distinct,remove group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct col from t_user where id=13 group by col")
	// scatter aggregate using distinct,,remove group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct col from t_user where id=13 group by name")
	// special shard using distinct-count
	// expected: []string{"(count(col))"}
	// actual  : []string{"count(col)"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompareAndNotEmpty("select distinct(count(col)) from t_user where id=123")
	mcmp.ExecAndNotEmpty("select distinct(count(col)) from t_user where id=123")
	// scatter aggregate in a subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select a from (select count(*) as a from t_user) t")
	// order by inside and outside parenthesis select
	mcmp.ExecWithColumnCompareAndNotEmpty("(select id from t_user order by 1 desc) order by 1 asc limit 2")
	// count non-null columns incoming from outer joins should work well
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(col) from (select t_user_extra.col as col from t_user left join t_user_extra on t_user.id = t_user_extra.id limit 10) as x")
	// Aggregations from derived table used in arithmetic outside derived table
	mcmp.ExecWithColumnCompareAndNotEmpty("select A.a, A.b, (A.a / A.b) as d from (select sum(a) as a, sum(b) as b from t_user) A")
	// having max()
	//mcmp.ExecWithColumnCompareAndNotEmpty("select col1, count(*) c from t_user where id=123 group by f_key having max(col1) > 10;") Should NOT be empty, but was []
	// having max()
	//mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) c from t_user where id=123 group by f_key having max(col) > 10;") Should NOT be empty, but was []
	// group by a unique vindex should revert to simple route, and having clause should find the correct symbols.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, count(*) c from t_user group by id having max(col) > 10")
	// aggregation filtering by having on a route
	//mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user group by id having count(id) = 22") Should NOT be empty, but was []
	// Column and Literal equality filter on scatter aggregates
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) a from t_user where id = 123 having a = 10") Should NOT be empty, but was []
	// Equality filtering with column and string literal on scatter aggregates
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) a from t_user where id = 123 having a = '1'")
	// Column and Literal not equal filter on scatter aggregates
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) a from t_user where id = 123 having a != 10")
	// Not equal filter with column and string literal on scatter aggregates
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) a from t_user where id = 123 having a != '1'") Should NOT be empty, but was []
	// Greater than filter on scatter aggregates
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) a from t_user where id=123 having a > 10") Should NOT be empty, but was []
	// Greater Equal filter on scatter aggregates
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) a from t_user where id = 123 having a >= 10") Should NOT be empty, but was []
	// Less Equal filter on scatter aggregates
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) a from t_user where id = 123 having a <= 10")
	// aggregation filtering by having on a route with no group by with non-unique vindex filter
	// MySQL不支持,报错Unknown column 'name' in 'having clause'  但是Vitess支持了
	_, err = mcmp.ExecAndIgnore("select 1 from t_user where id = 123 having count(id) = 10 and name = 'a'")
	require.NoError(t, err)
	//mcmp.ExecAndNotEmpty("select 1,name from t_user where id = 123 having count(id) = 10 and name = 'a'") Should NOT be empty, but was []
	// find aggregation expression and use column offset in filter
	//mcmp.ExecWithColumnCompareAndNotEmpty("select foo, count(*) from t_user where id = 123 group by foo having count(*) = 3") Should NOT be empty, but was []
	// find aggregation expression and use column offset in filter times two
	//mcmp.ExecWithColumnCompareAndNotEmpty("select foo, sum(foo), sum(b) from t_user where id = 123 group by foo having sum(foo)+sum(b) = 42") Should NOT be empty, but was []
	// find aggregation expression and use column offset in filter times three
	//mcmp.ExecWithColumnCompareAndNotEmpty("select foo, sum(foo) as fooSum, sum(b) as barSum from t_user where id=123 group by foo having fooSum+sum(b) = 42") Should NOT be empty, but was []
	// having should be able to add new aggregation expressions in having
	//mcmp.ExecWithColumnCompareAndNotEmpty("select foo from t_user where id=123 group by foo having count(*) = 3") Should NOT be empty, but was []
	// Can't inline derived table when it has HAVING with aggregation function
	//mcmp.ExecWithColumnCompareAndNotEmpty("select * from (select id from t_user having count(*) = 1) s") Should NOT be empty, but was []
	// distinct aggregation will 3 table join query
	//mcmp.ExecWithColumnCompareAndNotEmpty("select u.textcol1, count(distinct u.val2) from t_user u join t_user u2 on u.val2 = u2.id join t_music m on u2.val2 = m.id group by u.textcol1") Should NOT be empty, but was []
	// scatter aggregate using distinctdistinct
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct col from t_user")
	// optimize group by when using distinct with no aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct col1, col2 from t_user group by col1, col2")
	// scatter aggregate with ambiguous aliases
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct a, b as a from t_user")
	// scatter aggregate with complex select list (can't build order by)
	// expected: []string{"a+1"}
	// actual  : []string{"a + 1"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompareAndNotEmpty("select distinct a+1 from t_user")
	mcmp.ExecAndNotEmpty("select distinct a+1 from t_user")
	// count distinct and sum distinct on join query pushed down - unique vindex
	// results mismatched.
	//        Vitess Results:
	//        [NULL INT64(16) DECIMAL(184)]
	//        Vitess RowsAffected: 0
	//        MySQL Results:
	//        [NULL INT64(4) DECIMAL(46)]
	//        MySQL RowsAffected: 0
	//mcmp.ExecWithColumnCompareAndNotEmpty("select u.col1, count(distinct m.user_id), sum(distinct m.user_id) from t_user u join t_music m group by u.col1")
	// count with distinct tindex with EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, count(distinct col) from t_user where id=123")
	// count with distinct with EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, count(distinct intcol) from t_user where id=123")
	// count with distinct vindex with EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, count(distinct id) from t_user where id=123")
	// count with distinct with EqualUnique-EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, count(distinct intcol) from t_user where id = 123 and col = 1")
	// sum with distinct tindex with EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, sum(distinct col) from t_user where id=123")
	// sum with distinct with EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, sum(distinct intcol) from t_user where id=123")
	// sum with distinct with EqualUnique-EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, sum(distinct intcol) from t_user where id = 123 and col = 1")
	// sum with distinct vindex with EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, sum(distinct id) from t_user where id=123")
	// using two distinct columns, min & max with EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, min(distinct intcol), max(distinct intcol) from t_user where id=123")
	// using two distinct columns, min & max with EqualUnique-EqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id, min(distinct intcol), max(distinct intcol) from t_user where id = 123 and col = 1")
}
