package split_table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// table_postprocess_cases.json
func TestTablePostprocessCases(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1,  '6', 'aaa', 1, false, 1, 2, 3, 100,  200,  1 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (2,  '3', 'bbb', 2, false, 2, 3, 4, 103,  200,  1 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (3,  'a', 'ccc', 3, false, 3, 4, 5, 100,  200, 'a')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (4,  '5', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (5,  '6', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (6,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (7,  '2', 'bbb', 2, false, 2, 3, 4, 100,  300,  3 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (8,  '2', 'ccc', 3, false, 3, 4, 5, 102,  300,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (9,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (10, '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col2) values (11,  '2', '2', 2, false, 2, 3, 4, 100,  300,  3, 2)")

	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (1,  1, 2, 200, '1', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (2,  2, 4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (3,  3, 4, 200, '5', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (4,  4, 4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (5,  2, 2, 5,   '2', 5  , 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (6,  2, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (7,  2, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (8,  8, 5, 300, '4', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (9,  9, 3, 300, '5', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (10, 5, 3, 300, '4', 300, 5)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (1, 11, '42',  10, 1, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (2, 10, '42',  10, 2, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (3, 12, 'bbb', 10, 3, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (4, 13, 'bbb', 10, 2, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (5, 12, 'ccc', 10, 3, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (6, 11, '42',  10, 2, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (7, 10, '42',  10, 1, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (8, 12, 'bbb', 10, 1, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (9, 13, 'bbb', 10, 1, 302);")

	// ORDER BY, reference col from local table with shard key
	mcmp.ExecWithColumnCompare("select predef1 from t_user where id = 5 order by predef2")
	// ORDER BY, reference col from local table with shard key and split table key
	mcmp.ExecWithColumnCompare("select predef1 from t_user where id = 5 and col = 6 order by col")
	// ORDER BY on scatter
	mcmp.ExecWithColumnCompare("select col from t_user order by col")
	// ORDER BY unknown type on scatter
	mcmp.ExecWithColumnCompare("select id from t_user order by id")
	// ORDER BY column not in selectExprs
	mcmp.ExecWithColumnCompare("select col from t_user order by id")
	// ORDER BY uses column numbers
	mcmp.ExecWithColumnCompare("select col from t_user order by 1")
	// ORDER BY column numbers
	mcmp.ExecWithColumnCompare("select id as foo from t_user order by 1")
	// ORDER BY invalid col number on scatter
	mcmp.AssertContainsError("select col from t_user order by 2", "Unknown column '2' in 'order clause'")
	// ORDER BY NULL
	mcmp.ExecWithColumnCompare("select col from t_user order by null")
	// Order by, '*' expression
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by a")
	// Order by, qualified '*' expression
	mcmp.ExecWithColumnCompare("select t_user.* from t_user where id = 5 and col = 6 order by t_user.a")
	// Order by, '*' expression with qualified reference
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by t_user.a")
	// ORDER BY on select t.*
	mcmp.ExecWithColumnCompare("select t.*, t.col from t_user t order by t.col")
	// ORDER BY on select *
	mcmp.ExecWithColumnCompare("select *, col from t_user order by col")
	// ORDER BY on select multi t.*
	mcmp.ExecWithColumnCompare("select t.*, t.name, t.*, t.col from t_user t order by t.col")
	// ORDER BY on select multi *
	// MySQL不支持,但是Vitess支持了，因为VTGate把*展开了
	_, err := mcmp.ExecAndIgnore("select *, name, *, col from t_user order by col")
	require.NoError(t, err)
	// ORDER BY on scatter with multiple columns
	mcmp.ExecWithColumnCompare("select col from t_user order by a,b")
	// ORDER BY on scatter with multiple columns numbers
	mcmp.ExecWithColumnCompare("select id,col,a,b from t_user order by 2,3")
	// ORDER BY RAND()
	mcmp.ExecWithColumnCompare("select col from t_user order by RAND()")
	// Order by, qualified '*' expression, name mismatched.
	mcmp.AssertContainsError("select t_user.* from t_user where id = 5 order by e.col", "column 'e.col' not found (errno 1054)")
	// Order by, invalid column number
	// error parsing column number: 18446744073709551616 (errno 1105)
	_, err = mcmp.ExecAndIgnore("select col from t_user where id = 5 order by 18446744073709551616")
	require.ErrorContains(t, err, "error parsing column number: 18446744073709551616 (errno 1105)")
	// Order by, out of range column number
	mcmp.AssertContainsError("select col from t_user where id = 5 order by 2", "Unknown column '2' in 'order clause'")
	// Order by with math functions
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by -col1")
	// Order by with string operations
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by concat(col,col1) collate utf8mb4_general_ci desc")
	// Order by with math operations
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by id+col collate utf8mb4_general_ci desc")
	// routing rules: order by gets pushed for routes
	mcmp.ExecWithColumnCompare("select col from t_user where id = 1  and col = 6 order by col")
	// order by column alias
	mcmp.ExecWithColumnCompare("select col as foo from t_user order by foo")
	// column alias for a table column in order by
	mcmp.ExecWithColumnCompare("select col as foo,col2 as col from t_user order by col")
	// order by with ambiguous column reference ; valid in MySQL
	mcmp.ExecWithColumnCompare("select col, col from t_user order by col")
	// Order by uses cross-shard expression
	mcmp.ExecWithColumnCompare("select col from t_user order by col+1")
	// Order by column number with collate
	mcmp.ExecWithColumnCompare("select t_user.col1 as a from t_user order by 1 collate utf8mb4_general_ci")
	// aggregation and non-aggregations column without group by
	// results mismatched.
	//        Vitess Results:
	//        [INT64(10) NULL]
	//        MySQL Results:
	//        [INT64(10) INT32(100)]
	// 返回结果不稳定，intcol是随机的一行
	_, err = mcmp.ExecAndIgnore("select count(id), intcol from t_user")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select count(id) from t_user")
	// aggregation and non-aggregations column with order by
	//results mismatched.
	//        Vitess Results:
	//        [INT64(10) NULL]
	//        MySQL Results:
	//        [INT64(10) INT32(100)]
	// 返回结果不稳定，intcol是随机的一行
	_, err = mcmp.ExecAndIgnore("select count(id), intcol from t_user order by 2")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select count(id) from t_user order by 1")
	// min column
	// results mismatched.
	//        Vitess Results:
	//        [INT64(1) CHAR("3")]
	//        MySQL Results:
	//        [INT64(1) CHAR("6")]
	// 返回结果不稳定，col是随机的一行
	_, err = mcmp.ExecAndIgnore("select min(id),col from t_user")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select min(id) from t_user")
	// max column
	//results mismatched.
	//        Vitess Results:
	//        [INT64(10) CHAR("3")]
	//        MySQL Results:
	//        [INT64(10) CHAR("6")]
	// 返回结果不稳定，col是随机的一行
	_, err = mcmp.ExecAndIgnore("select max(id),col from t_user")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select max(id) from t_user")
	// select multi aggregator func columns
	// results mismatched.
	//         Vitess Results:
	//        [INT64(100865) INT64(6) INT64(15) CHAR("a")]
	//        MySQL Results:
	//        [INT64(100865) INT64(6) INT64(15) CHAR("2")]
	// 返回结果不稳定，col是随机的一行
	_, err = mcmp.ExecAndIgnore("select max(id),min(id),count(*),col from t_user")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select max(id),min(id),count(*) from t_user")
	// aggregator func columns with single shard
	mcmp.ExecWithColumnCompare("select count(*) from t_user where id=5")
	// aggregator func columns with single shard and single table
	mcmp.ExecWithColumnCompare("select count(*) from t_user where id=5 and col=6")
	// aggregation and non-aggregations column with group by
	mcmp.ExecWithColumnCompare("select count(id), intcol from t_user group by 2")
	// aggregation and non-aggregations column with group by and order by
	mcmp.ExecWithColumnCompare("select count(id), intcol from t_user group by 2 order by 1")
	// Scatter order by and aggregation: order by column must reference column from select list
	mcmp.ExecWithColumnCompare("select col, count(*) from t_user group by col order by c")
	// ORDER BY NULL for join
	mcmp.ExecWithColumnCompare("select t_user.col1 as a, t_user.col2, t_music.foo from t_user join t_music on t_user.id = t_music.id where t_user.id = 1 order by null")
	// ORDER BY non-key column for join
	mcmp.ExecWithColumnCompare("select t_user.col1 as a, t_user.col2, t_music.foo from t_user join t_music on t_user.id = t_music.id where t_user.id = 1 order by a")
	// ORDER BY non-key column for implicit join
	mcmp.ExecWithColumnCompare("select t_user.col1 as a, t_user.col2, t_music.foo from t_user, t_music where t_user.id = t_music.id and t_user.id = 1 order by a")
	// ORDER BY RAND() for join
	mcmp.ExecWithColumnCompare("select t_user.col1 as a, t_user.col2, t_music.foo from t_user join t_music on t_user.id = t_music.id where t_user.id = 1 order by RAND()")
	// limit for joins. Can't push down the limit because result # counts get multiplied by join operations.
	// results mismatched.
	//        Vitess Results:
	//        [CHAR("3")]
	//        MySQL Results:
	//        [CHAR("6")]
	// 返回结果不稳定，随机返回了一行
	_, err = mcmp.ExecAndIgnore("select t_user.col from t_user join t_user_extra limit 1")
	require.NoError(t, err)
	// ordering on the left side of the join
	mcmp.ExecWithColumnCompare("select name from t_user, t_music order by name")
	// join order by with ambiguous column reference ; valid in MySQL
	mcmp.ExecWithColumnCompare("select name, name from t_user, t_music order by name")
	// Order by column number with coalesce with columns from both sides
	mcmp.AssertContainsError("select id from t_user, t_user_extra order by coalesce(t_user.col, t_user_extra.col)", "Column 'id' in field list is ambiguous")
	mcmp.ExecWithColumnCompare("select t_user.id from t_user, t_user_extra order by coalesce(t_user.col, t_user_extra.col)")
	// ORDER BY column offset
	mcmp.ExecWithColumnCompareAndNotEmpty("select id as foo from t_music order by 1")
	// ORDER BY after pull-out subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where col in (select f_key from t_user) order by col")
	// scatter limit after pullout subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where col in (select f_key from t_user) order by col limit 1")
	// syntax to use near '+1'
	// arithmetic limit
	// mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user order by id limit 1+1")

	// having multi cols
	mcmp.ExecWithColumnCompareAndNotEmpty("select col,foo from t_music where user_id=11 having col='42' and foo=302")
	// join and having
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col2 from t_user join t_user_extra having t_user.col2 = 2")
	// HAVING uses subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user having id in (select col from t_user) order by id")
}
