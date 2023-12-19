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

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (1,  '6', 'aaa', 1, false, 1, 2, 3, 100,  200,  1 ,'A1B', 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (2,  '3', 'bbb', 2, false, 2, 3, 4, 103,  200,  1 ,'A1B', 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (3,  'a', 'ccc', 3, false, 3, 4, 5, 100,  200, 'a','AB' , 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (4,  '5', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 ,'A1B', 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (5,  '6', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 ,'A1B', 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (6,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 ,42   , 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (7,  '2', 'bbb', 2, false, 2, 3, 4, 100,  300,  3 ,42   , 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (8,  '2', 'ccc', 3, false, 3, 4, 5, 102,  300,  4 ,42   , 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (9,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 ,42   , 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (10, '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 ,42   , 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (11, '2', '2',   2, false, 2, 3, 4, 100,  300,  3, 2    , 4)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, col1, col2) values (12, '2', '2',   2, false, 2, 3, 4, 100,  300,  3, 2    , 2)")

	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (1,  1, 2, 200, '1', 200, 5, 10)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (2,  2, 4, 200, '3', 200, 5, 10)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (3,  3, 4, 200, '5', 200, 5, 10)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (4,  4, 4, 200, '3', 200, 5, 20)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (5,  2, 2, 5,   '2', 5  , 5, 20)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (6,  2, 3, 300, '2', 200, 5, 20)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (7,  2, 3, 300, '2', 200, 5, 20)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (8,  8, 5, 300, '4', 300, 5, 10)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (9,  9, 3, 300, '5', 300, 5, 10)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo, col1) VALUES (10, 5, 3, 300, '4', 300, 5, 10)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (1,  11, '42',  10, 1, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (2,  10, '42',  10, 2, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (3,  12, 'AB', 10, 3, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (4,  13, 'bbb', 10, 2, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (5,  12, 'A1B', 10, 3, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (6,  11, '42',  10, 2, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (7,  10, '42',  10, 1, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (8,  12, 'bbb', 10, 1, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (9,  13, 'bbb', 10, 1, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (10, 13, 'A1B', 10, 1, 302);")

	// ORDER BY, reference col from local table with shard key
	mcmp.ExecWithColumnCompareAndNotEmpty("select predef1 from t_user where id = 5 order by predef2")
	// ORDER BY, reference col from local table with shard key and split table key
	mcmp.ExecWithColumnCompareAndNotEmpty("select predef1 from t_user where id = 5 and col = 6 order by col")
	// ORDER BY on scatter
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user order by col")
	// ORDER BY unknown type on scatter
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user order by id")
	// ORDER BY column not in selectExprs
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user order by id")
	// ORDER BY uses column numbers
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user order by 1")
	// ORDER BY column numbers
	mcmp.ExecWithColumnCompareAndNotEmpty("select id as foo from t_user order by 1")
	// ORDER BY invalid col number on scatter
	mcmp.AssertContainsError("select col from t_user order by 2", "Unknown column '2' in 'order clause'")
	// ORDER BY NULL
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user order by null")
	// Order by, '*' expression
	mcmp.ExecWithColumnCompareAndNotEmpty("select * from t_user where id = 5 and col = 6 order by a")
	// Order by, qualified '*' expression
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.* from t_user where id = 5 and col = 6 order by t_user.a")
	// Order by, '*' expression with qualified reference
	mcmp.ExecWithColumnCompareAndNotEmpty("select * from t_user where id = 5 and col = 6 order by t_user.a")
	// ORDER BY on select t.*
	mcmp.ExecWithColumnCompareAndNotEmpty("select t.*, t.col from t_user t order by t.col")
	// ORDER BY on select *
	mcmp.ExecWithColumnCompareAndNotEmpty("select *, col from t_user order by col")
	// ORDER BY on select multi t.*
	mcmp.ExecWithColumnCompareAndNotEmpty("select t.*, t.name, t.*, t.col from t_user t order by t.col")
	// ORDER BY on select multi *
	// MySQL不支持,但是Vitess支持了，因为VTGate把*展开了
	_, err := mcmp.ExecAndIgnore("select *, name, *, col from t_user order by col")
	require.NoError(t, err)
	// ORDER BY on scatter with multiple columns
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user order by a,b")
	// ORDER BY on scatter with multiple columns numbers
	mcmp.ExecWithColumnCompareAndNotEmpty("select id,col,a,b from t_user order by 2,3")
	// ORDER BY RAND()
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user order by RAND()")
	// Order by, qualified '*' expression, name mismatched.
	mcmp.AssertContainsError("select t_user.* from t_user where id = 5 order by e.col", "column 'e.col' not found (errno 1054)")
	// Order by, invalid column number
	// error parsing column number: 18446744073709551616 (errno 1105)
	_, err = mcmp.ExecAndIgnore("select col from t_user where id = 5 order by 18446744073709551616")
	require.ErrorContains(t, err, "error parsing column number: 18446744073709551616 (errno 1105)")
	// Order by, out of range column number
	mcmp.AssertContainsError("select col from t_user where id = 5 order by 2", "Unknown column '2' in 'order clause'")
	// Order by with math functions
	mcmp.ExecWithColumnCompareAndNotEmpty("select * from t_user where id = 5 and col = 6 order by -col1")
	// Order by with string operations
	mcmp.ExecWithColumnCompareAndNotEmpty("select * from t_user where id = 5 and col = 6 order by concat(col,col1) collate utf8mb4_general_ci desc")
	// Order by with math operations
	mcmp.ExecWithColumnCompareAndNotEmpty("select * from t_user where id = 5 and col = 6 order by id+col collate utf8mb4_general_ci desc")
	// routing rules: order by gets pushed for routes
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where id = 1  and col = 6 order by col")
	// order by column alias
	mcmp.ExecWithColumnCompareAndNotEmpty("select col as foo from t_user order by foo")
	// column alias for a table column in order by
	mcmp.ExecWithColumnCompareAndNotEmpty("select col as foo,col2 as col from t_user order by col")
	// order by with ambiguous column reference ; valid in MySQL
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, col from t_user order by col")
	// Order by uses cross-shard expression
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user order by col+1")
	// Order by column number with collate
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1 as a from t_user order by 1 collate utf8mb4_general_ci")
	// aggregation and non-aggregations column without group by
	// results mismatched.
	//        Vitess Results:
	//        [INT64(10) NULL]
	//        MySQL Results:
	//        [INT64(10) INT32(100)]
	// 返回结果不稳定，intcol是随机的一行
	_, err = mcmp.ExecAndIgnore("select count(id), intcol from t_user")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(id) from t_user")
	// aggregation and non-aggregations column with order by
	//results mismatched.
	//        Vitess Results:
	//        [INT64(10) NULL]
	//        MySQL Results:
	//        [INT64(10) INT32(100)]
	// 返回结果不稳定，intcol是随机的一行
	_, err = mcmp.ExecAndIgnore("select count(id), intcol from t_user order by 2")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(id) from t_user order by 1")
	// min column
	// results mismatched.
	//        Vitess Results:
	//        [INT64(1) CHAR("3")]
	//        MySQL Results:
	//        [INT64(1) CHAR("6")]
	// 返回结果不稳定，col是随机的一行
	_, err = mcmp.ExecAndIgnore("select min(id),col from t_user")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompareAndNotEmpty("select min(id) from t_user")
	// max column
	//results mismatched.
	//        Vitess Results:
	//        [INT64(10) CHAR("3")]
	//        MySQL Results:
	//        [INT64(10) CHAR("6")]
	// 返回结果不稳定，col是随机的一行
	_, err = mcmp.ExecAndIgnore("select max(id),col from t_user")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompareAndNotEmpty("select max(id) from t_user")
	// select multi aggregator func columns
	// results mismatched.
	//         Vitess Results:
	//        [INT64(100865) INT64(6) INT64(15) CHAR("a")]
	//        MySQL Results:
	//        [INT64(100865) INT64(6) INT64(15) CHAR("2")]
	// 返回结果不稳定，col是随机的一行
	_, err = mcmp.ExecAndIgnore("select max(id),min(id),count(*),col from t_user")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompareAndNotEmpty("select max(id),min(id),count(*) from t_user")
	// aggregator func columns with single shard
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user where id=5")
	// aggregator func columns with single shard and single table
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user where id=5 and col=6")
	// aggregation and non-aggregations column with group by
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(id), intcol from t_user group by 2")
	// aggregation and non-aggregations column with group by and order by
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(id), intcol from t_user group by 2 order by 1")
	// Scatter order by and aggregation: order by column must reference column from select list
	mcmp.ExecWithColumnCompareAndNotEmpty("select col, count(*) from t_user group by col order by c")
	// ORDER BY NULL for join
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1 as a, t_user.col2, t_music.foo from t_user join t_music on t_user.id = t_music.id where t_user.id = 1 order by null")
	// ORDER BY non-key column for join
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1 as a, t_user.col2, t_music.foo from t_user join t_music on t_user.id = t_music.id where t_user.id = 1 order by a")
	// ORDER BY non-key column for implicit join
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1 as a, t_user.col2, t_music.foo from t_user, t_music where t_user.id = t_music.id and t_user.id = 1 order by a")
	// ORDER BY RAND() for join
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1 as a, t_user.col2, t_music.foo from t_user join t_music on t_user.id = t_music.id where t_user.id = 1 order by RAND()")
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
	mcmp.ExecWithColumnCompareAndNotEmpty("select name from t_user, t_music order by name")
	// join order by with ambiguous column reference ; valid in MySQL
	mcmp.ExecWithColumnCompareAndNotEmpty("select name, name from t_user, t_music order by name")
	// Order by column number with coalesce with columns from both sides
	mcmp.AssertContainsError("select id from t_user, t_user_extra order by coalesce(t_user.col, t_user_extra.col)", "Column 'id' in field list is ambiguous")
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.id from t_user, t_user_extra order by coalesce(t_user.col, t_user_extra.col)")
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

	// HAVING implicitly references table col
	_, err = mcmp.ExecAndIgnore("select t_user.col1 from t_user having col2 = 2")
	// MySQL不支持,报错Unknown column 'col2' in 'having clause'  但是Vitess支持了
	require.NoError(t, err)
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1,col2 from t_user having col2 = 2")
	// TODO: this should be 'Column 'col1' in having clause is ambiguous'
	//# non-ambiguous symbol reference
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.foo, t_user_extra.foo from t_user join t_user_extra having t_user_extra.foo = 5")
	// HAVING multi-route
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1 as ac, t_user.col2, t_user_extra.extra_id from t_user join t_user_extra having 1 = 1 and ac = 2 and ac = t_user.col2 and t_user_extra.extra_id = 3")
	// HAVING uses subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user having id in (select col from t_user)")
	// Order by, verify outer symtab is searched according to its own context.
	_, err = mcmp.ExecAndIgnore("select u.id from t_user u having u.id in (select col2 from t_user where t_user.id = u.id order by u.col)")
	require.ErrorContains(t, err, "VT12001: unsupported: subquery in split table")
	// Equal filter with hexadecimal value
	mcmp.Exec("select count(*) a from t_user having a = 0x01")
	// having filter with %
	mcmp.ExecWithColumnCompareAndNotEmpty("select a.col1 from t_user a join t_music b where a.col1 = b.col group by a.col1 having repeat(a.col1,min(a.id)) like 'A%B' order by a.col1")
	// ORDER BY on scatter with text column
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, textcol1, b from t_user order by a, textcol1, b")
	// ORDER BY on scatter with text column, qualified name TODO: can plan better
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, t_user.textcol1, b from t_user order by a, textcol1, b")
	// ORDER BY on scatter with multiple text columns
	mcmp.ExecWithColumnCompareAndNotEmpty("select a, textcol1, b, textcol2 from t_user order by a, textcol1, b, textcol2")
	// ORDER BY column offset
	mcmp.ExecWithColumnCompareAndNotEmpty("select id as foo from t_music order by 1")
	// ORDER BY after pull-out subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where col in (select col2 from t_user) order by col")
	// ORDER BY NULL after pull-out subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where col in (select col2 from t_user) order by null")
	// ORDER BY RAND() after pull-out subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where col in (select col2 from t_user) order by rand()")
	// LIMIT
	mcmp.ExecWithColumnCompareAndNotEmpty("select col1 from t_user where id = 1 limit 1")
	// limit for scatter
	mcmp.ExecNoCompare("select col from t_user limit 1")
	// limit for scatter with bind var
	// query arguments missing for a (errno 1105) (sqlstate HY000) during query: select col from t_user limit :a
	//mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user limit :a")
	// scatter limit after pullout subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where col in (select col1 from t_user) limit 1")
	// arithmetic limit
	// You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '+1' at line 1 (errno 1064) (sqlstate 42000) during query: select id from t_user limit 1+1
	//mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user limit 1+1")
	// having multi cols
	// MySQL不支持,报错Unknown column 'col2' in 'having clause' (errno 1054)  但是Vitess支持了
	_, err = mcmp.ExecAndIgnore("select col1 from t_user where id=123 having col2 = 2 and col = 1 and col1 = 3")
	require.NoError(t, err)
	//mcmp.ExecWithColumnCompareAndNotEmpty("select col1,col2,intcol from t_user where id=123 having col2 = 2 and intcol = 1 and col1 = 3") Should NOT be empty, but was []
	// join and having
	//mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col1, t_user_extra.col1 from t_user join t_user_extra having t_user_extra.col1 = 2") Should NOT be empty, but was []
	// HAVING uses subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user having id in (select col from t_user)")
	// Equal filter with hexadecimal value
	//mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) a from t_user having a = 0x01") Should NOT be empty, but was []
	// Distinct with cross shard query
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct t_user.a from t_user join t_user_extra")
	// Distinct with column alias
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct a as c, a from t_user")
	// Distinct with same column
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct a, a from t_user")
}
