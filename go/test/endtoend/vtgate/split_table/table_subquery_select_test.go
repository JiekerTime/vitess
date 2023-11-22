package split_table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubQuerySelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	// data init.
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo) values (1, '45', 'aaa', 1, false, 1, 2, 3, 100, 200),(2, 'b', 'bbb', 2, false, 2, 3, 4, 1030, 200), (3, 'c', 'ccc', 3, false, 3, 4, 5, 100, 200)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (1, 101, 101, 100, 'aaa', 100), (2, 102, 102, 200, 'bbb', 200), (3, 103, 103, 300, 'ccc', 300), (4, 104, 104, 400, 'ddd', 400), (5, 105, 105, 500, 'eee', 500)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (101, 1, 'aaa', 10, 200, 'aaa'), (202, 12, 'bbb', 20, 300, 'aaa'), (303, 3, 'ccc', 30, 400, 'bar')")

	// FIXME: not supported cases:
	/**
	table_subquery_select_cases.json:

	Vitess Results: VARCHAR; MySQL Results: CHAR
	1、select (select col from t_user limit 1) as a from t_user join t_user_extra order by a
	2、select t.a from (select (select col from t_user limit 1) as a from t_user join t_user_extra) t

	Subquery not supported:
	1、SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 2, 3)) _inner)
	2、SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT * FROM (SELECT t_music.id FROM t_music WHERE t_music.user_id = 5 LIMIT 10) subquery_for_limit) subquery_for_limit)

	This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery':
	1、SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.col = 'aaa' LIMIT 10)

	table_aggr_cases.json:

	Subquery not supported:
	1、select u.id, e.id from t_user u join t_user_extra e where u.col = e.col and u.col in (select * from t_user where t_user.id = u.id order by col)

	Result merge error:
	1、select col from t_user where col in (select col from t_user) order by null
	2、select col from t_user where col in (select col from t_user) order by rand()

	table_postprocess_cases.json:

	Unknown column 'ue.col' in 'field list':
	1、select (select 1 from t_user u having count(ue.col) > 10) from t_user_extra ue

	table_filter_cases.json:

	Subquery returned more than one row:
	1、select id from t_user where id not in (select col from t_user)
	2、select id from t_user where id = (select col from t_user)
	3、select id1 from t_user where id = (select id2 from t_user where id2 in (select id3 from t_user))

	Unsupported: cross-shard correlated subquery：
	1、select distinct t_user.id, t_user.col from t_user where t_user.col in (select id from t_music where col2 = 'a')

	table_from_cases.json:

	Incorrect usage/placement of 'NEXT':
	1、select 1 from t_user where id in (select next value from seq)

	Results mismatched:
	1、select id from t_user where id in (select id from t_user_extra) and f_key = (select col from t_user_extra limit 1)
	*/

	// table_subquery_select_cases.json

	// Subquery with `IN` condition using columns with matching lookup vindexes
	mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 2, 3))", `[[INT64(101)] [INT64(303)]]`)
	// Subquery with `IN` condition using columns with matching lookup vindexes, with inner scatter query
	mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.foo = 'bar') AND t_music.user_id IN (3, 4, 5)", `[[INT64(303)]]`)
	// Subquery with `IN` condition using columns with matching lookup vindexes
	mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 2, 3)) and t_music.user_id = 3", `[[INT64(303)]]`)
	// Subquery with `IN` condition using columns with matching lookup vindexes, but not a top level predicate
	mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 2, 3)) OR t_music.user_id = 5", `[[INT64(101)] [INT64(303)]]`)
	// Unmergeable scatter subquery with `GROUP BY` on-non vindex column
	mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.col = 'aaa' GROUP BY t_music.col)", `[[INT64(101)]]`)
	// Unmergeable subquery with multiple levels of derived statements, using a multi value `IN` predicate
	mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT * FROM (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 3) LIMIT 10) subquery_for_limit) subquery_for_limit)", `[[INT64(101)] [INT64(303)]]`)
	// Unmergeable subquery with multiple levels of derived statements
	mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT * FROM (SELECT t_music.id FROM t_music LIMIT 10) subquery_for_limit) subquery_for_limit)", `[[INT64(101)] [INT64(202)] [INT64(303)]]`)

	// unsupported subquery in split table
	// `IN` comparison on Vindex with `None` subquery, as routing predicate
	// subquery in split table
	// mcmp.AssertIsEmpty("SELECT `t_music`.id FROM `t_music` WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL)) AND t_music.user_id = 5")
	// `IN` comparison on Vindex with `None` subquery, as non-routing predicate
	// mcmp.AssertIsEmpty("SELECT `t_music`.id FROM `t_music` WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL)) OR t_music.user_id = 5")
	// Mergeable scatter subquery
	// mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.col = 'aaa')", `[[INT64(101)]]`)
	// Mergeable scatter subquery with `GROUP BY` on unique vindex column
	// mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.col = 'aaa' GROUP BY t_music.id)", `[[INT64(101)]]`)
	// Mergeable subquery with multiple levels of derived statements, using a single value `IN` predicate
	// mcmp.AssertMatches("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT * FROM (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (3) LIMIT 10) subquery_for_limit) subquery_for_limit)", `[[INT64(303)]]`)
	// `None` subquery as top level predicate - outer query changes from `Scatter` to `None` on merge
	// mcmp.AssertIsEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL))")
	// `None` subquery as top level predicate - outer query changes from `EqualUnique` to `None` on merge
	// mcmp.AssertIsEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL)) AND t_music.user_id = 5")
	// `None` subquery nested inside `OR` expression - outer query keeps routing information
	// mcmp.AssertIsEmpty("SELECT t_music.id FROM t_music WHERE (t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL)) OR t_music.user_id = 5)")
	// subquery having join table on clause, using column reference of outer select table
	// mcmp.AssertMatches("select (select 1 from t_user u1 join t_user u2 on u1.id = u2.id and u1.id = u3.id) subquery from t_user u3 where u3.id = 1", `[[INT64(1)]]`)

	/** Test passed but field names not matched: **/
	/** Begin **/

	// MySQL field name is `select id from t_user order by id limit 1` , but vitess field name is `1`
	// select (select id from t_user order by id limit 1) from t_user_extra
	mcmp.AssertMatches("select (select id from t_user order by id limit 1) from t_user_extra", `[[INT64(1)] [INT64(1)] [INT64(1)] [INT64(1)] [INT64(1)]]`)
	// earlier columns are in scope in subqueries https://github.com/vitessio/vitess/issues/11246
	// expected: []string{"x", "(SELECT x)"}  actual: []string{"x", "(select x from dual)"}
	mcmp.AssertMatches("SELECT 1 as x, (SELECT x)", `[[INT64(1) INT64(1)]]`)

	/** End **/

	// table_aggr_cases.json
	// HAVING uses subquery
	// unsupported subquery in split table
	// mcmp.AssertMatches("select id from t_user having id in (select id from t_user)", `[[INT64(1)] [INT64(3)] [INT64(2)]]`)
	// Order by, verify outer symtab is searched according to its own context.
	// mcmp.AssertIsEmpty("select u.id from t_user u having u.id in (select col2 from t_user where t_user.id = u.id order by u.col)")
	// ORDER BY after pull-out subquerydd
	mcmp.AssertMatches("select col from t_user where col in (select col from t_user) order by col", `[[CHAR("45")] [CHAR("b")] [CHAR("c")]]`)
	// scatter limit after pullout subquery
	mcmp.AssertMatches("select col from t_user where col in (select col from t_user) limit 1", `[[CHAR("45")]]`)

	// table_postprocess_cases.json
	// scatter aggregate in a subquery
	mcmp.AssertMatches("select a from (select count(*) as a from t_user) t", `[[INT64(3)]]`)
	// Aggregations from derived table used in arithmetic outside derived table
	mcmp.AssertMatches("select A.a, A.b, (A.a / A.b) as d from (select sum(a) as a, sum(b) as b from t_user) A", `[[DECIMAL(6) DECIMAL(9) DECIMAL(0.6667)]]`)

	// table_filter_cases.json
	// unsupported subquery in split table
	// cross-shard subquery in IN clause. Note the improved Underlying plan as SelectIN.
	// mcmp.AssertMatches("select id from t_user where id in (select id from t_user)", `[[INT64(1)] [INT64(3)] [INT64(2)]]`)

}

// table_filter_subquery_cases.json
func TestFilterSubQuerySelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (2,  '3',    'bbb', 2, false, 2, 3, 5, 103, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (3,  'abc',  'ccc', 3, true,  3, 4, 5, 100, 200, 'aaa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (4,  'abc',  'ccc', 3, true,  3, 5, 5, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (5,  '12',   'ccc', 3, true,  3, 4, 5, 103, 200, 'aaa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (10, '10',   'aaa', 1, false, 1, 3, 5, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (1,  1,   2, 200, '1', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (2,  2,   4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (3,  3,   4, 200, '5', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (4,  4,   4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (5,  411, 2, 5,   '2', 5  , 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (6,  411, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (7,  411, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (8,  42,  5, 300, '4', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (9,  42,  3, 300, '5', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (10, 42,  3, 300, '4', 300, 5)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (1, 11, '42',  1, 1, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (2, 10, '42',  1, 2, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (3, 12, 'bbb', 2, 3, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (4, 13, 'bbb', 2, 2, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (5, 12, 'ccc', 2, 3, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (6, 11, '42',  2, 2, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (7, 10, '42',  3, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (8, 12, 'bbb', 1, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (9, 13, 'bbb', 1, 1, 302)")

	// cross-shard subquery in IN clause. # Note the improved Underlying plan as SelectIN.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where id in (select col from t_user where id = 10)")
	// cross-shard subquery in NOT IN clause.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where id not in (select col from t_user where id = 10)")
	// cross-shard subquery as expression
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where id = (select col from t_user where id = 10)")
	// multi-level pullout
	// This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery' (errno 1235) (sqlstate 42000)
	//mcmp.ExecWithColumnCompareAndNotEmpty("select a from t_user where id = (select b from t_user where b in (select c from t_user))")
	// subquery on other table
	_, err := mcmp.ExecAndIgnore("select distinct t_user.id, t_user.col from t_user where t_user.col in (select id from t_music where col2 = 'a')")
	require.ErrorContains(t, err, "VT12001: unsupported: cross-shard correlated subquery")
	_, err = mcmp.ExecAndIgnore("select distinct t_user.id, t_user.col from t_user where t_user.col in (select id from t_music where t_user.col2 = 'a')")
	require.ErrorContains(t, err, "VT12001: unsupported: cross-shard correlated subquery")
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct t_user.id, t_user.col from t_user where t_user.col in (select id from t_music where t_music.col = 'bbb')")
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct t_user.id, t_user.col from t_user where t_user.col in (select id from t_music where foo = '202')")
	// cross-shard subquery in EXISTS clause.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where exists (select col from t_user)")
	// pullout sq after pullout sq
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where not id in (select t_user_extra.col from t_user_extra where t_user_extra.user_id = 42) and id in (select t_user_extra.col from t_user_extra where t_user_extra.user_id = 411)")
	// SelectScatter with NOT EXISTS uncorrelated subquery
	mcmp.ExecWithColumnCompareAndNotEmpty("select u1.col from t_user as u1 where not exists (select u2.name from t_user u2 where u2.id = 50)")
}

// table_subquery_select_cases.json
func TestSubQuerySelectCase(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (2,  '3',    'bbb', 2, false, 2, 3, 5, 103, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (3,  'abc',  'ccc', 3, true,  3, 4, 5, 100, 200, 'aaa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (4,  'abc',  'ccc', 3, true,  3, 5, 5, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (5,  '12',   'ccc', 3, true,  3, 4, 5, 103, 200, 'aaa')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (10, '10',   'aaa', 1, false, 1, 3, 5, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (1,  1,   2, 200, '1', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (2,  2,   4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (3,  3,   4, 200, '5', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (4,  4,   4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (5,  411, 2, 5,   '2', 5  , 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (6,  411, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (7,  411, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (8,  42,  5, 300, '4', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (9,  42,  3, 300, '5', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (10, 42,  3, 300, '4', 300, 5)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (1, 1, '42',  1, 1, 202, 'pop')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (2, 2, '42',  1, 2, 202, 'bob')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (3, 3, 'bbb', 2, 3, 202, 'bob')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (4, 4, 'bbb', 2, 2, 202, 'bob')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (5, 1, 'ccc', 2, 3, 202, 'bob')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (6, 2, '42',  2, 2, 302, 'bob')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (7, 3, '42',  3, 1, 302, 'pop')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (8, 4, 'bbb', 1, 1, 302, 'pop')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo, genre) VALUES (9, 5, 'bbb', 1, 1, 302, 'pop')")

	// select (select col from t_user limit 1) as a from t_user join t_user_extra order by a
	// results mismatched.
	//        Vitess Results:
	//        [VARCHAR("10")]
	//        [VARCHAR("10")]
	//        [VARCHAR("10")]...
	//        MySQL Results:
	//        [CHAR("3")]
	//        [CHAR("3")]
	//        [CHAR("3")]...
	//mcmp.ExecWithColumnCompareAndNotEmpty("select (select col from t_user limit 1) as a from t_user join t_user_extra order by a")
	// select t.a from (select (select col from t_user limit 1) as a from t_user join t_user_extra) t
	// results mismatched.
	//        Vitess Results:
	//        [VARCHAR("3")]
	//        [VARCHAR("3")]
	//        [VARCHAR("3")]...
	//        MySQL Results:
	//        [CHAR("3")]
	//        [CHAR("3")]
	//        [CHAR("3")]...
	//mcmp.ExecWithColumnCompareAndNotEmpty("select t.a from (select (select col from t_user limit 1) as a from t_user join t_user_extra) t")
	// select (select id from t_user order by id limit 1) from t_user_extra
	// expected: []string{"(select id from t_user order by id limit 1)"}
	// actual  : []string{"2"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompareAndNotEmpty("select (select id from t_user order by id limit 1) from t_user_extra")
	mcmp.ExecAndNotEmpty("select (select id from t_user order by id limit 1) from t_user_extra")
	// Subquery with `IN` condition using columns with matching lookup vindexes
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 2, 3))")
	// Subquery with `IN` condition using columns with matching lookup vindexes, with derived table
	_, err := mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 2, 3)) _inner)")
	require.ErrorContains(t, err, "VT12001: unsupported: unable to use: *sqlparser.DerivedTable in split table")
	// Subquery with `IN` condition using columns with matching lookup vindexes, with inner scatter query
	// plan单元测试期望报错VT12001: unsupported: multiple tables in split table，集成测试可以执行
	// 由于plan单元测试元数据不一致导致，plan的t_music表多了{"column": "id", "name": "music_user_map"}
	//mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.foo = 'bar') AND t_music.user_id IN (3, 4, 5)")
	//_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.foo = 'bar') AND t_music.user_id IN (3, 4, 5)")
	//require.ErrorContains(t, err, "VT12001: unsupported: multiple tables in split table")
	// Subquery with `IN` condition using columns with matching lookup vindexes
	//_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 2, 3)) and t_music.user_id = 5")
	//require.ErrorContains(t, err, "VT12001: unsupported: multiple tables in split table")
	// Subquery with `IN` condition using columns with matching lookup vindexes, but not a top level predicate
	//_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (1, 2, 3)) OR t_music.user_id = 5")
	//require.ErrorContains(t, err, "VT12001: unsupported: multiple tables in split table")
	// `IN` comparison on Vindex with `None` subquery, as routing predicate
	//_, err = mcmp.ExecAndIgnore("SELECT `t_music`.id FROM `t_music` WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL)) AND t_music.user_id = 5")
	//require.ErrorContains(t, err, "VT12001: unsupported: multiple tables in split table")
	// `IN` comparison on Vindex with `None` subquery, as non-routing predicate
	_, err = mcmp.ExecAndIgnore("SELECT `t_music`.id FROM `t_music` WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL)) OR t_music.user_id = 5")
	require.ErrorContains(t, err, "VT12001: unsupported: subquery in split table")
	// Mergeable scatter subquery
	//_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.genre = 'pop')")
	//require.ErrorContains(t, err, "VT12001: unsupported: subquery in split table")
	// Mergeable scatter subquery with `GROUP BY` on unique vindex column
	//_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.genre = 'pop' GROUP BY t_music.id)")
	//require.ErrorContains(t, err, "VT12001: unsupported: subquery in split table")
	// Unmergeable scatter subquery with LIMIT
	// This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'
	//mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.genre = 'pop' LIMIT 10)")
	// Unmergeable scatter subquery with `GROUP BY` on-non vindex column
	// results mismatched.
	//        Vitess Results:
	//        [INT64(1)]
	//        MySQL Results:
	//        [INT64(1)]
	//        [INT64(7)]
	//        [INT64(8)]
	//        [INT64(9)]
	//mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.genre = 'pop' GROUP BY t_music.genre)")
	// Mergeable subquery with multiple levels of derived statements
	_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT * FROM (SELECT t_music.id FROM t_music WHERE t_music.user_id = 5 LIMIT 10) subquery_for_limit) subquery_for_limit)")
	require.ErrorContains(t, err, "VT12001: unsupported: unable to use: *sqlparser.DerivedTable in split table")
	// Mergeable subquery with multiple levels of derived statements, using a single value `IN` predicate
	//_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT * FROM (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (5) LIMIT 10) subquery_for_limit) subquery_for_limit)")
	//require.ErrorContains(t, err, "VT12001: unsupported: multiple tables in split table")
	// Unmergeable subquery with multiple levels of derived statements, using a multi value `IN` predicate
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT * FROM (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (5, 6) LIMIT 10) subquery_for_limit) subquery_for_limit)")
	// Unmergeable subquery with multiple levels of derived statements
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT * FROM (SELECT * FROM (SELECT t_music.id FROM t_music LIMIT 10) subquery_for_limit) subquery_for_limit)")
	// `None` subquery as top level predicate - outer query changes from `Scatter` to `None` on merge
	_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL))")
	require.ErrorContains(t, err, "VT12001: unsupported: subquery in split table")
	// `None` subquery as top level predicate - outer query changes from `EqualUnique` to `None` on merge
	_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL)) AND t_music.user_id = 5")
	require.ErrorContains(t, err, "VT12001: unsupported: subquery in split table")
	// `None` subquery nested inside `OR` expression - outer query keeps routing information
	_, err = mcmp.ExecAndIgnore("SELECT t_music.id FROM t_music WHERE (t_music.id IN (SELECT t_music.id FROM t_music WHERE t_music.user_id IN (NULL)) OR t_music.user_id = 5)")
	require.ErrorContains(t, err, "VT12001: unsupported: subquery in split table")
	// subquery having join table on clause, using column reference of outer select table
	_, err = mcmp.ExecAndIgnore("select (select 1 from t_user u1 join t_user u2 on u1.id = u2.id and u1.id = u3.id) subquery from t_user u3 where u3.id = 1")
	require.ErrorContains(t, err, "VT12001: unsupported: subquery in split table")
	// Reference with a subquery which cannot be merged
	// expected: []string{"exists(select * from t_user)"}
	// actual  : []string{"1"}
	// column names do not match - the expected values are what mysql produced
	mcmp.ExecAndNotEmpty("select exists(select * from t_user)")
	// Unmergeable subquery with `MAX` aggregate
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT MAX(t_music.id) FROM t_music WHERE t_music.user_id IN (5, 6))")
	// Mergeable subquery with `MAX` aggregate with `EqualUnique` route operator
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT MAX(t_music.id) FROM t_music WHERE t_music.user_id = 5)")
	// Mergeable subquery with `LIMIT` due to `EqualUnique` route
	// This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'
	//mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id IN (SELECT MAX(t_music.id) FROM t_music WHERE t_music.user_id = 5 LIMIT 10)")
	// Unmergeable subquery with `MAX` aggregate and outer order
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_music.id FROM t_music WHERE t_music.id  > (SELECT MAX(t_music.user_id) FROM t_music) order by user_id DESC")
	// scar subquery in select expressions
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT id,(select col from t_music limit 1) as col1 FROM t_music order by user_id DESC")
}
