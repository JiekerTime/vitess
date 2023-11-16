package split_table

import (
	"testing"
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
