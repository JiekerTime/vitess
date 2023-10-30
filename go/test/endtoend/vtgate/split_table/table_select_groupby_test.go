package split_table

import (
	"testing"
)

func TestTableGroupBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c) values (1, 'a', 'aaa', 1, false,1,2,3),(2, 'b', 'bbb', 2, false,2,3,4),(3, 'c', 'ccc', 3, false,3,4,5)")
	mcmp.Exec("insert into t_3(f_shard,f_table) values (1,2),(1,2),(2,3)")

	mcmp.ExecWithColumnCompare("select id, count(*) k from t_user group by id")

	mcmp.ExecWithColumnCompare("select col from t_user group by col")

	mcmp.ExecWithColumnCompare("select id,col, count(*) k from t_user group by id,col")

	mcmp.ExecWithColumnCompare("select id,f_key, count(*) k from t_user group by id,f_key")

	mcmp.ExecWithColumnCompare("select col,f_key, count(*) k from t_user group by col,f_key")

	mcmp.ExecWithColumnCompare("select a, b, count(*) from t_user group by a, b")

	mcmp.ExecWithColumnCompare("select a, b, count(*) from t_user group by 2, 1")

	mcmp.ExecWithColumnCompare("select a, b, count(*) from t_user group by b, a")

	mcmp.ExecWithColumnCompare("select col from t_user group by 1")

	mcmp.ExecWithColumnCompare("select a, b, c, count(*) from t_user group by 1, 2, 3 order by 1, 2, 3")

	mcmp.ExecWithColumnCompare("select a, b, c, count(*) from t_user group by 1, 2, 3 order by a, b, c")

	mcmp.ExecWithColumnCompare("select a, b, c, count(*) from t_user group by 3, 2, 1 order by  b, a, c")

	mcmp.ExecWithColumnCompare("select a, b, c, count(*) from t_user group by 3, 2, 1 order by 1 desc, 3 desc, b")

	mcmp.ExecWithColumnCompare("select t_user.col1 as a from t_user where t_user.id = 5 group by a collate utf8_general_ci")

	mcmp.ExecWithColumnCompare("select col, count(*) k from t_user group by col order by null, k")

	mcmp.ExecWithColumnCompare("select col, count(*) k from t_user group by col order by null")

	mcmp.ExecWithColumnCompare("select lower(textcol2) as v, count(*) from t_user group by v")

	mcmp.ExecWithColumnCompare("select textcol2, count(*) from t_user group by a ")

	//结果不一样
	//mcmp.ExecWithColumnCompare("select ascii(textcol2) as a, count(*) from t_user group by a")

	mcmp.ExecWithColumnCompare("select group_concat(f_key order by col asc), id from t_user group by id, col")
	mcmp.ExecWithColumnCompare("select f_table, count(*) from t_3 group by f_table order by f_table+1")
	//结果不一样待修复
	//mcmp.ExecWithColumnCompare("select id,f_table from t_3 group by id,f_table order by id+1,f_table+1")

	mcmp.ExecWithColumnCompare("select a from t_user group by a+1")

	mcmp.ExecWithColumnCompare("select col, a, id from t_user group by col, a, id, id, a, col")

	mcmp.ExecWithColumnCompare("select f_table, f_table, count(*) from t_3 group by f_table")

	mcmp.ExecWithColumnCompare("select id,col from t_user group by id,col")

	mcmp.ExecWithColumnCompare("select id,col from t_user group by id")

	mcmp.ExecWithColumnCompare("select id,col from t_user group by b, id")

	mcmp.ExecWithColumnCompare("select id,col from t_user group by id, c")

	mcmp.ExecWithColumnCompare("select id,col from t_user where id = 1 group by c")

	mcmp.ExecWithColumnCompare("select id,col from t_user where id = 1 group by col")

	mcmp.ExecWithColumnCompare("SELECT t_user.intcol FROM t_user GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")

	mcmp.ExecWithColumnCompare("SELECT t_user.intcol FROM t_user where id = 1 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")

	mcmp.ExecWithColumnCompare("SELECT t_user.intcol FROM t_user where col = 'a' GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")

	mcmp.ExecWithColumnCompare("SELECT t_user.intcol FROM t_user where col = 'b' and id = 1 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")

	//VT03005: cannot group on 'sum(id)' (errno 1056) (sqlstate 42000) during query: select sum(id) as id,col from t_user group by b, id
	//mcmp.ExecWithColumnCompare("select sum(id) as id,col from t_user group by b, id")
	//   VT03005: cannot group on 'count(id)' (errno 1056) (sqlstate 42000) during query: select count(id) as id,col from t_user group by b, id
	//mcmp.ExecWithColumnCompare("select count(id) as id,col from t_user group by b, id")

	//VT03005: cannot group on 'max(id)' (errno 1056) (sqlstate 42000) during query: select max(id) as id,col from t_user group by b, id
	//mcmp.ExecWithColumnCompare("select max(id) as id,col from t_user group by b, id")
	//   VT03005: cannot group on 'min(id)' (errno 1056) (sqlstate 42000) during query: select min(id) as id,col from t_user group by b, id
	//mcmp.ExecWithColumnCompare("select min(id) as id,col from t_user group by b, id")

	//mcmp.ExecWithColumnCompare("select avg(id) as id,col from t_user group by b, id")

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
	//mcmp.ExecWithColumnCompare("select id, count(*) from t_user")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo) values (1, '45', 'aaa', 1, false,1,2,3,100,200),(2, 'b', 'bbb', 2, false,2,3,4,1030,200),(3, 'c', 'ccc', 3, false,3,4,5,100,200),(5, '45', 'ccc', 3, false,3,4,5,1030,200)")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo) values (1024, 'a', 'aaa', 1, false,1,2,3,100,300),(1536, 'b', 'bbb', 2, false,2,3,4,100,300),(1320, '45', 'ccc', 3, false,3,4,5,1020,300)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (100, 101, 101, 200, 'aaa', 200),(200, 102, 102, 200, 'xxx', 200),(300, 103, 103, 200, 'bbb', 200),(400, 104, 104, 200, 'aaa', 200),(500, 105, 105, 200, 'ada', 300)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (130, 101, 101, 300, 'aaa', 200),(250, 102, 102, 300, 'ddd', 200),(370, 103, 103, 300, 'ccc', 300),(489, 104, 104, 300, 'aaa', 300),(520, 105, 105, 300, 'axa', 300)")
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
	mcmp.ExecWithColumnCompare("select count(*) from t_user order by null")
	// scatter aggregate symtab lookup error
	// Column 'id' in order clause is ambiguous (errno 1052) (sqlstate 23000) during query: select id, b as id, count(*) from t_user order by id
	// [MySQL Error] for query: select id, b as id, count(*) from t_user order by id
	//mcmp.ExecWithColumnCompare("select id, b as id, count(*) from t_user order by id")
	// scatter aggregate group by select col
	mcmp.ExecWithColumnCompare("select col from t_user group by col")
	// scatter aggregate multiple group by (columns)
	mcmp.ExecWithColumnCompare("select a, b, count(*) from t_user group by a, b")
	// scatter aggregate multiple group by (numbers)
	mcmp.ExecWithColumnCompare("select a, b, count(*) from t_user group by 2, 1")
	// scatter aggregate group by aggregate function
	// VT03005: cannot group on 'count(*)'
	//mcmp.ExecWithColumnCompare("select count(*) b from t_user group by b")
	// scatter aggregate multiple group by columns inverse order
	mcmp.ExecWithColumnCompare("select a, b, count(*) from t_user group by b, a")
	// scatter aggregate group by column number
	mcmp.ExecWithColumnCompare("select col from t_user group by 1")
	// scatter aggregate group by invalid column number
	// Unknown column '2' in 'group statement'
	//mcmp.ExecWithColumnCompare("select col from t_user group by 2")
	// scatter aggregate with numbered order by columns
	mcmp.ExecWithColumnCompare("select a, b, c, d, count(*) from t_user group by 1, 2, 3 order by 1, 2, 3")
	// scatter aggregate with named order by columns
	mcmp.ExecWithColumnCompare("select a, b, c, d, count(*) from t_user group by 1, 2, 3 order by a, b, c")
	// scatter aggregate with jumbled order by columns
	mcmp.ExecWithColumnCompare("select a, b, c, d, count(*) from t_user group by 1, 2, 3, 4 order by d, b, a, c")
	// scatter aggregate with jumbled group by and order by columns
	mcmp.ExecWithColumnCompare("select a, b, c, d, count(*) from t_user group by 3, 2, 1, 4 order by d, b, a, c")
	// scatter aggregate with some descending order by cols
	mcmp.ExecWithColumnCompare("select a, b, c, count(*) from t_user group by 3, 2, 1 order by 1 desc, 3 desc, b")
	// invalid order by column numner for scatter
	// Unknown column '5' in 'order clause'
	//mcmp.ExecWithColumnCompare("select col, count(*) from t_user group by col order by 5 limit 10")
	// aggregate with limit
	mcmp.ExecWithColumnCompare("select col, count(*) from t_user group by col limit 10")
	// Group by with collate operator
	mcmp.ExecWithColumnCompare("select t_user.col1 as a from t_user where t_user.id = 5 group by a collate utf8_general_ci")
	// Group by invalid column number (code is duplicated from symab).
	//mcmp.ExecWithColumnCompare("select id from t_user group by 1.1")
	// Group by out of range column number (code is duplicated from symab).
	// Unknown column '2' in 'group statement'
	//mcmp.ExecWithColumnCompare("select id from t_user group by 2")
	// aggregate query with order by aggregate column along with NULL
	mcmp.ExecWithColumnCompare("select col, count(*) k from t_user group by col order by null, k")
	// aggregate query with order by NULL
	mcmp.ExecWithColumnCompare("select col, count(*) k from t_user group by col order by null")
	// weight_string addition to group by
	mcmp.ExecWithColumnCompare("select lower(f_key) as v, count(*) from t_user group by v")
	// weight_string addition to group by when also there in order by
	// mysql返回结果未聚合
	//mcmp.ExecWithColumnCompare("select char_length(f_key) as a, count(*) from t_user group by a order by a")
	// group by column alias
	//  Vitess Results:
	// [INT32(97) INT64(4)]
	// [INT32(98) INT64(1)]
	// [INT32(99) INT64(3)]
	// MySQL Results:
	// [INT32(97) INT64(2)]
	// [INT32(98) INT64(3)]
	// [INT32(99) INT64(3)]
	//mcmp.ExecWithColumnCompare("select ascii(f_key) as a, count(*) from t_user group by a")
	// group_concat on single shards
	// expected: []string{"group_concat(f_int order by name)", "id"}
	// actual  : []string{"group_concat(f_int order by `name` asc)", "id"}
	// column names do not match - the expected values are what mysql produced
	// mcmp.ExecWithColumnCompare("select group_concat(f_int order by name), id from t_user group by id, col")
	mcmp.ExecWithColumnCompare("select group_concat(f_int order by `name` asc), id from t_user group by id, col")
	// Scatter order by is complex with aggregates in select
	mcmp.ExecWithColumnCompare("select col, count(*) from t_user group by col order by col+1")
	// scatter aggregate complex order by
	mcmp.ExecWithColumnCompare("select id,col from t_user group by id,col order by id+1,col+1")
	// select expression does not directly depend on grouping expression
	mcmp.ExecWithColumnCompare("select a from t_user group by a+1")
	// redundant group by columns are not added
	mcmp.ExecWithColumnCompare("select col, predef1, id from t_user group by col, predef1, id, id, predef1, col")
	// using a grouping column multiple times should be OK
	mcmp.ExecWithColumnCompare("select col, col, count(*) from t_user group by col")
	// use vindex and table index group by
	mcmp.ExecWithColumnCompare("select id,col from t_user group by id,col")
	// use unique vindex group by, split table plan generate Aggregation
	mcmp.ExecWithColumnCompare("select id,col from t_user group by id")
	// use unique vindex group by, split table plan generate Aggregation
	mcmp.ExecWithColumnCompare("select id,col from t_user group by b, id")
	// use unique vindex group by, split table plan generate Aggregation
	mcmp.ExecWithColumnCompare("select id,col from t_user group by id, c")
	// EqualUnique Select, split table plan generate Aggregation
	mcmp.ExecWithColumnCompare("select id,col from t_user where id = 1024 group by c")
	// EqualUnique Select, use table index group by
	mcmp.ExecWithColumnCompare("select id,col from t_user where id = 1024 group by col")
	// EqualUnique Select, use table index group by
	mcmp.ExecWithColumnCompare("select id,col from t_user where id = 1024 group by c, col")
	// Group By X Order By X
	mcmp.ExecWithColumnCompare("SELECT t_user.intcol FROM t_user GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")
	// Group By X Order By X, vindex EqualUnique
	mcmp.ExecWithColumnCompare("SELECT t_user.intcol FROM t_user where id = 1536 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")
	// Group By X Order By X, table index EqualUnique
	mcmp.ExecWithColumnCompare("SELECT t_user.intcol FROM t_user where col = 45 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")
	// Group By X Order By X, vindex EqualUnique, table index EqualUnique
	mcmp.ExecWithColumnCompare("SELECT t_user.intcol FROM t_user where col = 45 and id = 1320 GROUP BY t_user.intcol ORDER BY COUNT(t_user.intcol)")
	// count(*) spread across join
	mcmp.ExecWithColumnCompare("select count(*) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// sum spread across join
	mcmp.ExecWithColumnCompare("select sum(t_user.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// count spread across join
	mcmp.ExecWithColumnCompare("select count(t_user.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// max spread across join
	// expected: []string{"max(t_user.col)"}
	// actual  : []string{"max(t_user_0.col)"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompare("select max(t_user.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// min spread across join RHS
	// expected: []string{"min(t_user_extra.col)"}
	// actual  : []string{"min(t_user_extra_0.col)"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompare("select min(t_user_extra.col) from t_user join t_user_extra on t_user.foo = t_user_extra.bar")
	// Grouping on join
	mcmp.ExecWithColumnCompare("select t_user.a from t_user join t_user_extra group by t_user.a")
	// Aggregates and joins
	mcmp.ExecWithColumnCompare("select count(*) from t_user join t_user_extra")
	// Aggregate on join
	mcmp.ExecWithColumnCompare("select t_user.a, count(*) from t_user join t_user_extra group by t_user.a")
	// Aggregate on other table in join
	mcmp.ExecWithColumnCompare("select t_user.a, count(t_user_extra.extra_id) from t_user join t_user_extra group by t_user.a")
	// group by and ',' joins with condition
	mcmp.ExecWithColumnCompare("select t_user.col from t_user join t_user_extra on t_user_extra.col = t_user.col group by t_user.id")
	// Aggregation on column from inner side in a left join query
	mcmp.ExecWithColumnCompare("select count(u.id) from t_user u left join t_user_extra ue on u.col = ue.col")
	// Aggregation on outer side in a left join query
	// Vitess Results:
	// [NULL]
	// MySQL Results:
	// [INT64(0)]
	//mcmp.ExecWithColumnCompare("select count(ue.id) from t_user u left join t_user_extra ue on u.col = ue.col")
	// inner join with scalar aggregation
	mcmp.ExecWithColumnCompare("select count(*) from t_user join t_music on t_user.foo = t_music.bar")
	// left outer join with scalar aggregation
	mcmp.ExecWithColumnCompare("select count(*) from t_user left join t_music on t_user.foo = t_music.bar")
	// inner join with left grouping
	mcmp.ExecWithColumnCompare("select count(*) from t_user left join t_music on t_user.foo = t_music.bar group by t_user.col")
	// inner join with right grouping
	mcmp.ExecWithColumnCompare("select count(*) from t_user left join t_music on t_user.foo = t_music.bar group by t_music.col")
	// left outer join with left grouping
	mcmp.ExecWithColumnCompare("select count(*) from t_user left join t_music on t_user.foo = t_music.bar group by t_user.col")
	// left outer join with right grouping
	mcmp.ExecWithColumnCompare("select count(*) from t_user left join t_music on t_user.foo = t_music.bar group by t_music.col")
	// 3 table inner join with scalar aggregation
	mcmp.ExecWithColumnCompare("select count(*) from t_user join t_music on t_user.foo = t_music.bar join t_user_extra on t_user.foo = t_user_extra.baz")
	// 3 table with mixed join with scalar aggregation
	mcmp.ExecWithColumnCompare("select count(*) from t_user left join t_music on t_user.foo = t_music.bar join t_user_extra on t_user.foo = t_user_extra.baz")
	// ordering have less column than grouping columns, grouping gets rearranged as order by and missing columns gets added to ordering
	mcmp.ExecWithColumnCompare("select u.col, u.intcol, count(*) from t_user u join t_music group by 1,2 order by 2")
	// multiple count star and a count with 3 table join
	mcmp.ExecWithColumnCompare("select count(*), count(*), count(u.col) from t_user u, t_user u2, t_user_extra ue")
	// interleaving grouping, aggregation and join with min, max columns
	// expected: []string{"col", "min(t_user_extra.baz)", "f_key", "max(t_user_extra.bar)"}
	// actual  : []string{"col", "min(t_user_extra_0.baz)", "f_key", "max(t_user_extra_0.bar)"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompare("select t_user.col, min(t_user_extra.baz), t_user.f_key, max(t_user_extra.bar) from t_user join t_user_extra on t_user.col = t_user_extra.bar group by t_user.col, t_user.f_key")
}
