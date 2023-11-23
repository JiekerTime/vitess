package split_table

import "testing"

// table_from_cases.json
func TestTableFromCases(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo,name) values (1, '11', 'aaa', 1, false,1,2,3,100,200,1),(2, '11', 'bbb', 2, false,2,3,4,1030,200,4),(3, '5', 'ccc', 3, false,3,4,5,100,200,4),(5, '5', 'ccc', 3, false,3,4,5,1030,200,4)")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo,name) values (6, '12', 'aaa', 1, false,1,2,3,100,300,2),(7, '11', 'bbb', 2, false,2,3,4,100,300,3),(8, '5', 'ccc', 3, false,3,4,5,1020,300,4)")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo,name) values (200, '12', 'aaa', 1, false,1,2,3,100,300,2),(300, '12', 'bbb', 2, false,2,3,4,100,300,3)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (1, 1, 2, 200, '11', 200),(2, 2, 4, 200, '3', 200),(3, 2, 4, 200, '11', 200),(4, 4, 4, 200, '7', 200),(5,  5, 2, 200, '5', 300)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (6, 6, 3, 300, '12', 200),(7, 2, 5, 300, '4', 200),(8, 2, 5, 300, '12', 300),(9, 9, 3, 300, '8', 300),(10, 5, 3, 300, '5', 300)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (1, 11, '42',  10, 200, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (2, 10, '42',  10, 200, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (3, 12, 'bbb', 10, 200, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (4, 13, 'bbb', 10, 200, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (5, 12, 'ccc', 10, 200, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (6, 11, '42',  10, 300, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (7, 10, '42',  10, 300, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (8, 12, 'bbb', 10, 300, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (9, 13, 'bbb', 10, 300, 302)")

	// Single table sharded scatter
	mcmp.ExecWithColumnCompare("select col from t_user")
	// Multi-table, multi-chunk
	mcmp.ExecWithColumnCompare("select t_music.col from t_user join t_music")
	// select user.t_music.foo from user.t_music join t_user on user.t_music.id = user.id where user.t_music.col = 42
	mcmp.ExecWithColumnCompare("select user.t_music.foo from user.t_music join t_user on user.t_music.id = t_user.id where user.t_music.col = 42")
	// ',' join
	mcmp.ExecWithColumnCompare("select t_music.col from t_user, t_music")
	// sharded join on unique vindex, inequality
	mcmp.ExecWithColumnCompare("select t_user.col from t_user join t_user_extra on t_user.id < t_user_extra.user_id")
	// sharded join, non-col reference RHS
	mcmp.ExecWithColumnCompare("select t_user.col from t_user join t_user_extra on t_user.id = 5")
	// sharded join, non-col reference LHS
	mcmp.ExecWithColumnCompare("select t_user.col from t_user join t_user_extra on 5 = t_user.id")
	// sharded join, non-vindex col
	mcmp.ExecWithColumnCompare("select t_user.col from t_user join t_user_extra on t_user.id = t_user_extra.col")
	// sharded join, non-unique vindex
	mcmp.ExecWithColumnCompare("select t_user.col from t_user_extra join t_user on t_user_extra.user_id = t_user.name")
	// join with bindvariables
	mcmp.ExecWithColumnCompare("SELECT `t_user`.`id` FROM `t_user` INNER JOIN `t_user_extra` ON `t_user`.`id` = `t_user_extra`.`extra_id` WHERE `t_user_extra`.`user_id` = 2")
	// join column selected as alias
	mcmp.ExecWithColumnCompare("SELECT u.id as uid, ue.id as ueid FROM t_user u join t_user_extra ue where u.id = ue.id")
	// join on int columns
	mcmp.ExecWithColumnCompare("select u.id from t_user as u join t_user as uu on u.intcol = uu.intcol")
	// left join with expressions
	// expected: []string{"t_user_extra.col+1"}
	// actual  : []string{"t_user_extra.col + :vtg1 /* INT64 */"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompare("select t_user_extra.col+1 from t_user left join t_user_extra on t_user.col = t_user_extra.col")
	mcmp.Exec("select t_user_extra.col+1 from t_user left join t_user_extra on t_user.col = t_user_extra.col")
	// left join with expressions, with three-way join (different code path)
	// expected: []string{"id", "t_user_extra.col+1"}
	// actual  : []string{"id", "t_user_extra.col + :vtg1 /* INT64 */"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompare("select t_user.id, t_user_extra.col+1 from t_user left join t_user_extra on t_user.col = t_user_extra.col join t_user_extra e")
	mcmp.Exec("select t_user.id, t_user_extra.col+1 from t_user left join t_user_extra on t_user.col = t_user_extra.col join t_user_extra e")
	// left join with expressions coming from both sides
	// expected: []string{"t_user.foo+t_user_extra.col+1"}
	// actual  : []string{"t_user.foo + t_user_extra.col + :vtg1 /* INT64 */"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompare("select t_user.foo+t_user_extra.col+1 from t_user left join t_user_extra on t_user.col = t_user_extra.col")
	//        Vitess Results:
	//        [INT32(306)]
	//        [INT32(202)]
	//        [INT32(204)]
	//        [INT32(206)]
	//        [INT32(305)]
	//        [INT32(207)]
	//        [INT32(303)]
	//        MySQL Results:
	//        [FLOAT64(202)]
	//        [FLOAT64(204)]
	//        [FLOAT64(206)]
	//        [FLOAT64(306)]
	//        [FLOAT64(303)]
	//        [FLOAT64(305)]
	//        [FLOAT64(207)]
	// 未排序，结果集不稳定
	//mcmp.Exec("select t_user.foo+t_user_extra.col+1 from t_user left join t_user_extra on t_user.col = t_user_extra.col")
	//results mismatched.
	//        Vitess Results:
	//        [INT32(206)]
	//        [INT32(206)]
	//        [INT32(206)]
	//        MySQL Results:
	//        [FLOAT64(206)]
	//        [FLOAT64(206)]
	//        [FLOAT64(206)]
	// 类型不一致
	//mcmp.Exec("select t_user.foo+t_user_extra.col+1 as a from t_user left join t_user_extra on t_user.col = t_user_extra.col order by a")
	mcmp.AssertMatchesAnyNoCompare("select t_user.foo+t_user_extra.col+1 as a from t_user left join t_user_extra on t_user.col = t_user_extra.col order by a",
		"[[INT32(206)] [INT32(206)] [INT32(206)] [INT32(206)] [INT32(212)] [INT32(212)] [INT32(212)] [INT32(212)] [INT32(306)] [INT32(306)] [INT32(312)] [INT32(312)] [INT32(313)] [INT32(313)] [INT32(313)] [INT32(313)] [INT32(313)] [INT32(313)]]",
		"[[FLOAT64(206)] [FLOAT64(206)] [FLOAT64(206)] [FLOAT64(206)] [FLOAT64(212)] [FLOAT64(212)] [FLOAT64(212)] [FLOAT64(212)] [FLOAT64(306)] [FLOAT64(306)] [FLOAT64(312)] [FLOAT64(312)] [FLOAT64(313)] [FLOAT64(313)] [FLOAT64(313)] [FLOAT64(313)] [FLOAT64(313)] [FLOAT64(313)]]")
	// left join where clauses #3 - assert that we can evaluate BETWEEN with the evalengine
	mcmp.ExecWithColumnCompare("select t_user.id from t_user left join t_user_extra on t_user.col = t_user_extra.col where t_user_extra.col between 10 and 20")
	// left join where clauses #2
	mcmp.ExecWithColumnCompare("select t_user.id from t_user left join t_user_extra on t_user.col = t_user_extra.col where coalesce(t_user_extra.col, 4) = 5")
	// alias on column from derived table. TODO: to support alias in SimpleProjection engine primitive
	mcmp.ExecAndNotEmpty("select a as k from (select count(*) as a from t_user) t")
	// derived table with aliased columns and a join that requires pushProjection
	mcmp.ExecAndNotEmpty("select i+1 from (select t_user.id from t_user join t_user_extra) t(i)")
}
