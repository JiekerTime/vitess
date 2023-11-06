package split_table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// table_memory_sort_cases.json
func TestMemorySortCases(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1,      '1',    'aaa', 1, false, 1, 2, 3, 100, 200, 1)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (2,      '3',    'bbb', 2, false, 2, 3, 4, 103, 200, 4)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (3,      '6',    'ccc', 3, false, 3, 4, 5, 100, 200, 4)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (5,      '5',    'ccc', 3, false, 3, 4, 5, 103, 200, 4)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (6,      '2',    'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (7,      '1024', 'bbb', 2, false, 2, 3, 4, 100, 300, 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (8,      '1024', 'ccc', 3, false, 3, 4, 5, 102, 300, 4)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (9,      '1024', 'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (10,     '1024', 'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1019,   '1024', 'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1020,   '1024', 'aaa', 1, false, 2, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1021,   '1024', 'aaa', 1, false, 3, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1022,   '1024', 'aaa', 1, false, 2, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (100865, '1024', 'aaa', 1, false, 2, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1024,   '1024', 'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (1, 11, '42',  1, 1, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (2, 10, '42',  1, 2, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (3, 12, 'bbb', 2, 3, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (4, 13, 'bbb', 2, 2, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (5, 12, 'ccc', 2, 3, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (6, 11, '42',  2, 2, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (7, 10, '42',  3, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (8, 12, 'bbb', 1, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (9, 13, 'bbb', 1, 1, 302)")

	// scatter aggregate order by references ungrouped column
	// results mismatched.
	//        Vitess Results:
	//        [INT32(2) INT32(2) INT64(5)]
	//        [INT32(1) INT32(2) INT64(6)]
	//        [INT32(3) INT32(4) INT64(4)]
	//        MySQL Results:
	//        [INT32(1) INT32(2) INT64(6)]
	//        [INT32(2) INT32(3) INT64(5)]
	//        [INT32(3) INT32(4) INT64(4)]
	// b的结果为随机，order by b会出现两边结果不一致的情况
	_, err := mcmp.ExecAndIgnore("select a, b, count(*) from t_user group by a order by b")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select a, count(*) from t_user group by a order by a")
	// scatter aggregate order by references aggregate expression
	// results mismatched.
	//        Vitess Results:
	//        [INT32(3) INT32(4) INT64(4)]
	//        [INT32(2) INT32(2) INT64(5)]
	//        [INT32(1) INT32(2) INT64(6)]
	//        MySQL Results:
	//        [INT32(3) INT32(4) INT64(4)]
	//        [INT32(2) INT32(3) INT64(5)]
	//        [INT32(1) INT32(2) INT64(6)]
	// b的结果为随机，会出现两边结果不一致的情况
	_, err = mcmp.ExecAndIgnore("select a, b, count(*) k from t_user group by a order by k")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select a, count(*) k from t_user group by a order by k")
	// select a, b, count(*) k from t_user group by a order by b, a, k
	// results mismatched.
	//        Vitess Results:
	//        [INT32(1) INT32(2) INT64(6)]
	//        [INT32(2) INT32(2) INT64(5)]
	//        [INT32(3) INT32(4) INT64(4)]
	//        MySQL Results:
	//        [INT32(1) INT32(2) INT64(6)]
	//        [INT32(2) INT32(3) INT64(5)]
	//        [INT32(3) INT32(4) INT64(4)]
	// b的结果为随机，order by b, a, k会出现两边结果不一致的情况
	_, err = mcmp.ExecAndIgnore("select a, b, count(*) k from t_user group by a order by b, a, k")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select a, count(*) k from t_user group by a order by b, a, k")
	// scatter aggregate with memory sort and limit
	//results mismatched.
	//        Vitess Results:
	//        [INT32(1) INT32(2) INT64(6)]
	//        [INT32(2) INT32(2) INT64(5)]
	//        [INT32(3) INT32(4) INT64(4)]
	//        MySQL Results:
	//        [INT32(1) INT32(2) INT64(6)]
	//        [INT32(2) INT32(3) INT64(5)]
	//        [INT32(3) INT32(4) INT64(4)]
	// b的结果为随机，会出现两边结果不一致的情况
	_, err = mcmp.ExecAndIgnore("select a, b, count(*) k from t_user group by a order by k desc limit 10")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select a, count(*) k from t_user group by a order by k desc limit 10")
	// scatter aggregate with memory sort and order by number
	// results mismatched.
	//        Vitess Results:
	//        [INT32(1) INT32(2) INT64(6)]
	//        [INT32(2) INT32(2) INT64(5)]
	//        [INT32(3) INT32(4) INT64(4)]
	//        MySQL Results:
	//        [INT32(1) INT32(2) INT64(6)]
	//        [INT32(2) INT32(3) INT64(5)]
	//        [INT32(3) INT32(4) INT64(4)]
	// b的结果为随机，会出现两边结果不一致的情况
	_, err = mcmp.ExecAndIgnore("select a, b, count(*) k from t_user group by a order by 1,3")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select a, count(*) k from t_user group by a order by 1,2")
	// scatter aggregate with memory sort and order by number, reuse weight_string # we have to use a meaningless construct to test this
	mcmp.ExecWithColumnCompare("select textcol1 as t, count(*) k from t_user group by textcol1 order by textcol1, k, textcol1")
	// unary expression
	mcmp.ExecWithColumnCompare("select a from t_user order by binary a desc")
	// intcol order by
	mcmp.ExecWithColumnCompare("select id, intcol from t_user order by intcol")
	// Scatter-Scatter order by with order by column not present
	mcmp.ExecWithColumnCompare("select col from t_user order by id")
	// EqualUnique Select, scatter aggregate order by references ungrouped column
	mcmp.ExecWithColumnCompare("select a, b, count(*) from t_user where id = 1024 group by a order by b")
	// scatter aggregate order by references ungrouped column, table index EqualUnique
	// results mismatched.
	//        Vitess Results:
	//        [INT32(2) INT32(2) INT64(4)]
	//        [INT32(1) INT32(2) INT64(4)]
	//        [INT32(3) INT32(4) INT64(2)]
	//        MySQL Results:
	//        [INT32(1) INT32(2) INT64(4)]
	//        [INT32(2) INT32(3) INT64(4)]
	//        [INT32(3) INT32(4) INT64(2)]
	// b的结果为随机，会出现两边结果不一致的情况
	_, err = mcmp.ExecAndIgnore("select a, b, count(*) from t_user where col = 1024 group by a order by b")
	require.NoError(t, err)
	mcmp.ExecWithColumnCompare("select a, count(*) from t_user where col = 1024 group by a order by a")
	// vindex EqualUnique, table index EqualUnique
	mcmp.ExecWithColumnCompare("select a, b, count(*) from t_user where col = 1024 and id = 100865 group by a order by b")
	// order by on a cross-shard query. Note: this happens only when an order by column is from the second table
	mcmp.ExecWithColumnCompare("select t_user.col1 as a, t_user.col2 b, t_music.bar c from t_user, t_music where t_user.id = t_music.id and t_user.id = 1 order by c")
	// Order by for join, with mixed cross-shard ordering
	mcmp.ExecWithColumnCompare("select t_user.col1 as a, t_user.col2, t_music.bar from t_user join t_music on t_user.id = t_music.id where t_user.id = 1 order by 1 asc, 3 desc, 2 asc")
	// unary expression in join query
	mcmp.ExecWithColumnCompare("select u.a from t_user u join t_music m on u.a = m.a order by binary u.a desc")
}
