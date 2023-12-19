package split_table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableUnion(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo, f_int) values (1, '45', 'aaa', 1, false,1,2,3,100, 200, 10)")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo, f_int) values (2, 'b',  'bbb', 2, false,2,3,4,1030,200, 20)")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo, f_int) values (3, 'c',  'ccc', 3, false,3,4,5,100, 200, 30)")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo, f_int) values (5, '45', 'ccc', 4, false,3,4,5,1030,200, 11)")
	mcmp.Exec("insert into t_1(f_shard,f_table,f_int) values (1,1,1),(2,2,2),(3,3,3),(4,4,4)")
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
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (3,  12, 'AB',  10, 3, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (4,  13, 'bbb', 10, 2, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (5,  12, 'A1B', 10, 3, 202);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (6,  11, '42',  10, 2, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (7,  10, '42',  10, 1, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (8,  12, 'bbb', 10, 1, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (9,  13, 'bbb', 10, 1, 302);")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (10, 13, 'A1B', 10, 1, 302);")

	// union all between two SelectEqualUnique
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where id = 1  union all select id from t_user where id = 5")
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where id = 1  and col='45' union all select id from t_user where id = 5 and col='45'")

	// almost dereks query - two queries with order by and limit being scattered to two different sets of tablets
	mcmp.ExecWithColumnCompareAndNotEmpty("(SELECT id FROM t_user ORDER BY id DESC LIMIT 1) UNION ALL (SELECT id FROM t_1 ORDER BY id DESC LIMIT 1)")
	mcmp.ExecWithColumnCompareAndNotEmpty("(SELECT f_int FROM t_user ORDER BY f_tinyint DESC LIMIT 1) UNION ALL (SELECT f_int FROM t_1 ORDER BY f_table DESC LIMIT 1)")

	// union all between two scatter selects, with order by
	mcmp.ExecWithColumnCompareAndNotEmpty("(select id from t_user order by id limit 5) union all (select id from t_1 order by id desc limit 5)")
	mcmp.ExecWithColumnCompareAndNotEmpty("(select f_int from t_user order by intcol limit 5) union all (select f_int from t_1 order by f_table desc limit 5)")

	// unmergable because we are using aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) as s from t_user union select count(*) as s from t_1")

	// derived table with union
	mcmp.ExecWithColumnCompareAndNotEmpty("select tbl2.id FROM ((select id from t_user order by id limit 5) union all (select id from t_user order by id desc limit 5)) as tbl1 INNER JOIN t_user as tbl2  ON tbl1.id = tbl2.id")
	mcmp.ExecWithColumnCompareAndNotEmpty("select tbl2.id FROM ((select id from t_user order by col limit 5) union all (select id from t_user order by col desc limit 5)) as tbl1 INNER JOIN t_user as tbl2  ON tbl1.id = tbl2.id")

	// union all with group by
	mcmp.ExecWithColumnCompareAndNotEmpty("(SELECT id FROM t_user group by id ORDER BY id DESC LIMIT 1 ) UNION ALL (SELECT id FROM t_1 ORDER BY id DESC LIMIT 1)")

	// derived table with union and group by
	mcmp.ExecWithColumnCompareAndNotEmpty("(SELECT id FROM t_user group by id ORDER BY id DESC LIMIT 1 ) UNION ALL (SELECT id FROM t_music ORDER BY col DESC LIMIT 1)")
	// union all between two scatter selects
	_, err := mcmp.ExecAndIgnore("select id from t_user union all select id from t_music")
	require.ErrorContains(t, err, "VT12001: unsupported: statement type *sqlparser.Union in split table")
	// union distinct between two scatter selects
	_, err = mcmp.ExecAndIgnore("select id from t_user union select id from t_music")
	require.ErrorContains(t, err, "VT12001: unsupported: unable to use: *sqlparser.DerivedTable in split table")
	// Union all
	_, err = mcmp.ExecAndIgnore("select col1, col2 from t_user union all select col1, foo from t_user_extra")
	require.ErrorContains(t, err, "VT12001: unsupported: statement type *sqlparser.Union in split table")
	// union operations in subqueries (FROM)
	_, err = mcmp.ExecAndIgnore("select * from (select * from t_5 union all select * from t_6) as t")
	require.ErrorContains(t, err, "VT12001: unsupported: unable to use: *sqlparser.DerivedTable in split table")
	// union with different target shards
	//mcmp.ExecWithColumnCompareAndNotEmpty("select 1 from t_music where id = 1 union select 1 from t_music where id = 2")
	// union distinct between a scatter query and a join (other side)
	mcmp.ExecWithColumnCompareAndNotEmpty("(select t_user.textcol1, t_user.name from t_user join t_user_extra where t_user_extra.extra_id = '3') union select 'b','c' from t_user")
	// union distinct between a scatter query and a join (other side)
	mcmp.ExecWithColumnCompareAndNotEmpty("select 'b','c' from t_user union (select t_user.textcol1, t_user.name from t_user join t_user_extra where t_user_extra.extra_id = '3')")
}
