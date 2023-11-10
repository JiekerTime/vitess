package split_table

import (
	"testing"
)

func TestTableUnion(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,a,b,c,intcol,foo) values (1, '45', 'aaa', 1, false,1,2,3,100,200),(2, 'b', 'bbb', 2, false,2,3,4,1030,200),(3, 'c', 'ccc', 3, false,3,4,5,100,200),(5, '45', 'ccc', 3, false,3,4,5,1030,200)")
	mcmp.Exec("insert into t_1(f_shard,f_table) values (1,1),(2,2),(3,3),(4,4)")

	//// union all between two SelectEqualUnique
	mcmp.ExecWithColumnCompare("select id from t_user where id = 1  union all select id from t_user where id = 5")
	mcmp.ExecWithColumnCompare("select id from t_user where id = 1  and col='45' union all select id from t_user where id = 5 and col='45'")

	// almost dereks query - two queries with order by and limit being scattered to two different sets of tablets
	mcmp.ExecWithColumnCompare("(SELECT id FROM t_user ORDER BY id DESC LIMIT 1) UNION ALL (SELECT id FROM t_1 ORDER BY id DESC LIMIT 1)")
	mcmp.ExecWithColumnCompare("(SELECT id FROM t_user ORDER BY col DESC LIMIT 1) UNION ALL (SELECT id FROM t_1 ORDER BY f_table DESC LIMIT 1)")

	// union all between two scatter selects, with order by
	mcmp.ExecWithColumnCompare("(select id from t_user order by id limit 5) union all (select id from t_1 order by id desc limit 5)")
	mcmp.ExecWithColumnCompare("(select id from t_user order by col limit 5) union all (select id from t_1 order by f_table desc limit 5)")

	// unmergable because we are using aggregation
	mcmp.ExecWithColumnCompare("select count(*) as s from t_user union select count(*) as s from t_1")

	// derived table with union
	mcmp.ExecWithColumnCompare("select tbl2.id FROM ((select id from t_user order by id limit 5) union all (select id from t_user order by id desc limit 5)) as tbl1 INNER JOIN t_user as tbl2  ON tbl1.id = tbl2.id")
	mcmp.ExecWithColumnCompare("select tbl2.id FROM ((select id from t_user order by col limit 5) union all (select id from t_user order by col desc limit 5)) as tbl1 INNER JOIN t_user as tbl2  ON tbl1.id = tbl2.id")

	// union all with group by
	mcmp.ExecWithColumnCompare("(SELECT id FROM t_user group by id ORDER BY id DESC LIMIT 1 ) UNION ALL (SELECT id FROM t_1 ORDER BY id DESC LIMIT 1)")
}
