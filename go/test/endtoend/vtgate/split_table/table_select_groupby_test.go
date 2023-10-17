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
