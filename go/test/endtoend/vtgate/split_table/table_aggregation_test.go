package split_table

import (
	"testing"
)

func TestTableGroupBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")

	mcmp.ExecWithColumnCompare("select id, count(*) k from t_user group by id")

	mcmp.ExecWithColumnCompare("select id,col, count(*) k from t_user group by id,col")

	mcmp.ExecWithColumnCompare("select id,f_key, count(*) k from t_user group by id,f_key")

	mcmp.ExecWithColumnCompare("select col,f_key, count(*) k from t_user group by col,f_key")

}

func testTableAggOnTopOfLimit(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")

	mcmp.AssertMatches("select  count(*)  from (select id,col from t_user where col='a' limit 2) as x", "[[INT64(1)]]")
	mcmp.AssertMatches("select  count(col)  from (select id,col from t_user where col='a' order by col desc limit 2) as x", "[[INT64(1)]]")
	mcmp.AssertMatches("select  count(col)  from (select id,col from t_user where col is not null  limit 2) as x", "[[INT64(2)]]")
	mcmp.AssertMatches("select  count(id)  from (select id,col from t_user where col is not null  limit 2) as x", "[[INT64(2)]]")
	//VT13001: [BUG] GROUP BY on: *planbuilder.simpleProjection (errno 1815) (sqlstate HY000)
	mcmp.AssertMatches("select  count(id)  from (select id,col from t_user where col is not null  limit 2) as x group by id", "[[INT64(2)]]")

}

func TestTableOrderByCount(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")

	//syntax error at position 63 near 'DESC' (errno 1105) (sqlstate HY000) during query: select  t_user.id  from  t_user GROUP BY COUNT(t_user.id) DESC
	//mcmp.AssertMatches("select  t_user.col  from  t_user GROUP BY COUNT(t_user.col) DESC", "[[INT64(1)]]")

}
