package split_table

import (
	"testing"
)

func TestLimit(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	//因为分表会在plan层依赖库名这里要加个use ks语句
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,name) values (1, 'a', 'aaa', 1, false,'a'),(2, 'b', 'bbb', 2, false,'b'),(3, 'c', 'ccc', 3, false,'c'),(4, 'd', 'ddd', 4, false,'d'),(5, 'e', 'eee', 5, false,'e')")

	mcmp.ExecWithColumnCompare("select * from t_user where name ='c' AND (id = 3) and (col = 'c') limit 5")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 4) AND (name ='c') AND (col = 'b') limit 5")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 2 AND name ='b' AND col = 'a') limit 5")
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 1 and col = 'a' order by user0_.col desc limit 2")
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 2 and col = 'b' order by col0_ desc limit 3")
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 3 order by user0_.col desc limit 2")
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 4 order by col0_ desc limit 3")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 1) and (col = 'a') AND name = true limit 5")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 2) and (col = 'b') AND name limit 5")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 5) and (col = 'e') AND name = true limit 5")

	mcmp.AssertMatches("select  count(*)  from (select id,col from t_user where col='a' limit 2) as x", "[[INT64(1)]]")
	mcmp.AssertMatches("select  count(col)  from (select id,col from t_user where col='a' order by col desc limit 2) as x", "[[INT64(1)]]")
	mcmp.AssertMatches("select  count(col)  from (select id,col from t_user where col is not null  limit 2) as x", "[[INT64(2)]]")
	mcmp.AssertMatches("select  count(id)  from (select id,col from t_user where col is not null  limit 2) as x", "[[INT64(2)]]")
	//VT13001: [BUG] GROUP BY on: *planbuilder.simpleProjection (errno 1815) (sqlstate HY000)
	//mcmp.AssertMatches("select  count(id)  from (select id,col from t_user where col is not null  limit 2) as x group by id", "[[INT64(2)]]")

	mcmp.ExecWithColumnCompare("select col, count(*) from t_user group by col limit 10")

	mcmp.ExecWithColumnCompare("select id  from t_user group by id order by id limit 1")

	mcmp.ExecWithColumnCompare("select id ,sum(f_tinyint) from t_user group by id order by sum(f_tinyint) limit 1")

}
