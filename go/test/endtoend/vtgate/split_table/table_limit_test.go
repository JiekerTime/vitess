package split_table

import (
	"testing"
)

func TestLimit(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	//因为分表会在plan层依赖库名这里要加个use ks语句
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")

	mcmp.ExecWithColumnCompare("select * from t_user where name ='abc' AND (id = 4) and (col = 123) limit 5")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 4) AND (name ='abc') AND (col = 'abc') limit 5")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 4 AND name ='abc' AND col = 'abc') limit 5")
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 1 and col = 3 order by user0_.col desc limit 2")
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 1 and col = 12 order by col0_ desc limit 3")
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 1 order by user0_.col desc limit 2")
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 1 order by col0_ desc limit 3")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 1) and (col = 12) AND name = true limit 5")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 1) and (col = 12) AND name limit 5")
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 5) and (col = 12) AND name = true limit 5")

}
