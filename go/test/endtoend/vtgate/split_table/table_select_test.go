package split_table

import (
	"testing"
)

func TestSelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	//因为分表会在plan层依赖库名这里要加个use ks语句
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")
	mcmp.ExecWithColumnCompare("select id,col,f_key,f_tinyint,f_bit from t_user")

	// table_select_case.json
	mcmp.ExecWithColumnCompare("select 1 from t_user")
	mcmp.ExecWithColumnCompare("select t_user.* from t_user")
	mcmp.ExecWithColumnCompare("select * from t_user")
	mcmp.ExecWithColumnCompare("select t_user.* from t_user")
	mcmp.ExecWithColumnCompare("select user.t_user.* from user.t_user")

}
