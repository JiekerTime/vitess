package split_table

import (
	"testing"
)

func TestSelectForUpdate(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")

	mcmp.ExecWithColumnCompare("select * from t_user where id =2 FOR UPDATE")
	mcmp.ExecWithColumnCompare("select col from t_user for update")
}
