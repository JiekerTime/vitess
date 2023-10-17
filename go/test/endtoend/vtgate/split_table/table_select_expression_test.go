package split_table

import (
	"testing"
)

func TestTableExpression(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,name) values (1, 'a', 'aaa', 1, false,'test'),(2, 'b', 'bbb', 2, false,'test')")

	mcmp.ExecWithColumnCompare("select * from t_user t where t.name regexp 'test' and t.col in('a','b') ")
	mcmp.ExecWithColumnCompare("select f_tinyint + 1 * 2 as exp from t_user t  ")

}
