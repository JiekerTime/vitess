package split_table

import (
	"testing"
)

func TestTableDelete(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	//因为分表会在plan层依赖库名这里要加个use ks语句
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")

	// table_dml_cases.json
	mcmp.ExecWithColumnCompare("delete from t_user")
	mcmp.ExecWithColumnCompare("delete from t_user where col = 'a'")
	//transaction rolled back to reverse changes of partial DML execution: VT13001: [BUG] unexpected 'autocommitted' state in transaction (errno 1815)
	//mcmp.ExecWithColumnCompare("delete from t_user where id = 1 ")
	mcmp.ExecWithColumnCompare("delete from t_user where id =1 and col ='a'")

	//
	//mcmp.ExecWithColumnCompare("delete from t_user where id in (1,2)")
	mcmp.ExecWithColumnCompare("delete from t_user where col in('a','b')")
	mcmp.ExecWithColumnCompare("delete from t_user where id +1 =2")
	mcmp.ExecWithColumnCompare("delete from t_user where id +1 =2")

	mcmp.ExecWithColumnCompare("delete u.* from t_user u where u.id * u.col = u.foo")
	// target: user.-80.primary: vttablet: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'as u where u.f_tinyint = 1 limit 10001 /* INT64 */' at line 1 (errno 1064) (sqlstate 42000) (CallerID: userData1): Sql: "delete from t_user_0 as u where u.f_tinyint = :u_f_tinyint /* INT64 */", BindVars: {#maxLimit: "type:INT64 value:\"10001\""u_f_tinyint: "type:INT64 value:\"1\""}
	//mcmp.ExecWithColumnCompare("delete from t_user u where u.f_tinyint =1")
	mcmp.ExecWithColumnCompare("delete from t_user where (f_key, col) in (('aa', 'bb'), ('cc', 'dd'))")
	mcmp.ExecWithColumnCompare("delete from t_user where id between 1 and 2")
	//
	mcmp.ExecWithColumnCompare("delete from t_user where col = 'jose'")
	//
	//mcmp.ExecWithColumnCompare("delete from t_user where id = 5 limit 5")

}
