package split_table

import (
	"testing"
)

func TestTableDelete(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	//因为分表会在plan层依赖库名这里要加个use ks语句
	mcmp.Exec("use user")
	// table_dml_cases.json

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false),(3, 'c', 'ccc', 3, false)")
	mcmp.ExecWithColumnCompare("delete from t_user")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false)")
	mcmp.ExecWithColumnCompare("delete from t_user where id = 1 ")

	mcmp.Exec("insert into t_3(f_shard,f_table) values (1,2),(1,2),(2,3)")
	mcmp.ExecWithColumnCompare("delete from t_3 where f_shard=1")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false),(3, 'c', 'ccc', 3, false)")
	mcmp.ExecWithColumnCompare("delete from t_user where id in (1,2,3)")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false), (2, 'a', 'aaa', 1, false),(3, 'a', 'aaa', 1, false)")
	mcmp.ExecWithColumnCompare("delete from t_user where col = 'a'")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")
	mcmp.ExecWithColumnCompare("delete from t_user where col in ('a','b')")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false)")
	mcmp.ExecWithColumnCompare("delete from t_user where id =1 and col ='a'")

	mcmp.Exec("insert into t_3(f_shard,f_table) values (1,2),(1,2),(2,2)")
	mcmp.ExecWithColumnCompare("delete from t_3 where f_shard=1 and f_table=2")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'b', 'bbb', 2, false)")
	mcmp.ExecWithColumnCompare("delete from t_user where id +1 =2")

	mcmp.Exec("insert into t_user(id,col,f_key,a,b) values (1, 'b', 'bbb', 1, 1)")
	mcmp.ExecWithColumnCompare("delete u.* from t_user u where u.id * u.a = u.b")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'b', 'bbb', 1, false)")
	mcmp.ExecWithColumnCompare("delete u from t_user u where u.f_tinyint =1")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")
	mcmp.ExecWithColumnCompare("delete from t_user where (f_key, col) in (('aaa', 'a'), ('bbb', 'b'))")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")
	mcmp.ExecWithColumnCompare("delete from t_user where id between 1 and 2")

	//unsupported: multi split tables DELETE with LIMIT
	//mcmp.ExecWithColumnCompare("delete from t_user where id = 5 limit 5")

	mcmp.Exec("insert into t_6(f_shard,f_table) values (1, 1),(2, 2),(3,3)")
	mcmp.ExecWithColumnCompare("delete from t_6")

	mcmp.Exec("insert into t_6(f_shard,f_table) values (1, 1),(2, 2),(3,3)")
	mcmp.ExecWithColumnCompare("delete from t_6 where f_shard=1")
	mcmp.ExecWithColumnCompare("delete from t_6")

	mcmp.Exec("insert into t_6(f_shard,f_table) values (1, 1),(2, 2),(3,3)")
	mcmp.ExecWithColumnCompare("delete from t_6 where f_shard in (1, 2, 3)")

	mcmp.Exec("insert into t_8(id,f_shard_table,f_int) VALUES (1,'1',1)")
	mcmp.ExecWithColumnCompare("delete from t_8  where f_shard_table='1'")

}
