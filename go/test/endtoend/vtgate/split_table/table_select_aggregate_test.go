package split_table

import (
	"testing"
)

func TestTableAggregate(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,name) values (1, 'a', 'aaa', 1, false,'test'),(2, 'b', 'bbb', 2, false,'test'),(3, 'c', 'ccc', 3, false,'test')")

	mcmp.ExecWithColumnCompare("select count(*) as user_count  from t_user")
	mcmp.ExecWithColumnCompare("select count(*) as user_count  from t_user where id > 1-1")
	mcmp.ExecWithColumnCompare("select count(*) as user_count  from t_user where id > 1 - 1")
	mcmp.ExecWithColumnCompare("select max(col) as user_count  from t_user ")
	mcmp.ExecWithColumnCompare("select min(col) as user_count  from t_user ")
	//        	            	VT12001: unsupported: in scatter query: aggregation function 'avg(f_tinyint) as user_count' (errno 1235) (sqlstate 42000) during query: select avg(f_tinyint) as user_count  from t_user
	//mcmp.ExecWithColumnCompare("select avg(f_tinyint) as user_count  from t_user ")

	mcmp.ExecWithColumnCompare("select COUNT(`f_tinyint`) as user_count  from t_user ")

	mcmp.ExecWithColumnCompare("select max(f_tinyint) as user_count  from t_user ")
	mcmp.ExecWithColumnCompare("select min(f_tinyint) as user_count  from t_user ")
	//VT12001: unsupported: in scatter query: aggregation function 'avg(f_tinyint) as user_count' (errno 1235) (sqlstate 42000) during query: select avg(f_tinyint) as user_count  from t_user
	//mcmp.ExecWithColumnCompare("select avg(f_tinyint) as user_count  from t_user ")
	mcmp.ExecWithColumnCompare("select sum(f_tinyint) as user_count  from t_user ")
	mcmp.ExecWithColumnCompare("select count(f_tinyint) as user_count  from t_user ")

	mcmp.ExecWithColumnCompare("SELECT sum(if(f_tinyint=0, 1, 0)) func_status FROM t_user WHERE id = 1 AND col = 'a'")

	mcmp.ExecWithColumnCompare("select count(0) from t_user t where t.name like concat('%%','test','%%') and t.id in(1,2) and t.f_tinyint between 1 and 2")

}
