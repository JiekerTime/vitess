package split_table

import (
	"testing"
)

func TestTableAggregate(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,name) values (1, 'a', 'aaa', 1, false,'test'),(2, 'b', 'bbb', 2, false,'test'),(3, 'c', 'ccc', 3, false,'test')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (4,  '5', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (5,  '6', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (6,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (7,  '2', 'bbb', 2, false, 2, 3, 4, 100,  300,  3 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (8,  '2', 'ccc', 3, false, 3, 4, 5, 102,  300,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (9,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (10, '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (1,  1, 2, 200, '1', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (2,  2, 4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (3,  3, 4, 200, '5', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (4,  4, 4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (5,  2, 2, 5,   '2', 5  , 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (6,  2, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (7,  2, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (8,  8, 5, 300, '4', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (9,  9, 3, 300, '5', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (10, 5, 3, 300, '4', 300, 5)")

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
	mcmp.ExecWithColumnCompareAndNotEmpty("select a from (select count(*) as a from t_user) t")
	mcmp.ExecWithColumnCompareAndNotEmpty("(select id from t_user order by 1 desc) order by 1 asc limit 2")
	mcmp.ExecWithColumnCompareAndNotEmpty("select sum(col) from (select t_user.col as col, 32 from t_user join t_user_extra) t")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(f_key) from (select id, f_key, col from t_user where id > 12 limit 3) as x")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(col) from (select t_user_extra.col as col from t_user left join t_user_extra on t_user.id = t_user_extra.id limit 3) as x")
}
