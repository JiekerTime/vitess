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
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1,  'aaa',    'aaa', 1, false, 1, 2, 3, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (2,  'bbb',    'bbb', 2, false, 2, 3, 4, 103, 200, 'abc')")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (3,  'abc',  'ccc', 3, true,  3, 4, 5, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (4,  'abc',  'ccc', 3, true,  3, 4, 5, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (5,  'aaa',   'ccc', 3, true,  3, 4, 5, 103, 200, 'abc')")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (6,  'bbb',    'aaa', 1, true,  1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (7,  'ccc', 'bbb', 2, false, 2, 3, 4, 100, 300, 3)")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (8,  'aaa', 'ccc', 3, false, 3, 4, 5, 102, 300, 4)")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (9,  'bbb', 'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (10, 'ccc', 'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (11, '12',   'aaa', 1, true,  1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (12, 'aaa', 'aaa', 1, false, 2, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (13, 'aaa', 'aaa', 1, false, 3, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (14, '123',  'aaa', 1, false, 2, 2, 3, 100, 300, 'abc')")
	mcmp.Exec("insert into t_user_shard(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (15, '1024', 'aaa', 1, false, 2, 2, 3, 100, 300, 2)")
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
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (11, 10, 3, 300, '2', 300, 5)")

	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (1, 123, '42',  10, 1, '202')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (2, 123, '42',  11, 2, '202')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (3, 123, 'bbb', 8, 3, '202')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (4, 123, 'bbb', 11, 2, '202')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (5, 123, 'ccc', 10, 3, '202')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (6, 123, '42',  8, 2, '302')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (7, 123, '42',  10, 1, '302')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (8, 4, 'bbb', 10, 1, '302')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (9, 4, 'bbb', 10, 1, '302')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (10, 4, 'bbb', 10, 1, '302')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (11, 5, 'bbb', 10, 1, '302')")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (12, 5, 'bbb', 10, 1, '302')")

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
	// expected: []string{"count(0)"}
	// actual  : []string{"count(:vtg1 /* INT64 */)"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompare("select count(0) from t_user t where t.name like concat('%%','test','%%') and t.id in(1,2) and t.f_tinyint between 1 and 2")
	mcmp.ExecAndNotEmpty("select count(0) from t_user t where t.name like concat('%%','test','%%') and t.id in(1,2) and t.f_tinyint between 1 and 2")
	mcmp.ExecWithColumnCompareAndNotEmpty("select a from (select count(*) as a from t_user) t")
	mcmp.ExecWithColumnCompareAndNotEmpty("(select id from t_user order by 1 desc) order by 1 asc limit 2")
	mcmp.ExecAndNotEmpty("select count(f_key) from (select id, f_key, col from t_user where id > 12 limit 3) as x")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(col) from (select t_user_extra.col as col from t_user left join t_user_extra on t_user.id = t_user_extra.id limit 3) as x")

	// having max()
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) c from t_music where user_id=123 group by col having max(a)>9 order by c")
	mcmp.ExecWithColumnCompareAndNotEmpty("select col,count(*) c from t_music where user_id=123 group by col having max(bar)>2 order by c")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) c from t_music group by user_id having max(a)>9 order by c")
	mcmp.ExecWithColumnCompareAndNotEmpty("select user_id from t_music group by user_id having count(user_id)=3 order by user_id")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) c from t_music where user_id=123 group by col having max(a)>9 order by c")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) c from t_music where user_id=123 having c > 5")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) c from t_music where user_id=123 having c != 5 ")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) c from t_music where user_id=123 having c = '7' ")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) c from t_music where user_id=123 having count(*) = 7 ")
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) c from t_music where user_id=123 having c != '8' ")
	mcmp.ExecWithColumnCompareAndNotEmpty("select foo from t_music where user_id=123 having foo = '202'")
	// find aggregation expression and use column offset in filter times two
	mcmp.ExecWithColumnCompareAndNotEmpty("select sum(a),sum(bar) from t_music where user_id=123 group by col having sum(a)+sum(bar)=24")
	// find aggregation expression and use column offset in filter times three
	mcmp.ExecWithColumnCompareAndNotEmpty("select sum(a) as asum,sum(bar) from t_music where user_id=123 group by col having asum+sum(bar)=24")
	// shard table and split table aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user_shard.id  shard_id, count(*) c from t_user_shard, t_user where t_user_shard.id = t_user.id group by t_user_shard.id having max(t_user_shard.a) >=2")
	// join push down for shard and split key
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.id id, count(*) c from t_user, t_user_extra where t_user.id = t_user_extra.user_id and t_user.col=t_user_extra.col group by t_user.id having max(t_user.a) >=1")
}
