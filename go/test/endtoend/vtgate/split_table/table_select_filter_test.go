package split_table

import (
	"testing"
)

func TestOne(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	//因为分表会在plan层依赖库名这里要加个use ks语句
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,someColumn,name) values (1, 'a', 'aaa', 1, false,null,'a'),(2, 'b', 'bbb', 2, false,null,'b'),(3, 'c', 'ccc', 3, false,'test','c'),(4, '1', 'ccc', 3, false,'test','c'),(5, 'a', 'aaa', 1,false,'test', '\\'')")

	// table_filter_cases.json
	mcmp.ExecWithColumnCompare("select id from t_user")
	mcmp.ExecWithColumnCompare("select id from t_user where someColumn = null")
	mcmp.ExecWithColumnCompare("SELECT id from t_user where someColumn <=> null")
	mcmp.ExecWithColumnCompare("select id from t_user where (col, name) in (('b', 'b')) and id = 2")

	mcmp.ExecWithColumnCompare("select Id from t_user where 1 in ('1','aa', 'bb')")
	mcmp.ExecWithColumnCompare("select id from t_user where name in (col, 'c')")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = 2")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = 1+2")

	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = 'b' and t_user.id in (1, 2)")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = case t_user.col when 'a' then true else false end and t_user.id in (1, 2)")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = 'a' and t_user.id in (1, 2) and t_user.name = 'a' and t_user.id = 1")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = 1 and t_user.name = 'a' and t_user.id in (1, 2) and t_user.col = 'a'")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = 1 or t_user.name = 'a' and t_user.id in (1, 2)")
	mcmp.ExecWithColumnCompare("select id from t_user where database()")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id > 1")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.name = t_user.col and t_user.col = 'a'")
	mcmp.ExecWithColumnCompare("select id from t_user where (id, name) = (1, '')")
	mcmp.ExecWithColumnCompare("select col from t_user where id = 1 or id = 2")
	mcmp.ExecWithColumnCompare("select col from t_user where id = 1 or id = 2 or id = 3")
	mcmp.ExecWithColumnCompare("select col from t_user where (id = 1 or id = 2) or (id = 3 or id = 4)")

	mcmp.ExecWithColumnCompare("select a+2 as a from t_user having a = 42")
	mcmp.ExecWithColumnCompare("select t_user.col + 2 as a from t_user having a = 42")
	mcmp.ExecWithColumnCompare("select id from t_user where (id = 1 and name ='a') or (id = 2 and someColumn = 'test')")
	mcmp.ExecWithColumnCompare("select id from t_user where (id = 1 and name ='a') or (id = 3 and name = 'c')")

	mcmp.ExecWithColumnCompare("select textcol1 from t_user where f_tinyint = 2 and t_user.f_tinyint = 2")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = 'a'")
	mcmp.ExecWithColumnCompare("select id from t_user where 1 = col")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col in (1)")

	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col in ('a', 'b', 'c')")

	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col in ('a', 'b', 'c') and col = 'a'")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col is null")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = null")

	mcmp.ExecWithColumnCompare("select id from t_user where not (not col = '3')")
	mcmp.ExecWithColumnCompare("select id from t_user where (col in ('1', '5') and B or C and col in ('5', '7'))")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = 1+0")
	mcmp.ExecWithColumnCompare("select id from t_user where id = 1 and t_user.col = 1+0")

	//        	            	target: user.80-.primary: vttablet: rpc error: code = InvalidArgument desc = Unknown table 't_user_0' (errno 1051) (sqlstate 42S02) (CallerID: userData1): Sql: "select t_user_0.* from t_user_0 as t_user where col = :col /* INT64 */ and id = :id /* INT64 */", BindVars: {#maxLimit: "type:INT64 value:\"10001\""col: "type:INT64 value:\"5\""id: "type:INT64 value:\"1\""} (errno 1051) (sqlstate 42S02) during query: select t_user.* from t_user t_user where col = 5 and id = 1
	//mcmp.ExecWithColumnCompare("select t_user.* from t_user t_user where col = 5 and id = 1")
	//        	            	target: user.80-.primary: vttablet: rpc error: code = NotFound desc = Unknown column 't_user_0.id' in 'field list' (errno 1054) (sqlstate 42S22) (CallerID: userData1): Sql: "select t_user_0.id, `name` from t_user_0 as t_user where t_user_0.id = :t_user_id /* INT64 */ and col = :col /* VARCHAR */", BindVars: {#maxLimit: "type:INT64 value:\"10001\""col: "type:VARCHAR value:\"1\""t_user_id: "type:INT64 value:\"3\""} (errno 1054) (sqlstate 42S22) during query: SELECT t_user.id,name FROM t_user t_user WHERE t_user.id = 3 AND col = '1'
	//mcmp.ExecWithColumnCompare("SELECT t_user.id,name FROM t_user t_user WHERE t_user.id = 3 AND col = '1'")

	mcmp.ExecWithColumnCompare("SELECT * FROM t_user  WHERE id = 3 AND col = '1'")

	mcmp.ExecWithColumnCompare("SELECT * FROM t_user  WHERE col = '1' AND col = '1'")

	mcmp.ExecWithColumnCompare("SELECT * /* this is &#x000D;&#x000A; block comment */ FROM /* this is another &#x000A; block comment */ t_user where name='1'")

	mcmp.ExecWithColumnCompare("SELECT * FROM t_user where name='\\'' ")

	mcmp.ExecWithColumnCompare("SELECT name as 'name' FROM t_user")

	mcmp.ExecWithColumnCompare("SELECT INTERVAL(name,1,5) func_status FROM t_user WHERE id = 3 AND col = '1'")

}
