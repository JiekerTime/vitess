package split_table

import (
	"testing"
)

func TestOne(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	//因为分表会在plan层依赖库名这里要加个use ks语句
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)")

	// table_filter_cases.json
	mcmp.ExecWithColumnCompare("select id from t_user")
	mcmp.ExecWithColumnCompare("select id from t_user where someColumn = null")
	mcmp.ExecWithColumnCompare("SELECT id from t_user where someColumn <=> null")
	mcmp.ExecWithColumnCompare("select id from t_user where (col, name) in (('aa', 'bb')) and id = 5")

	//Operand should contain 2 column(s) (errno 1241) (sqlstate 21000)
	//mcmp.ExecWithColumnCompare("select id from t_user where ((col1, name), col2) in (('aa', 'bb', 'cc'), (('dd', 'ee'), 'ff'))")
	mcmp.ExecWithColumnCompare("select Id from t_user where 1 in ('aa', 'bb')")
	mcmp.ExecWithColumnCompare("select id from t_user where name in (col, 'bb')")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = 5")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = 5+5")

	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = 5 and t_user.id in (1, 2)")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = case t_user.col when 'foo' then true else false end and t_user.id in (1, 2)")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = 5 and t_user.id in (1, 2) and t_user.name = 'aa' and t_user.id = 1")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = 1 and t_user.name = 'aa' and t_user.id in (1, 2) and t_user.col = 5")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = 1 or t_user.name = 'aa' and t_user.id in (1, 2)")
	mcmp.ExecWithColumnCompare("select id from t_user where database()")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id > 5")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.id = t_user.col and t_user.col = 5")
	mcmp.ExecWithColumnCompare("select id from t_user where (id, name) = (34, 'apa')")
	mcmp.ExecWithColumnCompare("select col from t_user where id = 1 or id = 2")
	mcmp.ExecWithColumnCompare("select col from t_user where id = 1 or id = 2 or id = 3")
	mcmp.ExecWithColumnCompare("select col from t_user where (id = 1 or id = 2) or (id = 3 or id = 4)")
	mcmp.ExecWithColumnCompare("select a+2 as a from t_user having a = 42")
	mcmp.ExecWithColumnCompare("select t_user.col + 2 as a from t_user having a = 42")
	mcmp.ExecWithColumnCompare("select id from t_user where (id = 5 and name ='apa') or (id = 5 and foo = 'bar')")
	mcmp.ExecWithColumnCompare("select id from t_user where (id = 5 and name ='foo') or (id = 12 and name = 'bar')")
	mcmp.ExecWithColumnCompare("select textcol1 from t_user where foo = 42 and t_user.foo = 42")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = 5")
	mcmp.ExecWithColumnCompare("select id from t_user where 5 = col")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col in (5)")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col in (5, 6, 7)")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col in (5, 6, 7) and col = 9")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col is null")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = null")
	mcmp.ExecWithColumnCompare("select id from t_user where not (not col = 3)")
	mcmp.ExecWithColumnCompare("select id from t_user where (col in (1, 5) and B or C and col in (5, 7))")
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.col = 5+5")
	mcmp.ExecWithColumnCompare("select id from t_user where id = 123 and t_user.col = 5+5")

}
