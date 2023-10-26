package split_table

import (
	"testing"
)

func TestIssue(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (1, 'a', 'aaa', 1, false, 'li'),(2, 'b', 'bbb', 2, false, 'zh'),(3, 'c', 'ccc', 3, false, 'kk')")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (4, 'ab', 'aaa', 10, true, 'li'),(5, 'bx', 'bbb', 4, false, 'zhsd'),(6, 'cdx', 'ccc', 34, true, 'kk')")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (7, 'ac', 'aaa', 100, false, 'li'),(8, 'bx', 'bbb', 25, true, 'zhff'),(9, 'cd', 'ccc', 33, false, 'kggk')")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (11, 'ad', 'aaa', 2, true, 'li'),(12, 'ba', 'bbb', 26, false, 'zdfh'),(13, 'cc', 'ccc', 13, true, 'kzzk')")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (14, 'ae', 'aaa', 3, false, 'a'),(15, 'bd', 'bbb', 27, true, 'zhdf'),(16, 'fdfc', 'ccc', 64, true, 'xxxxx')")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (17, 'a', 'aaa', 1, false, 'li'),(18, 'b', 'bbb', 2, false, 'zh'),(19, 'c', 'ccc', 3, false, 'kk')")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (20, 'ab', 'aaa', 10, true, 'li'),(21, 'bx', 'bbb', 4, false, 'zhsd'),(22, 'cdx', 'ccc', 34, true, 'kk')")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (23, 'ac', 'aaa', 100, false, 'li'),(24, 'bx', 'bbb', 25, true, 'zhff'),(25, 'cd', 'ccc', 33, false, 'kggk')")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_int,name) values (26, 'ad', 'aaa', 2, true, 'li'),(27, 'ba', 'bbb', 26, false, 'zdfh'),(28, 'cc', 'ccc', 13, true, 'kzzk')")

	// table_issue.json
	mcmp.ExecWithColumnCompare("select max(f_int),min(`name`) from t_user group by id,f_tinyint order by f_tinyint desc limit 20")
	mcmp.ExecWithColumnCompare("select max(f_int),min(`name`) from t_user group by id,f_tinyint order by f_tinyint asc")
	mcmp.ExecWithColumnCompare("select max(f_int),min(`name`) from t_user group by col order by f_tinyint asc")
	mcmp.ExecWithColumnCompare("select max(f_int),min(`name`) from t_user group by col,id order by f_tinyint asc")
}
