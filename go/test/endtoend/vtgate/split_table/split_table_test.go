package vtgate

import (
	"testing"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestOne(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	//因为分表会在plan层依赖库名这里要加个use ks语句
	mcmp.Exec("use ks")
	//todo 目前没有分表insert 只能去提前算分表hash找到映射关系  直接插入的物理表里
	utils.Exec(t, mcmp.VtConn, `insert into t_user_1(id,col,f_key,f_tinyint,f_bit) values (2, 'b', 'bbb', 2, false)`)
	utils.Exec(t, mcmp.VtConn, `insert into t_user_2(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false)`)
	utils.Exec(t, mcmp.MySQLConn, `insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false)`)
	mcmp.ExecWithColumnCompare(`select id,col,f_key,f_tinyint,f_bit from t_user`)

}
