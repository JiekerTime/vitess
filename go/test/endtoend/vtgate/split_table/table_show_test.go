package split_table

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestShowTable(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	got := "[[VARCHAR(\"t_1\")] [VARCHAR(\"t_1_0\")] [VARCHAR(\"t_1_1\")] [VARCHAR(\"t_2\")] [VARCHAR(\"t_2_0\")] [VARCHAR(\"t_2_1\")] [VARCHAR(\"t_3\")] [VARCHAR(\"t_3_0\")] [VARCHAR(\"t_3_1\")] " +
		"[VARCHAR(\"t_4\")] [VARCHAR(\"t_4_0\")] [VARCHAR(\"t_4_1\")] [VARCHAR(\"t_5\")] [VARCHAR(\"t_5_0\")] [VARCHAR(\"t_5_1\")] [VARCHAR(\"t_6\")] [VARCHAR(\"t_6_0\")] [VARCHAR(\"t_6_1\")] " +
		"[VARCHAR(\"t_7\")] [VARCHAR(\"t_7_0\")] [VARCHAR(\"t_7_1\")] [VARCHAR(\"t_music\")] [VARCHAR(\"t_music_0\")] [VARCHAR(\"t_music_1\")] [VARCHAR(\"t_seq\")] " +
		"[VARCHAR(\"t_user\")] [VARCHAR(\"t_user_0\")] [VARCHAR(\"t_user_1\")] [VARCHAR(\"t_user_extra\")] [VARCHAR(\"t_user_extra_0\")] [VARCHAR(\"t_user_extra_1\")]]"
	got1 := "[[VARBINARY(\"t_1\")] [VARBINARY(\"t_1_0\")] [VARBINARY(\"t_1_1\")] [VARBINARY(\"t_2\")] [VARBINARY(\"t_2_0\")] [VARBINARY(\"t_2_1\")] [VARBINARY(\"t_3\")] [VARBINARY(\"t_3_0\")] [VARBINARY(\"t_3_1\")] " +
		"[VARBINARY(\"t_4\")] [VARBINARY(\"t_4_0\")] [VARBINARY(\"t_4_1\")] [VARBINARY(\"t_5\")] [VARBINARY(\"t_5_0\")] [VARBINARY(\"t_5_1\")] [VARBINARY(\"t_6\")] [VARBINARY(\"t_6_0\")] [VARBINARY(\"t_6_1\")] " +
		"[VARBINARY(\"t_7\")] [VARBINARY(\"t_7_0\")] [VARBINARY(\"t_7_1\")] [VARBINARY(\"t_music\")] [VARBINARY(\"t_music_0\")] [VARBINARY(\"t_music_1\")] [VARBINARY(\"t_seq\")] " +
		"[VARBINARY(\"t_user\")] [VARBINARY(\"t_user_0\")] [VARBINARY(\"t_user_1\")] [VARBINARY(\"t_user_extra\")] [VARBINARY(\"t_user_extra_0\")] [VARBINARY(\"t_user_extra_1\")]]"
	utils.AssertMatchesAny(t, mcmp.VtConn, "show tables", got, got1)

	got = "[[VARCHAR(\"t_user\")] [VARCHAR(\"t_user_0\")] [VARCHAR(\"t_user_1\")] [VARCHAR(\"t_user_extra\")] [VARCHAR(\"t_user_extra_0\")] [VARCHAR(\"t_user_extra_1\")]]"
	got1 = "[[VARBINARY(\"t_user\")] [VARBINARY(\"t_user_0\")] [VARBINARY(\"t_user_1\")] [VARBINARY(\"t_user_extra\")] [VARBINARY(\"t_user_extra_0\")] [VARBINARY(\"t_user_extra_1\")]]"
	utils.AssertMatchesAny(t, mcmp.VtConn, "show tables like 't_user%'", got, got1)

	got = "[[VARCHAR(\"t_1\")] [VARCHAR(\"t_1_0\")] [VARCHAR(\"t_1_1\")] [VARCHAR(\"t_2_1\")] [VARCHAR(\"t_3_1\")] [VARCHAR(\"t_4_1\")] [VARCHAR(\"t_5_1\")] [VARCHAR(\"t_6_1\")] [VARCHAR(\"t_7_1\")] [VARCHAR(\"t_music_1\")] [VARCHAR(\"t_user_1\")] [VARCHAR(\"t_user_extra_1\")]]"
	got1 = "[[VARBINARY(\"t_1\")] [VARBINARY(\"t_1_0\")] [VARBINARY(\"t_1_1\")] [VARBINARY(\"t_2_1\")] [VARBINARY(\"t_3_1\")] [VARBINARY(\"t_4_1\")] [VARBINARY(\"t_5_1\")] [VARBINARY(\"t_6_1\")] [VARBINARY(\"t_7_1\")] [VARBINARY(\"t_music_1\")] [VARBINARY(\"t_user_1\")] [VARBINARY(\"t_user_extra_1\")]]"

	utils.AssertMatchesAny(t, mcmp.VtConn, "show tables like '%_1%'", got, got1)

	got = "[[VARCHAR(\"t_2\")]]"
	got1 = "[[VARBINARY(\"t_2\")]]"
	utils.AssertMatchesAny(t, mcmp.VtConn, "show tables like '%_2'", got, got1)
}

func TestShowColumn(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("show COLUMNS FROM t_user")
}
