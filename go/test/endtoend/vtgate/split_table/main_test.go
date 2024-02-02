package split_table

import (
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	shardedKs       = "user"
	unshardedKs     = "uks"
	shardedKsShards = []string{"-80", "80-"}

	Cell = "test"
	//go:embed sharded_schema.sql
	shardedSchemaSQL string

	//go:embed mysql_schema.sql
	mysqlSchemaSQL string

	//go:embed unsharded_schema.sql
	unshardedSchemaSQL string

	//go:embed sharded_vschema.json
	shardedVSchema string

	//go:embed unsharded_vschema.json
	unshardedVSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		sKs := &cluster.Keyspace{
			Name:      shardedKs,
			SchemaSQL: shardedSchemaSQL,
			VSchema:   shardedVSchema,
		}

		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal"}
		err = clusterInstance.StartKeyspace(*sKs, shardedKsShards, 0, false)
		if err != nil {
			return 1
		}

		uKs := &cluster.Keyspace{
			Name:      unshardedKs,
			SchemaSQL: unshardedSchemaSQL,
			VSchema:   unshardedVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*uKs, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGatePlannerVersion = planbuilder.Gen4 // enable Gen4 planner.
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		conn, closer, err := utils.NewMySQL(clusterInstance, shardedKs, mysqlSchemaSQL)
		if err != nil {
			fmt.Println(err)
			return 1
		}
		defer closer()
		mysqlParams = conn
		return m.Run()
	}()
	os.Exit(exitCode)
}

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)
	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")
		mcmp.Exec("use user")
		tables := []string{"t_user", "t_1", "t_2", "t_3", "t_4", "t_5", "t_6", "t_7", "t_8", "t_user_extra", "t_music", "t_user_shard"}
		for _, table := range tables {
			mcmp.Exec("delete from " + table)
		}
		utils.Exec(t, mcmp.VtConn, "insert IGNORE into user.t_seq (id, next_id, cache) values (0, 1, 1)")
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}
