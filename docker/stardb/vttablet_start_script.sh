#!/bin/bash
set -uo pipefail

runmode=${runmode:-normal}
if [[ X${runmode} == Xdebug ]]
then
    echo "entering debug mode."
    tail -f /dev/null
fi

function pre_start(){
    systemctl stop vtgate.service
    systemctl stop vtctld.service
    systemctl disable vtgate.service
    systemctl disable vtctld.service
}
function vttablet_start() {
      while [ -z "${KEYSPACE}" ]; do
          echo "KEYSPACE 内容为空，等待5秒后退出..."
          sleep 5
          exit
      done
      ipaddr=$(hostname -i)

      ARGS="--alsologtostderr \
      --log_dir /export/data/mysql/tmp \
      --topo_implementation etcd2 \
      --topo_global_server_address $ETCD_SERVER \
      --docker_run \
      --topo_global_root /vt/global \
      --topo_etcd_username $ETCD_USER \
      --topo_etcd_password $ETCD_PASSWORD \
      --tablet-path $TABLET_ALIAS \
      --tablet_hostname $ipaddr \
      --health_check_interval 5s \
      --mysqlctl_socket /export/data/mysql/mysqlctl.sock \
      --port $WEB_PORT \
      --grpc_port $GRPC_PORT \
      --service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
      --init_keyspace $KEYSPACE \
      --init_shard $SHARD \
      --grpc_max_message_size $GPRC_MAXSIZE\
      --queryserver-config-pool-size $qsPoolSize \
      --queryserver-config-query-pool-timeout $qsQueryPoolTimeOut \
      --queryserver-config-transaction-cap $qsTransactionCap \
      --queryserver-config-txpool-timeout	$qsTxPoolTimeOut \
      --queryserver-config-query-timeout $qsQueryTimeOut \
      --queryserver-config-transaction-timeout $qsTransactionTimeOut \
      --queryserver-config-max-result-size $qsMaxResultSize \
      --unhealthy_threshold	$unHealthyThreshold \
      --queryserver-config-schema-reload-time 60 \
      --init_tablet_type $TABLET_TYPE \
      --mycnf_server_id $MYSQL_SERVER_ID \
      --db_app_user $DB_APP_USER \
      --db_app_password $DB_APP_PASS \
      --db_allprivs_user $DB_ADMIN_USER \
      --db_allprivs_password $DB_ADMIN_PASS \
      --db_dba_user $DBA_USER \
      --db_dba_password $DBA_PASS \
      --db_filtered_user $DB_FILTER_USER \
      --db_filtered_password $DB_FILTER_PASS \
      --db_repl_user $DB_REPL_USER \
      --db_repl_password $DB_REPL_PASS \
      --heartbeat_enable=true \
      --enforce_strict_trans_tables=false \
      --track_schema_versions=true \
      --watch_replication_stream=true"

      echo "su -p -c \"/vt/bin/vttablet ${ARGS}\" mysql"
      exec su -p -c "/vt/bin/vttablet ${ARGS}" mysql
}

if [ "${COMPONENTROLE}" == "vttablet" ]; then
    pre_start
    vttablet_start
fi
