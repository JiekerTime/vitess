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
  source /vt/config/vttablet/keyspaceenv
  if [ -z "${KEYSPACE}" ]; then
    echo "KEYSPACE 内容为空，等待5秒后退出..."
    sleep 5
    exit
  fi

  if [ ! -d "/export/data/mysql/tmp" ]; then
    echo "等待 mysqlctld 初始化/export/data/mysql/tmp目录"
    sleep 5
    exit
  fi
  ipaddr=$(hostname -i)

  if [ "$ETCD_USER" == "" ];then
    ARGS="--alsologtostderr \
    --log_dir /export/data/mysql/tmp \
    --keep_logs_by_mtime 72h \
    --topo_implementation etcd2 \
    --topo_global_server_address http://$ETCD_SERVER \
    --docker_run \
    --topo_global_root /vt/global \
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
    --enforce_strict_trans_tables=false \
    --watch_replication_stream=true"
  else
    ARGS="--alsologtostderr \
    --log_dir /export/data/mysql/tmp \
    --keep_logs_by_mtime 72h \
    --topo_implementation etcd2 \
    --topo_global_server_address http://$ETCD_SERVER \
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
    --enforce_strict_trans_tables=false \
    --watch_replication_stream=true"
  fi

  echo "su -p -c \"/export/bin/vttablet ${ARGS}\" mysql"
  exec su -p -c "/export/bin/vttablet ${ARGS}" mysql
}

if [ "${COMPONENTROLE}" == "vttablet" ]; then
  pre_start
  vttablet_start
fi
