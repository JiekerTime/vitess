#!/bin/bash
set -uo pipefail

runmode=${runmode:-normal}
if [[ X${runmode} == Xdebug ]]
then
    echo "entering debug mode."
    tail -f /dev/null
fi

function install_dependencies() {
  if [ ! -d "/export/logs" ]; then
    mkdir -p /export/logs;
  fi

  if [ ! -d "/export/vtdatarooot" ];then
    mkdir -p /export/vtdataroot
  fi

  if [ ! -d "/export/data/mysql/tmp" ];then
     cd /vt && tar zxf stardbplus_bin.tar.gz && mv bin /export/ && rm -f stardbplus_bin.tar.gz
    mkdir -p /export/data/mysql/tmp
    chown -R vitess /vt
    chown -R vitess /export
  fi
}

function pre_start(){
  systemctl stop vtctld.service
  systemctl stop mysqlctld.service
  systemctl stop vttablet.service
  systemctl disable vtctld.service
  systemctl disable mysqlctld.service
  systemctl disable vttablet.service
}

function vtgate_start() {
  if [ "$ETCD_USER" == "" ];then
    ARGS="--alsologtostderr \
    --grpc_prometheus \
    --log_dir /export/data/mysql/tmp \
    --keep_logs_by_mtime 72h \
    --topo_implementation etcd2 \
    --topo_global_server_address http://$ETCD_SERVER \
    --topo_global_root /vt/global \
    --log_queries_to_file /export/data/mysql/tmp/vtgate_querylog.txt \
    --port $WEB_PORT \
    --grpc_port $GRPC_PORT \
    --grpc_max_message_size $GPRC_MAXSIZE \
    --mysql_server_port $MYSQL_PORT \
    --cell $MAIN_CELL \
    --cells_to_watch $WATCH_CELLS \
    --tablet_types_to_wait PRIMARY,REPLICA \
    --service_map 'grpc-vtgateservice' \
    --mysql_server_version $MYSQL_VERSION \
    --pid_file /export/data/mysql/tmp/vtgate.pid \
    --mysql_auth_server_config_file /vt/config/vtgate/user.json \
    > /export/data/mysql/tmp/vtgate.out 2>&1"
  else
    ARGS="--alsologtostderr \
    --grpc_prometheus \
    --log_dir /export/data/mysql/tmp \
    --keep_logs_by_mtime 72h \
    --topo_implementation etcd2 \
    --topo_global_server_address http://$ETCD_SERVER \
    --topo_global_root /vt/global \
    --topo_etcd_username $ETCD_USER \
    --topo_etcd_password $ETCD_PASSWORD \
    --log_queries_to_file /export/data/mysql/tmp/vtgate_querylog.txt \
    --port $WEB_PORT \
    --grpc_port $GRPC_PORT \
    --grpc_max_message_size $GPRC_MAXSIZE \
    --mysql_server_port $MYSQL_PORT \
    --cell $MAIN_CELL \
    --cells_to_watch $WATCH_CELLS \
    --tablet_types_to_wait PRIMARY,REPLICA \
    --service_map 'grpc-vtgateservice' \
    --mysql_server_version $MYSQL_VERSION \
    --pid_file /export/data/mysql/tmp/vtgate.pid \
    --mysql_auth_server_config_file /vt/config/vtgate/user.json \
    > /export/data/mysql/tmp/vtgate.out 2>&1"
  fi
  echo "su -c \"/export/bin/vtgate ${ARGS}\" vitess"
  exec su -p -c "/export/bin/vtgate ${ARGS}" vitess
}

if [ "${COMPONENTROLE}" == "vtgate" ]; then
  pre_start
  install_dependencies
  vtgate_start
fi