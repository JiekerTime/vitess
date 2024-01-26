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
  systemctl stop vtgate.service
  systemctl stop mysqlctld.service
  systemctl stop vttablet.service
  systemctl disable vtgate.service
  systemctl disable mysqlctld.service
  systemctl disable vttablet.service
}

function vtctld_start() {
  if [ "$ETCD_USER" == "" ];then
    ARGS="--alsologtostderr \
    --log_dir /export/data/mysql/tmp \
    --keep_logs_by_mtime 72h \
    --topo_implementation etcd2 \
    --topo_global_server_address http://$ETCD_SERVER \
    --topo_global_root /vt/global \
    --cell $CELL \
    --service_map 'grpc-vtctl,grpc-vtctld' \
    --backup_storage_implementation file \
    --file_backup_storage_root /export/data/mysql/backups \
    --log_dir /export/data/mysql/tmp \
    --port $WEB_PORT \
    --grpc_port $GRPC_PORT \
    --pid_file /export/data/mysql/tmp/vtctld.pid \
    > /export/data/mysql/tmp/vtctld.out 2>&1"
  else
    ARGS="--alsologtostderr \
    --log_dir /export/data/mysql/tmp \
    --keep_logs_by_mtime 72h \
    --topo_implementation etcd2 \
    --topo_global_server_address http://$ETCD_SERVER \
    --topo_global_root /vt/global \
    --topo_etcd_username $ETCD_USER \
    --topo_etcd_password $ETCD_PASSWORD \
    --cell $CELL \
    --service_map 'grpc-vtctl,grpc-vtctld' \
    --backup_storage_implementation file \
    --file_backup_storage_root /export/data/mysql/backups \
    --log_dir /export/data/mysql/tmp \
    --port $WEB_PORT \
    --grpc_port $GRPC_PORT \
    --pid_file /export/data/mysql/tmp/vtctld.pid \
    > /export/data/mysql/tmp/vtctld.out 2>&1"
  fi
  echo "su -p -c \"/export/bin/vtctld ${ARGS}\""
  exec su -p -c "/export/bin/vtctld ${ARGS}" vitess
}

if [ "${COMPONENTROLE}" == "vtctld" ]; then
  pre_start
  install_dependencies
  vtctld_start
fi
