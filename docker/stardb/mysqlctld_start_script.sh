#!/bin/bash
set -uo pipefail

runmode=${runmode:-normal}
if [[ X${runmode} == Xdebug ]]
then
    echo "entering debug mode."
    tail -f /dev/null
fi

if [ ! -d "/export/logs" ]; then
  mkdir -p /export/logs;
fi

if [ ! -d /export/servers ]; then
  cd /vt && tar zxvf stardbplus_bin.tar.gz && rm -f stardbplus_bin.tar.gz
  cd /servers && tar zxf mysql.tar.gz && rm -f mysql.tar.gz
  mv /servers /export/
  ln -s -b /export/servers/mysql/bin/* /usr/bin/
  chown -R mysql:mysql /vt
  chown -R mysql:mysql /export
fi

function stop_all_service(){
  systemctl stop vtgate.service
  systemctl stop vtctld.service
  systemctl stop mysqlctld.service
  systemctl stop vttablet.service
  systemctl disable vtgate.service
  systemctl disable vtctld.service
  systemctl disable mysqlctld.service
  systemctl disable vttablet.service
}

function pre_start() {
  systemctl stop vtgate.service
  systemctl stop vtctld.service
  systemctl disable vtgate.service
  systemctl disable vtctld.service
}

function mysqlctld_start() {
      ARGS="--alsologtostderr \
      --db_charset utf8mb4 \
      --db_dba_user $DBA_USER \
      --init_db_sql_file /vt/config/init_db.sql \
      --log_dir /export/data/mysql/tmp \
      --mysql_port $MYSQL_PORT \
      --socket_file /export/data/mysql/mysqlctl.sock \
      --docker_run \
      --mysqlctl_mycnf_template /vt/config/etc/my.cnf \
      --tablet_uid $TABLET_UID"

      echo "su -p -c \"/vt/bin/mysqlctld ${ARGS}\" mysql"
      exec su -p -c "/vt/bin/mysqlctld ${ARGS}" mysql
}

if [ "${COMPONENTROLE}" == "vttablet" ]; then
  pre_start
  mysqlctld_start
fi

#exporter/agent stop all service
if [ -z "${COMPONENTROLE}" ]; then
  stop_all_service
fi