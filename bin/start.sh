LOGS_DIR="/export/log/vtgate"

if [ ! -d $LOGS_DIR ]; then
	mkdir -p $LOGS_DIR
fi

nohup ./vtgate  --topo_implementation etcd2 --topo_global_server_address http://preglo151.etcd.jddb.com:4001 --topo_global_root /vt/global --log_dir /export/log/vtgate/tmp  --redact-debug-ui-queries --port 15009 --grpc_port 15999 --grpc_max_message_size 67108864 --mysql_server_port 3359 --service_map grpc-vtgateservice --cells_to_watch zyx151a --tablet_types_to_wait MASTER,REPLICA --mysql_auth_server_config_file /etc/config/acl/user2.json --cell zyx151a > /export/log/vtgate/vtgate_console.log 2>&1 &