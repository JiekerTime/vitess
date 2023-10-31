# FROM is.jd.com/vitesss/tidb_base:v20211108164542
FROM is.jd.com/vitesss/tidb_base:v20220106090944
## set timezone
## timedatectl
## timedatectl set-timezone Asia/Shanghai
# https://docs.pingcap.com/zh/tidb/stable/tune-operating-system
# GLIBC_2.28
# Service
COPY vttablet.service /usr/lib/systemd/system
COPY mysqlctld.service /usr/lib/systemd/system

RUN ln -s /usr/lib/systemd/system/vttablet.service /etc/systemd/system/multi-user.target.wants/vttablet.service && \
    ln -s /usr/lib/systemd/system/mysqlctld.service /etc/systemd/system/multi-user.target.wants/mysqlctld.service

# logrotate
RUN yum install logrotate -y
# COPY rotate /etc/logrotate.d/

# fuse
RUN yum install fuse -y

# telnet
RUN yum install telnet -y

# jq
RUN yum install jq -y

# journalctl
# COPY journald.conf /etc/systemd/

# authorized_keys
RUN mkdir -p /root/.ssh
COPY authorized_keys /root/.ssh/

# Start script and TiKV binary
RUN mkdir -p /tidb
RUN mkdir -p /tidb/bin
COPY tikv-ctl tikv-server /tidb/bin/
COPY mysqlctld.tar.gz /tmp/
RUN cd /tmp && \
tar -zxf mysqlctld.tar.gz && \
mv /tmp/mysqlctld/* /tidb/bin/ && \
/bin/rm -rf /tmp/mysqlctld*


RUN set -eux; \
    echo "export TZ=\${TZ:-/etc/localtime}" >> /etc/profile; \
    echo "export LD_LIBRARY_PATH=/usr/local/lib" >> /etc/profile; \
    echo "export LD_LIBRARY_PATH=/tidb/bin:\$LD_LIBRARY_PATH" >> /etc/profile

RUN set -eux; \
    echo "export TZ=\${TZ:-/etc/localtime}" >> /root/.bashrc; \
    echo "export LD_LIBRARY_PATH=/usr/local/lib" >> /root/.bashrc; \
    echo "export LD_LIBRARY_PATH=/tidb/bin:\$LD_LIBRARY_PATH" >> /root/.bashrc

COPY vttablet_start_script.sh /tidb/scripts/
COPY mysqlctld_start_script.sh /tidb/scripts/

# Set perms to support non-root & arbitrary uid runtime in OpenShift
RUN chown -R 99:0 /tidb && \
    chmod -R 775 /tidb

# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /tidb/

# Expose TCP Ports
#EXPOSE 2379/tcp 2380/tcp

# Non-root runtime
# USER 99

# Run the script to start up the TiKV process
ENTRYPOINT ["/usr/sbin/init"]
