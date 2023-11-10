/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"github.com/google/uuid"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"
)

var (
	mysqlServerPort                   = -1
	mysqlServerBindAddress            string
	mysqlServerSocketPath             string
	mysqlTCPVersion                   = "tcp"
	mysqlAllowClearTextWithoutTLS     bool
	mysqlProxyProtocol                bool
	mysqlServerRequireSecureTransport bool
	mysqlSslCert                      string
	mysqlSslKey                       string
	mysqlSslCa                        string
	mysqlSslCrl                       string
	mysqlSslServerCA                  string
	mysqlTLSMinVersion                string

	mysqlKeepAlivePeriod          time.Duration
	mysqlConnReadTimeout          time.Duration
	mysqlConnWriteTimeout         time.Duration
	mysqlQueryTimeout             time.Duration
	mysqlSlowConnectWarnThreshold time.Duration
	mysqlConnBufferPooling        bool

	mysqlDefaultWorkloadName = "OLTP"
	mysqlDefaultWorkload     int32
)

func registerPluginFlags(fs *pflag.FlagSet) {
	fs.IntVar(&mysqlServerPort, "mysql_server_port", mysqlServerPort, "If set, also listen for MySQL binary protocol connections on this port.")
	fs.StringVar(&mysqlServerBindAddress, "mysql_server_bind_address", mysqlServerBindAddress, "Binds on this address when listening to MySQL binary protocol. Useful to restrict listening to 'localhost' only for instance.")
	fs.StringVar(&mysqlServerSocketPath, "mysql_server_socket_path", mysqlServerSocketPath, "This option specifies the Unix socket file to use when listening for local connections. By default it will be empty and it won't listen to a unix socket")
	fs.StringVar(&mysqlTCPVersion, "mysql_tcp_version", mysqlTCPVersion, "Select tcp, tcp4, or tcp6 to control the socket type.")
	fs.BoolVar(&mysqlAllowClearTextWithoutTLS, "mysql_allow_clear_text_without_tls", mysqlAllowClearTextWithoutTLS, "If set, the server will allow the use of a clear text password over non-SSL connections.")
	fs.BoolVar(&mysqlProxyProtocol, "proxy_protocol", mysqlProxyProtocol, "Enable HAProxy PROXY protocol on MySQL listener socket")
	fs.BoolVar(&mysqlServerRequireSecureTransport, "mysql_server_require_secure_transport", mysqlServerRequireSecureTransport, "Reject insecure connections but only if mysql_server_ssl_cert and mysql_server_ssl_key are provided")
	fs.StringVar(&mysqlSslCert, "mysql_server_ssl_cert", mysqlSslCert, "Path to the ssl cert for mysql server plugin SSL")
	fs.StringVar(&mysqlSslKey, "mysql_server_ssl_key", mysqlSslKey, "Path to ssl key for mysql server plugin SSL")
	fs.StringVar(&mysqlSslCa, "mysql_server_ssl_ca", mysqlSslCa, "Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs.")
	fs.StringVar(&mysqlSslCrl, "mysql_server_ssl_crl", mysqlSslCrl, "Path to ssl CRL for mysql server plugin SSL")
	fs.StringVar(&mysqlTLSMinVersion, "mysql_server_tls_min_version", mysqlTLSMinVersion, "Configures the minimal TLS version negotiated when SSL is enabled. Defaults to TLSv1.2. Options: TLSv1.0, TLSv1.1, TLSv1.2, TLSv1.3.")
	fs.StringVar(&mysqlSslServerCA, "mysql_server_ssl_server_ca", mysqlSslServerCA, "path to server CA in PEM format, which will be combine with server cert, return full certificate chain to clients")
	fs.DurationVar(&mysqlSlowConnectWarnThreshold, "mysql_slow_connect_warn_threshold", mysqlSlowConnectWarnThreshold, "Warn if it takes more than the given threshold for a mysql connection to establish")
	fs.DurationVar(&mysqlConnReadTimeout, "mysql_server_read_timeout", mysqlConnReadTimeout, "connection read timeout")
	fs.DurationVar(&mysqlConnWriteTimeout, "mysql_server_write_timeout", mysqlConnWriteTimeout, "connection write timeout")
	fs.DurationVar(&mysqlQueryTimeout, "mysql_server_query_timeout", mysqlQueryTimeout, "mysql query timeout")
	fs.BoolVar(&mysqlConnBufferPooling, "mysql-server-pool-conn-read-buffers", mysqlConnBufferPooling, "If set, the server will pool incoming connection read buffers")
	fs.DurationVar(&mysqlKeepAlivePeriod, "mysql-server-keepalive-period", mysqlKeepAlivePeriod, "TCP period between keep-alives")
	fs.StringVar(&mysqlDefaultWorkloadName, "mysql_default_workload", mysqlDefaultWorkloadName, "Default session workload (OLTP, OLAP, DBA)")
}

// vtgateHandler implements the Listener interface.
// It stores the Session in the ClientData of a Connection.
type vtgateHandler struct {
	mysql.UnimplementedHandler
	mu sync.Mutex

	vtg         *VTGate
	connections map[uint32]*mysql.Conn

	busyConnections atomic.Int32
}

func newVtgateHandler(vtg *VTGate) *vtgateHandler {
	return &vtgateHandler{
		vtg:         vtg,
		connections: make(map[uint32]*mysql.Conn),
	}
}

func (vh *vtgateHandler) NewConnection(c *mysql.Conn) {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	vh.connections[c.ConnectionID] = c
}

func (vh *vtgateHandler) numConnections() int {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	return len(vh.connections)
}

func (vh *vtgateHandler) ComResetConnection(c *mysql.Conn) {
	ctx := context.Background()
	session := vh.session(c)
	if session.InTransaction {
		defer vh.busyConnections.Add(-1)
	}
	err := vh.vtg.CloseSession(ctx, session)
	if err != nil {
		log.Errorf("Error happened in transaction rollback: %v", err)
	}
}

func (vh *vtgateHandler) ConnectionClosed(c *mysql.Conn) {
	// Rollback if there is an ongoing transaction. Ignore error.
	defer func() {
		vh.mu.Lock()
		delete(vh.connections, c.ConnectionID)
		vh.mu.Unlock()
	}()

	var ctx context.Context
	var cancel context.CancelFunc
	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	session := vh.session(c)
	if session.InTransaction {
		defer vh.busyConnections.Add(-1)
	}
	_ = vh.vtg.CloseSession(ctx, session)
}

// Regexp to extract parent span id over the sql query
var r = regexp.MustCompile(`/\*VT_SPAN_CONTEXT=(.*)\*/`)

// this function is here to make this logic easy to test by decoupling the logic from the `trace.NewSpan` and `trace.NewFromString` functions
func startSpanTestable(ctx context.Context, query, label string,
	newSpan func(context.Context, string) (trace.Span, context.Context),
	newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error)) (trace.Span, context.Context, error) {
	_, comments := sqlparser.SplitMarginComments(query)
	match := r.FindStringSubmatch(comments.Leading)
	span, ctx := getSpan(ctx, match, newSpan, label, newSpanFromString)

	trace.AnnotateSQL(span, sqlparser.Preview(query))

	return span, ctx, nil
}

func getSpan(ctx context.Context, match []string, newSpan func(context.Context, string) (trace.Span, context.Context), label string, newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error)) (trace.Span, context.Context) {
	var span trace.Span
	if len(match) != 0 {
		var err error
		span, ctx, err = newSpanFromString(ctx, match[1], label)
		if err == nil {
			return span, ctx
		}
		log.Warningf("Unable to parse VT_SPAN_CONTEXT: %s", err.Error())
	}
	span, ctx = newSpan(ctx, label)
	return span, ctx
}

func startSpan(ctx context.Context, query, label string) (trace.Span, context.Context, error) {
	return startSpanTestable(ctx, query, label, trace.NewSpan, trace.NewFromString)
}

func (vh *vtgateHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.UpdateCancelCtx(cancel)

	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, mysqlQueryTimeout)
		defer cancel()
	}

	span, ctx, err := startSpan(ctx, query, "vtgateHandler.ComQuery")
	if err != nil {
		return vterrors.Wrap(err, "failed to extract span")
	}
	defer span.Finish()

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session := vh.session(c)
	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	//log app_user and app_host
	if !strings.Contains(session.Options.UagInfo, "uag::") {
		session.Options.UagInfo = fmt.Sprintf("/* uag::%v;%s;%s;%s */", c.User, c.ClientHost, c.RemoteAddr().String(), c.GetLocalAddr())
	}
	tabletType, err := vh.getTabletType(c.AccountType, query, c.User)
	if err != nil {
		return err
	}

	session.TargetString = strings.Split(session.TargetString, "@")[0] + tabletType

	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		session, err := vh.vtg.StreamExecute(ctx, vh, c, session, query, make(map[string]*querypb.BindVariable), callback)
		if err != nil {
			return sqlerror.NewSQLErrorFromError(err)
		}
		fillInTxStatusFlags(c, session)
		return nil
	}
	session, result, err := vh.vtg.Execute(ctx, vh, c, session, query, make(map[string]*querypb.BindVariable))

	if err := sqlerror.NewSQLErrorFromError(err); err != nil {
		return err
	}
	fillInTxStatusFlags(c, session)
	return callback(result)
}

func fillInTxStatusFlags(c *mysql.Conn, session *vtgatepb.Session) {
	if session.InTransaction {
		c.StatusFlags |= mysql.ServerStatusInTrans
	} else {
		c.StatusFlags &= mysql.NoServerStatusInTrans
	}
	if session.Autocommit {
		c.StatusFlags |= mysql.ServerStatusAutocommit
	} else {
		c.StatusFlags &= mysql.NoServerStatusAutocommit
	}
}

// ComPrepare is the handler for command prepare.
func (vh *vtgateHandler) ComPrepare(c *mysql.Conn, query string, bindVars map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	var ctx context.Context
	var cancel context.CancelFunc
	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session := vh.session(c)
	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	session, fld, err := vh.vtg.Prepare(ctx, session, query, bindVars)
	err = sqlerror.NewSQLErrorFromError(err)
	if err != nil {
		return nil, err
	}
	return fld, nil
}

func (vh *vtgateHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.UpdateCancelCtx(cancel)

	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, mysqlQueryTimeout)
		defer cancel()
	}

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session := vh.session(c)
	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		_, err := vh.vtg.StreamExecute(ctx, vh, c, session, prepare.PrepareStmt, prepare.BindVars, callback)
		if err != nil {
			return sqlerror.NewSQLErrorFromError(err)
		}
		fillInTxStatusFlags(c, session)
		return nil
	}
	_, qr, err := vh.vtg.Execute(ctx, vh, c, session, prepare.PrepareStmt, prepare.BindVars)
	if err != nil {
		return sqlerror.NewSQLErrorFromError(err)
	}
	fillInTxStatusFlags(c, session)

	return callback(qr)
}

func (vh *vtgateHandler) WarningCount(c *mysql.Conn) uint16 {
	return uint16(len(vh.session(c).GetWarnings()))
}

// ComRegisterReplica is part of the mysql.Handler interface.
func (vh *vtgateHandler) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	return vterrors.VT12001("ComRegisterReplica for the VTGate handler")
}

// ComBinlogDump is part of the mysql.Handler interface.
func (vh *vtgateHandler) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	return vterrors.VT12001("ComBinlogDump for the VTGate handler")
}

// ComBinlogDumpGTID is part of the mysql.Handler interface.
func (vh *vtgateHandler) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet) error {
	return vterrors.VT12001("ComBinlogDumpGTID for the VTGate handler")
}

// KillConnection closes an open connection by connection ID.
func (vh *vtgateHandler) KillConnection(ctx context.Context, connectionID uint32) error {
	vh.mu.Lock()
	defer vh.mu.Unlock()

	c, exists := vh.connections[connectionID]
	if !exists {
		return sqlerror.NewSQLError(sqlerror.ERNoSuchThread, sqlerror.SSUnknownSQLState, "Unknown thread id: %d", connectionID)
	}

	// First, we mark the connection for close, so that even when the context is cancelled, while returning the response back to client,
	// the connection can get closed,
	// Closing the connection will trigger ConnectionClosed method which rollback any open transaction.
	c.MarkForClose()
	c.CancelCtx()

	return nil
}

// KillQuery cancels any execution query on the provided connection ID.
func (vh *vtgateHandler) KillQuery(connectionID uint32) error {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	c, exists := vh.connections[connectionID]
	if !exists {
		return sqlerror.NewSQLError(sqlerror.ERNoSuchThread, sqlerror.SSUnknownSQLState, "Unknown thread id: %d", connectionID)
	}
	c.CancelCtx()
	return nil
}

func (vh *vtgateHandler) session(c *mysql.Conn) *vtgatepb.Session {
	session, _ := c.ClientData.(*vtgatepb.Session)
	if session == nil {
		u, _ := uuid.NewUUID()
		session = &vtgatepb.Session{
			Options: &querypb.ExecuteOptions{
				IncludedFields: querypb.ExecuteOptions_ALL,
				Workload:       querypb.ExecuteOptions_Workload(mysqlDefaultWorkload),
				UagInfo:        generateUagInfo(c),
				// The collation field of ExecuteOption is set right before an execution.
			},
			Autocommit:           true,
			DDLStrategy:          defaultDDLStrategy,
			MigrationContext:     "",
			SessionUUID:          u.String(),
			EnableSystemSettings: sysVarSetEnabled,
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
		// 根据账号信息设置 workload。针对读rdonly的账号，设置olap。流式读
		if c.AccountType == mysql.AccountTypeStream {
			session.Options.Workload = querypb.ExecuteOptions_OLAP
		}
		c.ClientData = session
	}
	return session
}

type mysqlServer struct {
	tcpListener  *mysql.Listener
	unixListener *mysql.Listener
	sigChan      chan os.Signal
	vtgateHandle *vtgateHandler
}

// initTLSConfig inits tls config for the given mysql listener
func initTLSConfig(ctx context.Context, srv *mysqlServer, mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA string, mysqlServerRequireSecureTransport bool, mysqlMinTLSVersion uint16) error {
	serverConfig, err := vttls.ServerConfig(mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlMinTLSVersion)
	if err != nil {
		log.Exitf("grpcutils.TLSServerConfig failed: %v", err)
		return err
	}
	srv.tcpListener.TLSConfig.Store(serverConfig)
	srv.tcpListener.RequireSecureTransport = mysqlServerRequireSecureTransport
	srv.sigChan = make(chan os.Signal, 1)
	signal.Notify(srv.sigChan, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-srv.sigChan:
				serverConfig, err := vttls.ServerConfig(mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlMinTLSVersion)
				if err != nil {
					log.Errorf("grpcutils.TLSServerConfig failed: %v", err)
				} else {
					log.Info("grpcutils.TLSServerConfig updated")
					srv.tcpListener.TLSConfig.Store(serverConfig)
				}
			}
		}
	}()
	return nil
}

// initiMySQLProtocol starts the mysql protocol.
// It should be called only once in a process.
func initMySQLProtocol(vtgate *VTGate) *mysqlServer {
	// Flag is not set, just return.
	if mysqlServerPort < 0 && mysqlServerSocketPath == "" {
		return nil
	}

	// If no VTGate was created, just return.
	if vtgate == nil {
		return nil
	}

	// Initialize registered AuthServer implementations (or other plugins)
	for _, initFn := range pluginInitializers {
		initFn()
	}
	authServer := mysql.GetAuthServer(mysql.GetAuthServerImpl())

	// Check mysql_default_workload
	var ok bool
	if mysqlDefaultWorkload, ok = querypb.ExecuteOptions_Workload_value[strings.ToUpper(mysqlDefaultWorkloadName)]; !ok {
		log.Exitf("-mysql_default_workload must be one of [OLTP, OLAP, DBA, UNSPECIFIED]")
	}

	switch mysqlTCPVersion {
	case "tcp", "tcp4", "tcp6":
		// Valid flag value.
	default:
		log.Exitf("-mysql_tcp_version must be one of [tcp, tcp4, tcp6]")
	}

	// Create a Listener.
	var err error
	srv := &mysqlServer{}
	srv.vtgateHandle = newVtgateHandler(vtgate)
	if mysqlServerPort >= 0 {
		srv.tcpListener, err = mysql.NewListener(
			mysqlTCPVersion,
			net.JoinHostPort(mysqlServerBindAddress, fmt.Sprintf("%v", mysqlServerPort)),
			authServer,
			srv.vtgateHandle,
			mysqlConnReadTimeout,
			mysqlConnWriteTimeout,
			mysqlProxyProtocol,
			mysqlConnBufferPooling,
			mysqlKeepAlivePeriod,
		)
		if err != nil {
			log.Exitf("mysql.NewListener failed: %v", err)
		}
		srv.tcpListener.ServerVersion = servenv.MySQLServerVersion()
		if mysqlSslCert != "" && mysqlSslKey != "" {
			tlsVersion, err := vttls.TLSVersionToNumber(mysqlTLSMinVersion)
			if err != nil {
				log.Exitf("mysql.NewListener failed: %v", err)
			}

			_ = initTLSConfig(context.Background(), srv, mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlServerRequireSecureTransport, tlsVersion)
		}
		srv.tcpListener.AllowClearTextWithoutTLS.Store(mysqlAllowClearTextWithoutTLS)
		// Check for the connection threshold
		if mysqlSlowConnectWarnThreshold != 0 {
			log.Infof("setting mysql slow connection threshold to %v", mysqlSlowConnectWarnThreshold)
			srv.tcpListener.SlowConnectWarnThreshold.Store(mysqlSlowConnectWarnThreshold.Nanoseconds())
		}
		// Start listening for tcp
		go srv.tcpListener.Accept()
	}

	if mysqlServerSocketPath != "" {
		// Let's create this unix socket with permissions to all users. In this way,
		// clients can connect to vtgate mysql server without being vtgate user
		oldMask := syscall.Umask(000)
		srv.unixListener, err = newMysqlUnixSocket(mysqlServerSocketPath, authServer, srv.vtgateHandle)
		_ = syscall.Umask(oldMask)
		if err != nil {
			log.Exitf("mysql.NewListener failed: %v", err)
			return nil
		}
		// Listen for unix socket
		go srv.unixListener.Accept()
	}
	return srv
}

// newMysqlUnixSocket creates a new unix socket mysql listener. If a socket file already exists, attempts
// to clean it up.
func newMysqlUnixSocket(address string, authServer mysql.AuthServer, handler mysql.Handler) (*mysql.Listener, error) {
	listener, err := mysql.NewListener(
		"unix",
		address,
		authServer,
		handler,
		mysqlConnReadTimeout,
		mysqlConnWriteTimeout,
		false,
		mysqlConnBufferPooling,
		mysqlKeepAlivePeriod,
	)

	switch err := err.(type) {
	case nil:
		return listener, nil
	case *net.OpError:
		log.Warningf("Found existent socket when trying to create new unix mysql listener: %s, attempting to clean up", address)
		// err.Op should never be different from listen, just being extra careful
		// in case in the future other errors are returned here
		if err.Op != "listen" {
			return nil, err
		}
		_, dialErr := net.Dial("unix", address)
		if dialErr == nil {
			log.Errorf("Existent socket '%s' is still accepting connections, aborting", address)
			return nil, err
		}
		removeFileErr := os.Remove(address)
		if removeFileErr != nil {
			log.Errorf("Couldn't remove existent socket file: %s", address)
			return nil, err
		}
		listener, listenerErr := mysql.NewListener(
			"unix",
			address,
			authServer,
			handler,
			mysqlConnReadTimeout,
			mysqlConnWriteTimeout,
			false,
			mysqlConnBufferPooling,
			mysqlKeepAlivePeriod,
		)
		return listener, listenerErr
	default:
		return nil, err
	}
}

func (srv *mysqlServer) shutdownMysqlProtocolAndDrain() {
	if srv.tcpListener != nil {
		srv.tcpListener.Close()
		srv.tcpListener = nil
	}
	if srv.unixListener != nil {
		srv.unixListener.Close()
		srv.unixListener = nil
	}
	if srv.sigChan != nil {
		signal.Stop(srv.sigChan)
	}

	if busy := srv.vtgateHandle.busyConnections.Load(); busy > 0 {
		log.Infof("Waiting for all client connections to be idle (%d active)...", busy)
		start := time.Now()
		reported := start
		for busy > 0 {
			if time.Since(reported) > 2*time.Second {
				log.Infof("Still waiting for client connections to be idle (%d active)...", busy)
				reported = time.Now()
			}

			time.Sleep(1 * time.Millisecond)
			busy = srv.vtgateHandle.busyConnections.Load()
		}
	}
}

func (srv *mysqlServer) rollbackAtShutdown() {
	defer log.Flush()
	if srv.vtgateHandle == nil {
		// we still haven't been able to initialise the vtgateHandler, so we don't need to rollback anything
		return
	}

	// Close all open connections. If they're waiting for reads, this will cause
	// them to error out, which will automatically rollback open transactions.
	func() {
		if srv.vtgateHandle != nil {
			srv.vtgateHandle.mu.Lock()
			defer srv.vtgateHandle.mu.Unlock()
			for id, c := range srv.vtgateHandle.connections {
				if c != nil {
					log.Infof("Rolling back transactions associated with connection ID: %v", id)
					c.Close()
				}
			}
		}
	}()

	// If vtgate is instead busy executing a query, the number of open conns
	// will be non-zero. Give another second for those queries to finish.
	for i := 0; i < 100; i++ {
		if srv.vtgateHandle.numConnections() == 0 {
			log.Infof("All connections have been rolled back.")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Errorf("All connections did not go idle. Shutting down anyway.")
}

func mysqlSocketPath() string {
	if mysqlServerSocketPath == "" {
		return ""
	}
	return mysqlServerSocketPath
}

func init() {
	servenv.OnParseFor("vtgate", registerPluginFlags)
	servenv.OnParseFor("vtcombo", registerPluginFlags)
}

var pluginInitializers []func()

// RegisterPluginInitializer lets plugins register themselves to be init'ed at servenv.OnRun-time
func RegisterPluginInitializer(initializer func()) {
	pluginInitializers = append(pluginInitializers, initializer)
}

func (vh *vtgateHandler) CheckAttachedHost(c *mysql.Conn) error {
	if !c.CrossEnable && !c.AttachEnable {
		return nil
	}
	tabletType := topodatapb.TabletType_REPLICA
	if c.AccountType == mysql.AccountTypeAdmin || c.AccountType == mysql.AccountTypeRW || c.AccountType == mysql.AccountTypeUnknown {
		tabletType = topodatapb.TabletType_PRIMARY
	} else if c.AccountType == mysql.AccountTypeStream {
		tabletType = topodatapb.TabletType_RDONLY
	}
	keyspace := c.SchemaName
	if c.AttachEnable {
		keyspace = c.AttachTo
	}
	tablets := vh.GetTabletHost(keyspace, "0", tabletType)
	if len(tablets) == 0 {
		if tabletType == topodatapb.TabletType_PRIMARY {
			return fmt.Errorf("keyspace %v no vailid %v", keyspace, tabletType)
		}
		if tabletType == topodatapb.TabletType_REPLICA {
			tablets = vh.GetTabletHost(keyspace, "0", topodatapb.TabletType_RDONLY)
		} else {
			tablets = vh.GetTabletHost(keyspace, "0", topodatapb.TabletType_REPLICA)
		}
	}

	for _, tablet := range tablets {
		if tablet.MysqlHostname == c.CtMysql.MysqlIP && tablet.MysqlPort == c.CtMysql.MysqlPort {
			return nil
		}
	}

	return fmt.Errorf("backend server changed, please reconnect")
}

func (vh *vtgateHandler) InitCrossTabletConn(c *mysql.Conn, authServer mysql.AuthServer, ks string) error {
	c.ResetCrossTablet()
	userks, err := authServer.GetKeyspace(c.User)
	if err != nil {
		return err
	}
	var dstks string
	if len(ks) != 0 {
		dstks = ks
	} else {
		if len(userks) == 1 {
			dstks = userks[0]
		}
	}
	if len(dstks) == 0 {
		return nil
	}
	vschema, ok := vh.vtg.executor.vschema.Keyspaces[dstks]
	if !ok {
		c.ResetCrossTablet()
		return nil
	}

	if vschema.Keyspace.CrossTablet || vschema.Keyspace.AttachEnable {
		// 直连模式和共享实例最多一个为true。因为拼写DbName字段规则不同
		if vschema.Keyspace.CrossTablet {
			c.CrossEnable = true
		} else if vschema.Keyspace.AttachEnable {
			c.AttachEnable = true
			c.AttachTo = vschema.Keyspace.AttachTo
		}
		c.SchemaName = dstks
		tabletType := topodatapb.TabletType_REPLICA
		if c.AccountType == mysql.AccountTypeAdmin || c.AccountType == mysql.AccountTypeRW || c.AccountType == mysql.AccountTypeUnknown {
			tabletType = topodatapb.TabletType_PRIMARY
		}
		if c.AccountType == mysql.AccountTypeStream {
			tabletType = topodatapb.TabletType_RDONLY
		}

		//tablets := vh.vtg.gateway.GetHealthyTabletStats("", c.AttachBackend.AttachTo, "0", tabletType)
		if vschema.Keyspace.AttachEnable {
			dstks = vschema.Keyspace.AttachTo
		}
		tablets := vh.GetTabletHost(dstks, "0", tabletType)
		if len(tablets) == 0 {
			if tabletType == topodatapb.TabletType_PRIMARY {
				return fmt.Errorf("keyspace %v no vailid primary", dstks)
			}
			if tabletType == topodatapb.TabletType_RDONLY {
				tabletType = topodatapb.TabletType_REPLICA
			} else {
				tabletType = topodatapb.TabletType_RDONLY
			}
			tablets = vh.GetTabletHost(dstks, "0", tabletType)
			if len(tablets) == 0 {
				return fmt.Errorf("keyspace %v no vailid replica", dstks)
			}
		}
		targetTablet := tablets[0]
		if tabletType == topodatapb.TabletType_PRIMARY && len(tablets) > 1 {
			for _, v := range tablets {
				if v.PrimaryTermStartTime.Seconds > targetTablet.PrimaryTermStartTime.Seconds {
					targetTablet = v
				}
			}
		}

		if tabletType != topodatapb.TabletType_PRIMARY && len(tablets) > 1 {
			targetTablet = tablets[rand.Intn(len(tablets))]
		}

		c.CtMysql.MysqlIP = targetTablet.MysqlHostname
		c.CtMysql.MysqlPort = targetTablet.MysqlPort

		password, err := authServer.GetPassword(c.User)
		if err != nil {
			return err
		}
		c.CtMysql.UserName = c.User
		c.CtMysql.Password = password

		return c.ReConnectCrossTablet()
	}
	return nil
}

func (vh *vtgateHandler) GetTabletHost(ks, shard string, tabletType topodatapb.TabletType) (tablet []*topodatapb.Tablet) {
	tblist := vh.vtg.resolver.scatterConn.GetHealthCheckHealthyStatus(&querypb.Target{Keyspace: ks, Shard: shard, TabletType: tabletType})
	for _, v := range tblist {
		if v.Serving {
			tablet = append(tablet, v.Tablet)
		}
	}
	return tablet
}

func generateUagInfo(c *mysql.Conn) string {
	buf := strings.Builder{}

	buf.WriteString("/* uag::")
	buf.WriteString(c.User)
	buf.WriteString(";")
	buf.WriteString(c.ClientHost)
	buf.WriteString(";")
	buf.WriteString(c.RemoteAddr().String())
	buf.WriteString(";")
	buf.WriteString(c.GetLocalAddr())
	buf.WriteString(" */")

	return buf.String()
}

func (vh *vtgateHandler) ComFieldList(c *mysql.Conn, tableName string, callback func(*sqltypes.Result) error) error {
	var ctx context.Context
	var cancel context.CancelFunc
	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session, _ := c.ClientData.(*vtgatepb.Session)
	if session == nil {
		session = &vtgatepb.Session{
			Options: &querypb.ExecuteOptions{
				IncludedFields: querypb.ExecuteOptions_ALL,
			},
			Autocommit: true,
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
	}

	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	if c.SchemaName != "" {
		session.TargetString = c.SchemaName
	}

	var tabletType string
	/*	if c.AccountType == mysql.AccountTypeAdmin ||
		c.AccountType == mysql.AccountTypeRW {
		tabletType = "@PRIMARY"
	}*/
	switch {
	case c.AccountType == mysql.AccountTypeRW:
		tabletType = "@PRIMARY"
	case c.AccountType == mysql.AccountTypeAdmin:
		tabletType = "@PRIMARY"
	case c.AccountType == mysql.AccountTypeRR:
		tabletType = "@REPLICA"
	case c.AccountType == mysql.AccountTypeStream:
		tabletType = "@RDONLY"
	default:
		tabletType = "@REPLICA"
	}

	session.TargetString = strings.Split(session.TargetString, "@")[0] + tabletType

	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	session, result, err := vh.vtg.Prepare(ctx, session, query, make(map[string]*querypb.BindVariable))
	c.ClientData = session
	err = sqlerror.NewSQLErrorFromError(err)
	if err != nil {
		return err
	}
	sqltypeResult := &sqltypes.Result{
		Fields: result,
	}
	return callback(sqltypeResult)
}

// SetAuthServer set auth server as property
func (vh *vtgateHandler) SetAuthServer(authServer mysql.AuthServer) {
	vh.vtg.executor.authServer = authServer
}

// ValidUseDB Valid UseDB statement
func (vh *vtgateHandler) ValidUseDB(c *mysql.Conn, usedb string, authServer mysql.AuthServer) error {
	//if sqlparser.SystemSchema(usedb) {
	//	return nil
	//}
	userkss, err := authServer.GetKeyspace(c.User)
	if err != nil {
		return err
	}
	if len(userkss) == 0 {
		return nil
	}

	usedb = strings.Split(usedb, ":")[0]

	for _, usks := range userkss {
		if strings.EqualFold(usks, usedb) || usks == "*" {
			return nil
		}
	}
	err = fmt.Errorf("keyspace %s not found in vschema", usedb)
	return sqlerror.NewSQLErrorFromError(err)
}

func (vh *vtgateHandler) getTabletType(accountType int8, query, user string) (string, error) {
	var tabletType string

	switch {
	case accountType == mysql.AccountTypeRW:
		tabletType = "@PRIMARY"
	case accountType == mysql.AccountTypeAdmin:
		tabletType = "@PRIMARY"
	case accountType == mysql.AccountTypeRR:
		tabletType = "@REPLICA"
	case accountType == mysql.AccountTypeStream:
		tabletType = "@RDONLY"
	case accountType == mysql.AccountTypeUnknown:
		tabletType = "@PRIMARY"
	default:
		tabletType = "@REPLICA"
	}

	stmtType := sqlparser.Preview(query)

	if isBasicPrivilege(stmtType) {
		return tabletType, nil
	}
	privileges, err := vh.vtg.executor.authServer.GetPrivilege(user)
	if err != nil {
		return "", sqlerror.NewSQLErrorFromError(err)
	}

	// In this privileges not set to 0, the following operations need to be interception:
	// create/drop/alter/truncate/rename
	if privileges == 0 {
		err := fmt.Errorf("user %s has no permission to run query, sql: %s", user, query)
		return "", sqlerror.NewSQLErrorFromError(err)
	}

	if !hasPrivilege(stmtType, privileges) {
		err := fmt.Errorf("user %s has no permission to run query, sql: %s", user, query)
		return "", sqlerror.NewSQLErrorFromError(err)
	}

	return tabletType, nil
}

// isBasicPrivilege is used to Determine whether it is a
// basic permission, rw user basic permission.
func isBasicPrivilege(stmtType sqlparser.StatementType) bool {
	switch stmtType {
	case sqlparser.StmtSet, sqlparser.StmtShow,
		sqlparser.StmtUse, sqlparser.StmtOther, sqlparser.StmtBegin, sqlparser.StmtCommit,
		sqlparser.StmtRollback, sqlparser.StmtUnknown,
		sqlparser.StmtComment, sqlparser.StmtPlan, sqlparser.StmtLoadData:
		return true
	default:
		return false
	}
}

// isBasicReadPrivilege is used to Determine whether it is a
// drc basic permission, drc rw user can only do  permission.
func isBasicReadPrivilege(stmtType sqlparser.StatementType) bool {
	switch stmtType {
	case sqlparser.StmtSelect, sqlparser.StmtSet,
		sqlparser.StmtShow, sqlparser.StmtUse,
		sqlparser.StmtOther, sqlparser.StmtBegin,
		sqlparser.StmtCommit, sqlparser.StmtRollback,
		sqlparser.StmtUnknown, sqlparser.StmtExplain,
		sqlparser.StmtComment, sqlparser.StmtPlan:
		return true
	default:
		return false
	}
}

// privilege type
const (
	privilegeSelect   = 1
	privilegeInsert   = 1 << 1
	privilegeUpdate   = 1 << 2
	privilegeDelete   = 1 << 3
	privilegeCreate   = 1 << 4
	privilegeAlter    = 1 << 5
	privilegeDrop     = 1 << 6
	privilegeTruncate = 1 << 7
	privilegeRename   = 1 << 8
)

// hasPrivilege is used to check if the user has permission
// to execute the corresponding SQL.
// Privilege is a uint16 number. The structure is as follows:
// +------+--------+----+-----+------+------+------+------+------+
// |rename|truncate|drop|alter|create|delete|update|insert|select|
// +------+--------+----+-----+------+------+------+------+------+
func hasPrivilege(stmtType sqlparser.StatementType, privileges uint16) bool {
	pri := privileges
	switch stmtType {
	case sqlparser.StmtRename:
		pri |= privilegeRename
	case sqlparser.StmtSelect:
		pri |= privilegeSelect
	case sqlparser.StmtInsert:
		pri |= privilegeInsert
	case sqlparser.StmtUpdate:
		pri |= privilegeUpdate
	case sqlparser.StmtDelete:
		pri |= privilegeDelete
	case sqlparser.StmtCreate:
		pri |= privilegeCreate
	case sqlparser.StmtAlter:
		pri |= privilegeAlter
	case sqlparser.StmtDrop:
		pri |= privilegeDrop
	case sqlparser.StmtTruncate:
		pri |= privilegeTruncate
	case sqlparser.StmtReplace:
		pri |= privilegeInsert
		pri |= privilegeDelete
	}
	return pri == privileges
}
