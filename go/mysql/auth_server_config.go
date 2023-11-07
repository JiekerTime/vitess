package mysql

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"sync"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/ipfilters"
	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/compressutil"
	"vitess.io/vitess/go/notifyutil"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	mysqlAuthServerConfigFile   string
	mysqlAuthServerConfigString string
	configAuthMethod            string
	mysqlAuthServerImpl         = "config"
)

func init() {
	servenv.OnParseFor("vtgate", func(fs *pflag.FlagSet) {
		fs.StringVar(&mysqlAuthServerConfigFile, "mysql_auth_server_config_file", mysqlAuthServerConfigFile, "JSON File to read the users/passwords from.")
		fs.StringVar(&mysqlAuthServerConfigString, "mysql_auth_server_config_string", "", "SON representation of the users/passwords config.")
		fs.StringVar(&configAuthMethod, "mysql_config_auth_method", string(MysqlNativePassword), "client-side authentication method to use. Supported values:mysql_native_password.")
		fs.StringVar(&mysqlAuthServerImpl, "mysql_auth_server_impl", mysqlAuthServerImpl, "Which auth server implementation to use. Options: none, ldap, clientcert, static, vault.")
	})
}

// AuthServerConfig implements AuthServer using a static configuration.
type AuthServerConfig struct {
	methods []AuthMethod
	// Method can be set to:
	// - MysqlNativePassword
	// - MysqlClearPassword
	// - MysqlDialog
	// It defaults to MysqlNativePassword.
	Method AuthMethodDescription
	// This mutex helps us prevent data races between the multiple updates of Entries.
	mu sync.Mutex
	// ClearText can be set to force the use of ClearText auth.
	ClearText bool
	// Entries contains the users, passwords and user data.
	Entries    map[string]*AuthServerConfigEntry
	ConfigName string
}

func (asc *AuthServerConfig) ValidClient(user, keyspace, ip string) bool {
	//if sqlparser.SystemSchema(keyspace) {
	//	return true
	//}
	entry, ok := asc.Entries[user]
	if !ok || len(entry.KeySpaces) == 0 || ip == "" {
		return false
	}
	if keyspace == "" {
		if len(entry.KeySpaces[0].WhiteIPs) == 0 {
			return true
		}
		return entry.KeySpaces[0].IPFilter.FilterIPString(ip)
	}
	for _, ks := range entry.KeySpaces {
		if ks.Name == keyspace || ks.Name == "*" {
			if len(ks.WhiteIPs) == 0 {
				return true
			}
			return ks.IPFilter.FilterIPString(ip)
		}
	}

	return false
}

// KeySpace db
type KeySpace struct {
	Name     string
	WhiteIPs []string
	IPFilter ipfilters.IPFilter
}

// AuthServerConfigEntry stores the values for a given user.
type AuthServerConfigEntry struct {
	Password            string
	MysqlNativePassword string
	UserData            string
	KeySpaces           []KeySpace
	AttachTo            string
	AuditKey            bool
	Privilege           uint16
	SourceHost          string
	Groups              []string
	ReadRole            int8
}

// InitAuthServerConfig Handles initializing the AuthServerConfig if necessary.
func InitAuthServerConfig() {
	// Check parameters.
	if mysqlAuthServerConfigFile == "" && mysqlAuthServerConfigString == "" {
		// Not configured, nothing to do.
		log.Infof("Not configuring AuthServerConfig, as mysql_auth_server_config_file and mysql_auth_server_config_string are empty")
		return
	}
	if mysqlAuthServerConfigFile != "" && mysqlAuthServerConfigString != "" {
		// Both parameters specified, can only use one.
		log.Exitf("Both mysql_auth_server_config_file and mysql_auth_server_config_string specified, can only use one.")
	}

	notifyutil.MonitorFile(mysqlAuthServerConfigFile, func() {
		UpdateAuthServerConfigFromParams(mysqlAuthServerConfigFile, mysqlAuthServerConfigString)
	})

	// Create and register auth server.
	RegisterAuthServerConfigFromParams(mysqlAuthServerConfigFile, mysqlAuthServerConfigString)
}

// NewAuthServerConfig returns a new empty AuthServerConfig.
func NewAuthServerConfig() *AuthServerConfig {
	asc := &AuthServerConfig{
		Method: AuthMethodDescription(configAuthMethod),
	}
	var authMethod AuthMethod
	switch AuthMethodDescription(configAuthMethod) {
	case MysqlNativePassword:
		authMethod = NewMysqlNativeAuthMethod(asc, asc)
	default:
		log.Exitf("Invalid mysql_config_auth_method value: only support mysql_native_password")
	}
	asc.methods = []AuthMethod{authMethod}
	return asc
}

// AuthMethods returns the implement auth methods for the config
func (asc *AuthServerConfig) AuthMethods() []AuthMethod {
	return asc.methods
}

// RegisterAuthServerConfigFromParams creates and registers a new
// AuthServerConfig, loaded for a JSON file or string. If file is set,
// it uses file. Otherwise, load the string. It log.Fatals out in case
// of error.
func RegisterAuthServerConfigFromParams(file, str string) {
	authServerConfig := NewAuthServerConfig() // Every time call the NewAuthServerConfig will create a new pointer
	jsonConfig := []byte(str)
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("Failed to read mysql_auth_server_config_file file: %v", err)
		}
		unCompressData, err := compressutil.UnCompressData(data)
		if err != nil {
			log.Fatalf("Failed to read mysql_auth_server_config_file file: %v", err)
		}
		jsonConfig = unCompressData
	}

	// Parse JSON config.
	//&authServerConfig.Entries  is Entries adress
	if err := json.Unmarshal(jsonConfig, &authServerConfig.Entries); err != nil {
		log.Fatalf("Error parsing auth server config: %v", err)
	}
	//load wirte ip list
	if len(authServerConfig.Entries) > 0 {
		for user, en := range authServerConfig.Entries {
			for index, ks := range en.KeySpaces {
				for _, ip := range ks.WhiteIPs {
					authServerConfig.Entries[user].KeySpaces[index].IPFilter.Load([]byte(ip))
				}
			}
		}
	}
	// And register the server.
	RegisterAuthServer("config", authServerConfig)
	log.Info("init authServerConfig successful")
}

// UpdateAuthServerConfigFromParams update
// AuthServerConfig, loaded for a JSON file or string. If file is set,
// it uses file. Otherwise, load the string. It just log.Error, do not exit
func UpdateAuthServerConfigFromParams(file, str string) {
	authServerConfig := NewAuthServerConfig() // Every time call the NewAuthServerConfig will create a new pointer
	jsonConfig := []byte(str)
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Errorf("Failed to read mysql_auth_server_config_file file: %v", err)
			return
		}
		unCompressData, err := compressutil.UnCompressData(data)
		if err != nil {
			log.Errorf("Failed to read mysql_auth_server_config_file file: %v", err)
			return
		}
		jsonConfig = unCompressData
	}

	// Parse JSON config.
	//&authServerConfig.Entries  is Entries adress
	if err := json.Unmarshal(jsonConfig, &authServerConfig.Entries); err != nil {
		log.Errorf("Error parsing auth server config: %v", err)
		return
	}

	if len(authServerConfig.Entries) > 0 {
		for user, en := range authServerConfig.Entries {
			for index, ks := range en.KeySpaces {
				for _, ip := range ks.WhiteIPs {
					authServerConfig.Entries[user].KeySpaces[index].IPFilter.Load([]byte(ip))
				}
			}
		}
	}
	// And register the server.
	RegisterAuthServer("config", authServerConfig)
	log.Info("update authServerConfig successful")
}

// UseClearText is part of the AuthServer interface.
func (asc *AuthServerConfig) UseClearText() bool {
	return asc.ClearText
}

// DefaultAuthMethodDescription returns always MysqlNativePassword
// for the client certificate authentication setup.
func (asc *AuthServerConfig) DefaultAuthMethodDescription() AuthMethodDescription {
	return MysqlNativePassword
}

// HandleUser is part of the UserValidator interface. We
// handle any user here since we don't check up front.
func (asc *AuthServerConfig) HandleUser(user string) bool {
	return true
}

// UserEntryWithHash implements password lookup based on a
// mysql_native_password hash that is negotiated with the client.
func (asc *AuthServerConfig) UserEntryWithHash(conn *Conn, salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, error) {
	// Find the entry.
	asc.mu.Lock()
	entry, ok := asc.Entries[user]
	asc.mu.Unlock()
	if !ok {
		return &ConfigUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	if entry.MysqlNativePassword != "" {
		hash, err := DecodeMysqlNativePasswordHex(entry.MysqlNativePassword)
		if err != nil {
			return &ConfigUserData{username: entry.UserData, groups: entry.Groups}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
		}
		isPass := VerifyHashedMysqlNativePassword(authResponse, salt, hash)
		if isPass {
			return &ConfigUserData{entry.UserData, entry.Groups}, nil
		}
	}

	encryptFromEnt := asc.Entries["encrypt_version"]
	ecnPassStr := ""

	// Depending on the version number of the useracl, decide which aseV method to execute.
	// In the future, if you add other encryption methods, you need to add an if option
	// method, such as aseV_002, for easy upgrade and degradation.
	if encryptFromEnt == nil {
		ecnPassStr = entry.Password
	} else if encryptFromEnt.UserData == "v_0001" {
		ecnPass, err := aseV001(entry.Password, []byte("akArIfh/a28N8w=="))
		if err != nil {
			return &ConfigUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v' error ses descryption", user)
		}
		ecnPassStr = ecnPass
	} else {
		return &ConfigUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v' error desencryption verssion", user)
	}

	computedAuthResponse := ScrambleMysqlNativePassword(salt, []byte(ecnPassStr))

	if !bytes.Equal(authResponse, computedAuthResponse) {
		return &ConfigUserData{}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	return &ConfigUserData{entry.UserData, entry.Groups}, nil
}

// aseV001 is the version 1
func aseV001(encPass string, key []byte) (string, error) {
	return AesDecrypt(encPass, key)
}

// ValidateClearText is part of the AuthServer interface.
func (asc *AuthServerConfig) ValidateClearText(user, password string) (string, error) {
	// Find the entry.
	asc.mu.Lock()
	entry, ok := asc.Entries[user]
	asc.mu.Unlock()
	if !ok {
		return "", NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	// Validate the password.
	if entry.Password != password {
		return "", NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	return entry.UserData, nil
}

// GetPrivilege return the user privilege.
func (asc *AuthServerConfig) GetPrivilege(user string) (uint16, error) {
	// Find the entry.
	asc.mu.Lock()
	entry, ok := asc.Entries[user]
	asc.mu.Unlock()
	if !ok {
		return 0, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	return entry.Privilege, nil
}

// GetUserKeyspaces return the user keyspace.
func (asc *AuthServerConfig) GetUserKeyspaces(user string) ([]string, error) {
	userKeyspaces := make([]string, 0)
	// Find the entry.
	asc.mu.Lock()
	entry, ok := asc.Entries[user]
	asc.mu.Unlock()
	if !ok {
		return nil, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	for _, v := range entry.KeySpaces {
		userKeyspaces = append(userKeyspaces, v.Name)
	}
	return userKeyspaces, nil
}

// GetKeyspace get keyspace via user
func (asc *AuthServerConfig) GetKeyspace(user string) ([]string, error) {
	entry, ok := asc.Entries[user]
	if !ok {
		return nil, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	if len(entry.KeySpaces) == 0 {
		return nil, nil
	}
	l := make([]string, len(entry.KeySpaces))

	for idx, keyspace := range entry.KeySpaces {
		l[idx] = keyspace.Name
	}
	return l, nil
}

// GetKeyspace get keyspace via user
func (asc *AuthServerConfig) GetRoleType(user string) (int8, error) {
	entry, ok := asc.Entries[user]
	if !ok {
		return 0, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	if len(entry.KeySpaces) == 0 {
		return 0, nil
	}
	return entry.ReadRole, nil
}

// GetPassword get password via user
func (asc *AuthServerConfig) GetPassword(user string) (string, error) {
	// Find the entry.
	entry, ok := asc.Entries[user]
	if !ok {
		return "", NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	encryptFromEnt := asc.Entries["encrypt_version"]
	if encryptFromEnt == nil {
		return entry.Password, nil
	}

	if encryptFromEnt.UserData == "v_0001" {
		ecnPass, err := aseV001(entry.Password, []byte("akArIfh/a28N8w=="))
		if err != nil {
			return "", NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v' error ses descryption", user)
		}
		return ecnPass, nil
	}

	return "", NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "unsupported encrypt version for user :%v", user)
}

// ConfigUserData holds the username and groups
type ConfigUserData struct {
	username string
	groups   []string
}

// Get returns the wrapped username and groups
func (sud *ConfigUserData) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: sud.username, Groups: sud.groups}
}

// PKCS7Padding padding
func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

// PKCS7UnPadding unpadding
func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

// AesEncrypt encrypt of aes
func AesEncrypt(origData, key []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	blockSize := block.BlockSize()
	origData = PKCS7Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize])
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)
	return base64.StdEncoding.EncodeToString(crypted), nil
}

// AesDecrypt descrype of aes
func AesDecrypt(cryptedStr string, key []byte) (string, error) {
	crypted, err := base64.StdEncoding.DecodeString(cryptedStr)
	if err != nil {
		return "", err

	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS7UnPadding(origData)
	return string(origData), nil
}

// GetKeyspace return the user keyspace.
func (ascc *AuthServerClientCert) GetKeyspace(user string) ([]string, error) {
	return nil, nil
}

// GetPassword return the user password.
func (ascc *AuthServerClientCert) GetPassword(user string) (string, error) {
	return "", nil
}
