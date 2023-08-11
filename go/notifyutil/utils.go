package notifyutil

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"

	"github.com/fsnotify/fsnotify"

	"vitess.io/vitess/go/vt/log"
)

var configType bool

func init() {
	servenv.OnParseFor("vtgate", func(fs *pflag.FlagSet) {
		fs.BoolVar(&configType, "use_configmap", true, "JSON File to read the users/passwords from.")
	})
}

func substr(s string, pos, length int) (string, error) {
	if s == "" || length < 0 {
		log.Errorf("配置文件路径错误或者文件不存在: '%s'", s)
		return s, fmt.Errorf("配置文件路径: %s路径错误或者文件不存在", s)
	}
	runes := []rune(s)
	l := pos + length
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l]), nil
}

// MonitorFile monitor changing of file by monitor parent Folder of file.
func MonitorFile(file string, callback func()) {
	parentPath, err := substr(file, 0, strings.LastIndex(file, "/"))
	if err != nil {
		return
	}

	pathOfSplit := strings.Split(file, "/")
	userConfName := pathOfSplit[len(pathOfSplit)-1]

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf(" fsnotify.NewWatcher() failed %s", err.Error())
	}

	go func() {
		defer func() {
			if recover() != nil {
				log.Errorf("MonitorFile expected panic, but there wasn't one")
			}
		}()

		for {
			select {
			case event := <-watcher.Events:
				trigFile := event.Name
				log.Infof("monitor file event info %s", event.Name)
				trigFileOfSplit := strings.Split(trigFile, "/")
				trigFileName := strings.Split(trigFile, "/")[len(trigFileOfSplit)-1]
				if configType {
					// configMap way.
					userConfName = "..data"
					if event.Op&fsnotify.Create == fsnotify.Create && trigFileName == userConfName {
						log.Infof("configMap modified file: %s", event.Name)
						callback()
					}
				} else if event.Op&fsnotify.Write == fsnotify.Write && trigFileName == userConfName {
					log.Infof("traditional modified file %s:", event.Name)
					callback()
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Infof("error: %s", err.Error())
					return
				}
				// Make sure could not stop server when met the error.
				log.Infof("error: %s", err.Error())
			}
		}
	}()

	// Make sure could not stop server when met the error.
	if err := watcher.Add(parentPath); err != nil {
		log.Errorf("watcher.Add(parentPath) %s", err.Error())
	}
}
