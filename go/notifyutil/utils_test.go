package notifyutil

import (
	"strings"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

//func TestMonitFile(t *testing.T) {
//	MonitFileTest(t,"/export/monitor_sheng", func() {
//		t.Log("监控目录成功了！！")
//	})
//}

func MonitFileTest(t *testing.T, file string, callback func()) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		t.Error(err, " fsnotify.NewWatcher() failed !")
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Error("MonitorFile is die! ", err)
			}
		}()

		for {
			select {
			case event := <-watcher.Events:
				trigFile := event.Name
				t.Log("monitor file event info ", event.Name)
				trigFileOfSplit := strings.Split(trigFile, "/")
				trigFileName := strings.Split(trigFile, "/")[len(trigFileOfSplit)-1]
				// just monit write event
				if event.Op&fsnotify.Write == fsnotify.Write && trigFileName == "monitor_sheng" {
					t.Log("modified file:", event.Name)
					callback()
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					t.Log("error:", err.Error())
					return
				}
				// Make sure could not stop server when met the error
				t.Log("error:", err.Error())
			}
		}
	}()

	// Make sure could not stop server when met the error
	if err := watcher.Add("/export"); err != nil {
		t.Error(err, "watcher.Add(parentPath)  ")
	}

	time.Sleep(1000 * time.Hour)
}
