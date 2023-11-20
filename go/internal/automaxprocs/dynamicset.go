package automaxprocs

import (
	"github.com/fsnotify/fsnotify"

	"vitess.io/vitess/go/vt/log"
)

// DynamicSetCPUS - Dynamically modify parameters
func DynamicSetGOMAXPROCS(mtype int) {
	//创建一个监控对象
	watch, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("fsnotify.NewWatcher error:%v", err)
		return
	}
	defer watch.Close()
	//添加要监控的对象，文件或文件夹
	err = watch.Add("/sys/fs/cgroup/cpuacct/cpu.cfs_period_us")
	if err != nil {
		log.Errorf("watch.Add error:%v", err)
		return
	}
	//我们另启一个goroutine来处理监控对象的事件
	go func() {
		for {
			select {
			case ev := <-watch.Events:
				{
					if ev.Op&fsnotify.Write == fsnotify.Write {
						log.Infof("detach file write ,reset binds")
						SetGOMAXPROCS(mtype)
					}
				}
			case err := <-watch.Errors:
				{
					log.Errorf("watch.Errors : %v", err)
					return
				}
			}
		}
	}()
	select {}
}
