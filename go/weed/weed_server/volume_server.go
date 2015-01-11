package weed_server

import (
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/mcqueenorama/weed-fs/go/glog"
	"github.com/mcqueenorama/weed-fs/go/security"
	"github.com/mcqueenorama/weed-fs/go/storage"
)

type VolumeServer struct {
	masterNode   string
	pulseSeconds int
	dataCenter   string
	rack         string
	store        *storage.Store
	guard        *security.Guard

	FixJpgOrientation bool
}

func NewVolumeServer(r *http.ServeMux, ip string, port int, publicIp string, folders []string, maxCounts []int,
	masterNode string, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	fixJpgOrientation bool) *VolumeServer {
	publicUrl := publicIp + ":" + strconv.Itoa(port)
	vs := &VolumeServer{
		masterNode:        masterNode,
		pulseSeconds:      pulseSeconds,
		dataCenter:        dataCenter,
		rack:              rack,
		FixJpgOrientation: fixJpgOrientation,
	}
	vs.store = storage.NewStore(port, ip, publicUrl, folders, maxCounts)

	vs.guard = security.NewGuard(whiteList, "")

	r.HandleFunc("/status", vs.guard.Secure(vs.statusHandler))
	r.HandleFunc("/admin/assign_volume", vs.guard.Secure(vs.assignVolumeHandler))
	r.HandleFunc("/admin/vacuum_volume_check", vs.guard.Secure(vs.vacuumVolumeCheckHandler))
	r.HandleFunc("/admin/vacuum_volume_compact", vs.guard.Secure(vs.vacuumVolumeCompactHandler))
	r.HandleFunc("/admin/vacuum_volume_commit", vs.guard.Secure(vs.vacuumVolumeCommitHandler))
	r.HandleFunc("/admin/freeze_volume", vs.guard.Secure(vs.freezeVolumeHandler))
	r.HandleFunc("/admin/delete_collection", vs.guard.Secure(vs.deleteCollectionHandler))
	r.HandleFunc("/stats/counter", vs.guard.Secure(statsCounterHandler))
	r.HandleFunc("/stats/memory", vs.guard.Secure(statsMemoryHandler))
	r.HandleFunc("/stats/disk", vs.guard.Secure(vs.statsDiskHandler))
	r.HandleFunc("/delete", vs.guard.Secure(vs.batchDeleteHandler))
	r.HandleFunc("/", vs.storeHandler)

	go func() {
		connected := true
		vs.store.SetBootstrapMaster(vs.masterNode)
		vs.store.SetDataCenter(vs.dataCenter)
		vs.store.SetRack(vs.rack)
		for {
			master, err := vs.store.Join()
			if err == nil {
				if !connected {
					connected = true
					glog.V(0).Infoln("Volume Server Connected with master at", master)
				}
			} else {
				glog.V(4).Infoln("Volume Server Failed to talk with master:", err.Error())
				if connected {
					connected = false
				}
			}
			if connected {
				time.Sleep(time.Duration(float32(vs.pulseSeconds*1e3)*(1+rand.Float32())) * time.Millisecond)
			} else {
				time.Sleep(time.Duration(float32(vs.pulseSeconds*1e3)*0.25) * time.Millisecond)
			}
		}
	}()

	return vs
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}
