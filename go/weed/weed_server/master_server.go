package weed_server

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/mcqueenorama/weed-fs/go/glog"
	"github.com/mcqueenorama/weed-fs/go/security"
	"github.com/mcqueenorama/weed-fs/go/sequence"
	"github.com/mcqueenorama/weed-fs/go/topology"
	"github.com/mcqueenorama/weed-fs/go/util"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
)

type MasterServer struct {
	port                    int
	metaFolder              string
	volumeSizeLimitMB       uint
	pulseSeconds            int
	defaultReplicaPlacement string
	garbageThreshold        string

	Topo   *topology.Topology
	vg     *topology.VolumeGrowth
	vgLock sync.Mutex

	bounedLeaderChan chan int
}

func NewMasterServer(r *mux.Router, port int, metaFolder string,
	volumeSizeLimitMB uint,
	pulseSeconds int,
	confFile string,
	defaultReplicaPlacement string,
	garbageThreshold string,
	whiteList []string,
	secureKey string,
) *MasterServer {
	ms := &MasterServer{
		port:                    port,
		volumeSizeLimitMB:       volumeSizeLimitMB,
		pulseSeconds:            pulseSeconds,
		defaultReplicaPlacement: defaultReplicaPlacement,
		garbageThreshold:        garbageThreshold,
	}
	ms.bounedLeaderChan = make(chan int, 16)
	seq := sequence.NewMemorySequencer()
	var e error
	if ms.Topo, e = topology.NewTopology("topo", confFile, seq,
		uint64(volumeSizeLimitMB)*1024*1024, pulseSeconds); e != nil {
		glog.Fatalf("cannot create topology:%s", e)
	}
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", volumeSizeLimitMB, "MB")

	guard := security.NewGuard(whiteList, secureKey)

	r.HandleFunc("/dir/assign", ms.proxyToLeader(guard.Secure(ms.dirAssignHandler)))
	r.HandleFunc("/dir/lookup", ms.proxyToLeader(guard.Secure(ms.dirLookupHandler)))
	r.HandleFunc("/dir/join", ms.proxyToLeader(guard.Secure(ms.dirJoinHandler)))
	r.HandleFunc("/dir/status", ms.proxyToLeader(guard.Secure(ms.dirStatusHandler)))
	r.HandleFunc("/col/delete", ms.proxyToLeader(guard.Secure(ms.collectionDeleteHandler)))
	r.HandleFunc("/vol/lookup", ms.proxyToLeader(guard.Secure(ms.volumeLookupHandler)))
	r.HandleFunc("/vol/grow", ms.proxyToLeader(guard.Secure(ms.volumeGrowHandler)))
	r.HandleFunc("/vol/status", ms.proxyToLeader(guard.Secure(ms.volumeStatusHandler)))
	r.HandleFunc("/vol/vacuum", ms.proxyToLeader(guard.Secure(ms.volumeVacuumHandler)))
	r.HandleFunc("/submit", guard.Secure(ms.submitFromMasterServerHandler))
	r.HandleFunc("/delete", guard.Secure(ms.deleteFromMasterServerHandler))
	r.HandleFunc("/{fileId}", ms.redirectHandler)
	r.HandleFunc("/stats/counter", guard.Secure(statsCounterHandler))
	r.HandleFunc("/stats/memory", guard.Secure(statsMemoryHandler))

	ms.Topo.StartRefreshWritableVolumes(garbageThreshold)

	return ms
}

func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	ms.Topo.RaftServer = raftServer.raftServer
	ms.Topo.RaftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "becomes leader.")
		}
	})
	if ms.Topo.IsLeader() {
		glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", "I am the leader!")
	} else {
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "is the leader.")
		}
	}
}

func (ms *MasterServer) proxyToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.Topo.IsLeader() {
			f(w, r)
		} else if ms.Topo.RaftServer != nil && ms.Topo.RaftServer.Leader() != "" {
			ms.bounedLeaderChan <- 1
			defer func() { <-ms.bounedLeaderChan }()
			targetUrl, err := url.Parse("http://" + ms.Topo.RaftServer.Leader())
			if err != nil {
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Leader URL http://%s Parse Error: %v", ms.Topo.RaftServer.Leader(), err))
				return
			}
			glog.V(4).Infoln("proxying to leader", ms.Topo.RaftServer.Leader())
			proxy := httputil.NewSingleHostReverseProxy(targetUrl)
			proxy.Transport = util.Transport
			proxy.ServeHTTP(w, r)
		} else {
			//drop it to the floor
			//writeJsonError(w, r, errors.New(ms.Topo.RaftServer.Name()+" does not know Leader yet:"+ms.Topo.RaftServer.Leader()))
		}
	}
}
