package weed_server

import (
	"net/http"
	"strconv"

	"github.com/mcqueenorama/weed-fs/go/filer"
	"github.com/mcqueenorama/weed-fs/go/filer/cassandra_store"
	"github.com/mcqueenorama/weed-fs/go/filer/embedded_filer"
	"github.com/mcqueenorama/weed-fs/go/filer/flat_namespace"
	"github.com/mcqueenorama/weed-fs/go/filer/redis_store"
	"github.com/mcqueenorama/weed-fs/go/filer/ssdb_store"
	"github.com/mcqueenorama/weed-fs/go/glog"
)

type FilerServer struct {
	port               string
	master             string
	collection         string
	defaultReplication string
	redirectOnRead     bool
	filer              filer.Filer
}

func NewFilerServer(r *http.ServeMux, port int, master string, dir string, collection string,
	replication string, redirectOnRead bool,
	cassandra_server string, cassandra_keyspace string,
	redis_server string, redis_database int,
	ssdb_server string,
) (fs *FilerServer, err error) {
	fs = &FilerServer{
		master:             master,
		collection:         collection,
		defaultReplication: replication,
		redirectOnRead:     redirectOnRead,
		port:               ":" + strconv.Itoa(port),
	}

	glog.V(0).Infof("Starting NewFilerServer:cassandra_store:%s:redis_server:%s:ssdb_server:%s:", cassandra_server, redis_server, ssdb_server)

	if cassandra_server != "" {
		cassandra_store, err := cassandra_store.NewCassandraStore(cassandra_keyspace, cassandra_server)
		if err != nil {
			glog.Fatalf("Can not connect to cassandra server %s with keyspace %s: %v", cassandra_server, cassandra_keyspace, err)
		}
		fs.filer = flat_namespace.NewFlatNamesapceFiler(master, cassandra_store)
	} else if redis_server != "" {
		redis_store := redis_store.NewRedisStore(redis_server, redis_database)
		fs.filer = flat_namespace.NewFlatNamesapceFiler(master, redis_store)
	} else if ssdb_server != "" {
		ssdb_store, err := ssdb_store.NewSsdbStore(ssdb_server)
		if err != nil {
			glog.Fatalf("Can not connect to ssdb server:%s: %v", ssdb_server, err)
		}
		glog.V(0).Infof("Started NewSsdbStore:ssdb_server:ssdb_store:%v:", ssdb_store)
		fs.filer = flat_namespace.NewFlatNamesapceFiler(master, ssdb_store)
	} else {
		if fs.filer, err = embedded_filer.NewFilerEmbedded(master, dir); err != nil {
			glog.Fatalf("Can not start filer in dir %s : %v", err)
			return
		}

		r.HandleFunc("/admin/mv", fs.moveHandler)
	}

	r.HandleFunc("/", fs.filerHandler)

	return fs, nil
}
