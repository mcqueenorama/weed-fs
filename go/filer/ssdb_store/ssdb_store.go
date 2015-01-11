package ssdb_store

import (
	// "fmt"
	"errors"
	"strings"
	"strconv"

	"github.com/mcqueenorama/weed-fs/go/glog"
	ssdb "github.com/ssdb/gossdb/ssdb"
)

type SsdbStore struct {
	Client *ssdb.Client
}

func NewSsdbStore(hostPort string) (*SsdbStore, error) {

    // var host string
    // var port int

	glog.V(0).Infof("Starting new ssdb_store:hostPort:%s:", hostPort)
	// n, err := fmt.Sscanf(hostPort, "%s:%d", &host, &port)
	// if(err != nil){
	// 	glog.V(0).Infof("ssdb_store:failed to parse hostPort:%s:n:%d:err:%v:", hostPort, n, err)
 //        return nil, err
 //    }

	_hostPort := strings.Split(hostPort, ":")

	if(len(_hostPort) != 2) {
		glog.V(0).Infof("ssdb_store:failed to parse hostPort:%s:", hostPort)
        return nil, errors.New("NewSsdbStore couldn't parse hostPort string: " + hostPort)
    }

	host := _hostPort[0]
	port, err := strconv.Atoi(_hostPort[1])
	if(err != nil) {
		glog.V(0).Infof("ssdb_store:failed to parse port from hostPort:%s:err:%v:", hostPort, err)
        return nil, errors.New("NewSsdbStore couldn't parse hostPort string: " + hostPort)
    }
	// glog.V(0).Infof("Starting new ssdb_store:host:%s:port:%d:", host, port)

    // if n != 2 {
    	// return nil, errors.New("NewSsdbStore couldn't parse hostPort string: " + hostPort)
    // }
	client, err := ssdb.Connect(host, port)
	if(err != nil){
        return nil, err
    }

	return &SsdbStore{Client: client}, nil
}

func (s *SsdbStore) Get(fullFileName string) (fid string, err error) {
	if s.Client == nil {
		return "", errors.New("Get called with nil client")
	}
	_fid, err := s.Client.Get(fullFileName)

    var ok bool
	fid, ok = _fid.(string)
	if ! ok {

		//make this msg better
		return "", errors.New("Get got a non-string")
			
	}

	glog.V(0).Infof("ssdb_store:get:%s:fid:%s:", fullFileName, fid)
	return
	// return _fid.(string), err
}
func (s *SsdbStore) Put(fullFileName string, fid string) (err error) {
	if s.Client == nil {
		return errors.New("Set called with nil client")
	}
	_, err = s.Client.Set(fullFileName, fid)

	glog.V(0).Infof("ssdb_store:put:%s:fid:%s:", fullFileName, fid)
	return
}

// Currently the fid is not returned
func (s *SsdbStore) Delete(fullFileName string) (fid string, err error) {
	if s.Client == nil {
		return "", errors.New("Delete called with nil client")
	}
	_, err = s.Client.Del(fullFileName)
	if err != nil {
		return
	}

	return "", err
}

func (s *SsdbStore) Close() {
	if s.Client != nil {
		s.Client.Close()
	}
}
