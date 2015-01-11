package storage

import (
	"fmt"
	"github.com/mcqueenorama/weed-fs/go/operation"
)

type VolumeInfo struct {
	Id               VolumeId
	Size             uint64
	ReplicaPlacement *ReplicaPlacement
	Ttl              *TTL
	Collection       string
	Version          Version
	FileCount        int
	DeleteCount      int
	DeletedByteCount uint64
	ReadOnly         bool
}

func NewVolumeInfo(m *operation.VolumeInformationMessage) (vi VolumeInfo, err error) {
	vi = VolumeInfo{
		Id:               VolumeId(*m.Id),
		Size:             *m.Size,
		Collection:       *m.Collection,
		FileCount:        int(*m.FileCount),
		DeleteCount:      int(*m.DeleteCount),
		DeletedByteCount: *m.DeletedByteCount,
		ReadOnly:         *m.ReadOnly,
		Version:          Version(*m.Version),
	}
	rp, e := NewReplicaPlacementFromByte(byte(*m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	vi.Ttl = LoadTTLFromUint32(*m.Ttl)
	return vi, nil
}

func (vi VolumeInfo) String() string {
	return fmt.Sprintf("Id:%s, Size:%d, ReplicaPlacement:%s, Collection:%s, Version:%v, FileCount:%d, DeleteCount:%d, DeletedByteCount:%d, ReadOnly:%v", vi.Id, vi.Size, vi.ReplicaPlacement, vi.Collection, vi.Version, vi.FileCount, vi.DeleteCount, vi.DeletedByteCount, vi.ReadOnly)
}
