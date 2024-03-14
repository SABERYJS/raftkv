package main

import (
	"context"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
	"raftapp/protos"
	"time"
)

type ProtocolsServicesImpl struct {
	protos.ProtocolsServiceServer
	raftnode *raft.Raft
	store    *kvStore
}

func NewProtocolsServices(store *kvStore, rnode *raft.Raft) *ProtocolsServicesImpl {
	return &ProtocolsServicesImpl{raftnode: rnode, store: store}
}

func (impl *ProtocolsServicesImpl) CreateKey(ctx context.Context, request *protos.CreateKeyRequest) (*protos.CreateKeyResponse, error) {
	bytes, err := proto.Marshal(request.Data)
	if err != nil {
		return nil, err
	}
	future := impl.raftnode.Apply(bytes, time.Second*3)
	err = future.Error()
	if err != nil {
		return nil, err
	}
	return &protos.CreateKeyResponse{KeysCnt: int32(impl.store.size()), Ret: 0}, nil
}

func (impl *ProtocolsServicesImpl) QueryKey(ctx context.Context, request *protos.QueryKeyRequest) (*protos.QueryKeyResponse, error) {
	value, exist := impl.store.get(request.Key)
	result := &protos.QueryKeyResponse{Value: value}
	if exist {
		result.Ret = 1
		result.Value = value
	} else {
		result.Ret = -1
	}
	return result, nil
}
