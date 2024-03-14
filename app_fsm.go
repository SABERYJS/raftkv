package main

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"io"
	"raftapp/protos"
	"sync"
)

type kvStore struct {
	raft.FSM
	lock sync.Mutex
	kws  map[string]string
}

func NewKvStore() *kvStore {
	return &kvStore{kws: map[string]string{}}
}

func (s *kvStore) size() int {
	defer s.lock.Unlock()
	s.lock.Lock()
	return len(s.kws)
}

func (s *kvStore) get(key string) (string, bool) {
	defer s.lock.Unlock()
	s.lock.Lock()
	if v, ok := s.kws[key]; ok {
		return v, true
	} else {
		return "", false
	}
}

func (s *kvStore) Apply(log *raft.Log) interface{} {
	logger.Info("applylog", zap.Uint64("index", log.Index), zap.Uint64("term", log.Term))
	resp := &protos.CreateKeyResponse{}
	pair := &protos.KvPair{}
	err := proto.Unmarshal(log.Data, pair)
	if err != nil {
		resp.Ret = -1
		return resp
	}
	s.kws[pair.Key] = pair.Value
	resp.Ret = 1
	//logger.Info("apply ok")
	return resp
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running. Generally this means Snapshot should
// only capture a pointer to the state, and any expensive IO should happen
// as part of FSMSnapshot.Persist.
//
// Apply and Snapshot are always called from the same thread, but Apply will
// be called concurrently with FSMSnapshot.Persist. This means the FSM should
// be implemented to allow for concurrent updates while a snapshot is happening.
func (s *kvStore) Snapshot() (raft.FSMSnapshot, error) {
	kvs := NewSnapshot(s)
	return kvs, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (s *kvStore) Restore(snapshot io.ReadCloser) error {
	logger.Info("restore_snapshot")
	bytes, err := io.ReadAll(snapshot)
	if err != nil && err != io.EOF {
		return err
	}
	err = json.Unmarshal(bytes, &s.kws)
	if err != nil {
		logger.Error("jsonfailed", zap.Error(err))
		return err
	}
	return nil
}
