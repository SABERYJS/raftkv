package main

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type kvSnapshot struct {
	raft.FSMSnapshot
	store *kvStore
}

func NewSnapshot(s *kvStore) *kvSnapshot {
	return &kvSnapshot{store: s}
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (snapshot *kvSnapshot) Persist(sink raft.SnapshotSink) error {
	logger.Info("snapshot persist request")
	var kws map[string]string
	snapshot.store.lock.Lock()
	kws = snapshot.store.kws
	snapshot.store.lock.Unlock()
	bytes, err := json.Marshal(kws)
	if err != nil {
		logger.Error("jsonfailed", zap.Error(err))
		return err
	}
	_, err = sink.Write(bytes)
	if err != nil {
		logger.Error("sink write failed", zap.Error(err))
		return err
	}
	return nil
}

// Release is invoked when we are finished with the snapshot.
func (snapshot *kvSnapshot) Release() {
}
