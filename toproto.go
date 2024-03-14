package main

import (
	"github.com/hashicorp/raft"
	"raftapp/protos"
)

func encodeAppendEntriesRequest(s *raft.AppendEntriesRequest) *protos.AppendEntriesRequest {
	return &protos.AppendEntriesRequest{
		RpcHeader:         encodeRPCHeader(s.RPCHeader),
		Term:              s.Term,
		Leader:            s.Leader,
		PrevLogEntry:      s.PrevLogEntry,
		PrevLogTerm:       s.PrevLogTerm,
		Entries:           encodeLogs(s.Entries),
		LeaderCommitIndex: s.LeaderCommitIndex,
	}
}

func encodeRPCHeader(s raft.RPCHeader) *protos.RPCHeader {
	return &protos.RPCHeader{
		ProtocolVersion: int64(s.ProtocolVersion),
		Id:              s.ID,
		Addr:            s.Addr,
	}
}

func encodeLogs(s []*raft.Log) []*protos.Log {
	ret := make([]*protos.Log, len(s))
	for i, l := range s {
		ret[i] = encodeLog(l)
	}
	return ret
}

func encodeLog(s *raft.Log) *protos.Log {
	return &protos.Log{
		Index:      s.Index,
		Term:       s.Term,
		Type:       encodeLogType(s.Type),
		Data:       s.Data,
		Extensions: s.Extensions,
		AppendedAt: s.AppendedAt.Unix(),
	}
}

func encodeLogType(s raft.LogType) protos.Log_LogType {
	switch s {
	case raft.LogCommand:
		return protos.Log_LOG_COMMAND
	case raft.LogNoop:
		return protos.Log_LOG_NOOP
	case raft.LogAddPeerDeprecated:
		return protos.Log_LOG_ADD_PEER_DEPRECATED
	case raft.LogRemovePeerDeprecated:
		return protos.Log_LOG_REMOVE_PEER_DEPRECATED
	case raft.LogBarrier:
		return protos.Log_LOG_BARRIER
	case raft.LogConfiguration:
		return protos.Log_LOG_CONFIGURATION
	default:
		panic("invalid LogType")
	}
}

func encodeAppendEntriesResponse(s *raft.AppendEntriesResponse) *protos.AppendEntriesResponse {
	return &protos.AppendEntriesResponse{
		RpcHeader:      encodeRPCHeader(s.RPCHeader),
		Term:           s.Term,
		LastLog:        s.LastLog,
		Success:        s.Success,
		NoRetryBackoff: s.NoRetryBackoff,
	}
}

func encodeRequestVoteRequest(s *raft.RequestVoteRequest) *protos.RequestVoteRequest {
	return &protos.RequestVoteRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		Term:               s.Term,
		Candidate:          s.Candidate,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		LeadershipTransfer: s.LeadershipTransfer,
	}
}

func encodeRequestVoteResponse(s *raft.RequestVoteResponse) *protos.RequestVoteResponse {
	return &protos.RequestVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Peers:     s.Peers,
		Granted:   s.Granted,
	}
}

func encodeInstallSnapshotRequest(s *raft.InstallSnapshotRequest) *protos.InstallSnapshotRequest {
	return &protos.InstallSnapshotRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		SnapshotVersion:    int64(s.SnapshotVersion),
		Term:               s.Term,
		Leader:             s.Leader,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		Peers:              s.Peers,
		Configuration:      s.Configuration,
		ConfigurationIndex: s.ConfigurationIndex,
		Size:               s.Size,
	}
}

func encodeInstallSnapshotResponse(s *raft.InstallSnapshotResponse) *protos.InstallSnapshotResponse {
	return &protos.InstallSnapshotResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Success:   s.Success,
	}
}

func encodeTimeoutNowRequest(s *raft.TimeoutNowRequest) *protos.TimeoutNowRequest {
	return &protos.TimeoutNowRequest{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func encodeTimeoutNowResponse(s *raft.TimeoutNowResponse) *protos.TimeoutNowResponse {
	return &protos.TimeoutNowResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}
