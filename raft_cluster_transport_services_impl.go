package main

import (
	"context"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"raftapp/protos"
)

type RaftClusterTransportServerImpl struct {
	protos.UnimplementedRaftTransportServer
	rafthandler *RaftHandler
	bridge      *RaftBridge
}

func NewRaftTransportServer(handler *RaftHandler, bridge *RaftBridge) *RaftClusterTransportServerImpl {
	return &RaftClusterTransportServerImpl{rafthandler: handler, bridge: bridge}
}

// AppendEntriesPipeline opens an AppendEntries message stream.
func (rts *RaftClusterTransportServerImpl) AppendEntriesPipeline(request protos.RaftTransport_AppendEntriesPipelineServer) error {
	for {
		msg, err := request.Recv()
		if err != nil {
			return err
		}
		resp, err := rts.bridge.handleGrpcRequest(decodeAppendEntriesRequest(msg), nil)
		if err != nil {
			// TODO(quis): One failure doesn't have to break the entire stream?
			// Or does it all go wrong when it's out of order anyway?
			return err
		}
		if err := request.Send(encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse))); err != nil {
			return err
		}
	}
}

// AppendEntries performs a single append entries request / response.
func (rts *RaftClusterTransportServerImpl) AppendEntries(ctx context.Context, request *protos.AppendEntriesRequest) (*protos.AppendEntriesResponse, error) {
	resp, err := rts.bridge.handleGrpcRequest(decodeAppendEntriesRequest(request), nil)
	if err != nil {
		return nil, err
	} else {
		return encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse)), nil
	}
}

// RequestVote is the command used by a candidate to ask a Raft peer for a vote in an election.
func (rts *RaftClusterTransportServerImpl) RequestVote(ctx context.Context, request *protos.RequestVoteRequest) (*protos.RequestVoteResponse, error) {
	logger.Info("RequestVote", zap.Reflect("payload", request))
	resp, err := rts.bridge.handleGrpcRequest(decodeRequestVoteRequest(request), nil)
	if err != nil {
		logger.Error("RequestVoteFailed", zap.Error(err))
		return nil, err
	} else {
		requestVoteResponse := encodeRequestVoteResponse(resp.(*raft.RequestVoteResponse))
		logger.Info("RequestVoteResponse", zap.Reflect("payload", requestVoteResponse))
		return requestVoteResponse, nil
	}
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (rts *RaftClusterTransportServerImpl) TimeoutNow(ctx context.Context, request *protos.TimeoutNowRequest) (*protos.TimeoutNowResponse, error) {
	resp, err := rts.bridge.handleGrpcRequest(decodeTimeoutNowRequest(request), nil)
	if err != nil {
		return nil, err
	} else {
		return encodeTimeoutNowResponse(resp.(*raft.TimeoutNowResponse)), nil
	}
}

// InstallSnapshot is the command sent to a Raft peer to bootstrap its log (and state machine) from a snapshot on another peer.
func (rts *RaftClusterTransportServerImpl) InstallSnapshot(request protos.RaftTransport_InstallSnapshotServer) error {
	isr, err := request.Recv()
	if err != nil {
		return err
	}
	resp, err := rts.bridge.handleGrpcRequest(decodeInstallSnapshotRequest(isr), &snapshotStream{request, isr.GetData()})
	if err != nil {
		return err
	}
	return request.SendAndClose(encodeInstallSnapshotResponse(resp.(*raft.InstallSnapshotResponse)))
}

type snapshotStream struct {
	s protos.RaftTransport_InstallSnapshotServer

	buf []byte
}

func (s *snapshotStream) Read(b []byte) (int, error) {
	if len(s.buf) > 0 {
		n := copy(b, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}
	m, err := s.s.Recv()
	if err != nil {
		return 0, err
	}
	n := copy(b, m.GetData())
	if n < len(m.GetData()) {
		s.buf = m.GetData()[n:]
	}
	return n, nil
}

func isHeartbeat(command interface{}) bool {
	req, ok := command.(*raft.AppendEntriesRequest)
	if !ok {
		return false
	}
	return req.Term != 0 && len(req.Leader) != 0 && req.PrevLogEntry == 0 && req.PrevLogTerm == 0 && len(req.Entries) == 0 && req.LeaderCommitIndex == 0
}
