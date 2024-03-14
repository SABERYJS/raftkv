package main

import (
	"context"
	"github.com/hashicorp/raft"
	"raftapp/protos"
)

type RaftAdminServer struct {
	protos.UnimplementedRaftAdminServer
	raftnode *raft.Raft
}

func NewRaftAdminServer(rn *raft.Raft) *RaftAdminServer {
	return &RaftAdminServer{raftnode: rn}
}

func (ras *RaftAdminServer) AddNonvoter(ctx context.Context, request *protos.AddNonvoterRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) AddVoter(ctx context.Context, request *protos.AddVoterRequest) (*protos.Future, error) {
	ras.raftnode.AddVoter(raft.ServerID(request.Id), raft.ServerAddress(request.Address), request.PreviousIndex, 0)
	return nil, nil
}
func (ras *RaftAdminServer) AppliedIndex(ctx context.Context, request *protos.AppliedIndexRequest) (*protos.AppliedIndexResponse, error) {
	return nil, nil
}
func (ras *RaftAdminServer) ApplyLog(ctx context.Context, request *protos.ApplyLogRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) Barrier(ctx context.Context, request *protos.BarrierRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) DemoteVoter(ctx context.Context, request *protos.DemoteVoterRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) GetConfiguration(ctx context.Context, request *protos.GetConfigurationRequest) (*protos.GetConfigurationResponse, error) {
	return nil, nil
}
func (ras *RaftAdminServer) LastContact(ctx context.Context, request *protos.LastContactRequest) (*protos.LastContactResponse, error) {
	return nil, nil
}
func (ras *RaftAdminServer) LastIndex(ctx context.Context, request *protos.LastIndexRequest) (*protos.LastIndexResponse, error) {
	return nil, nil
}
func (ras *RaftAdminServer) Leader(ctx context.Context, request *protos.LeaderRequest) (*protos.LeaderResponse, error) {
	return nil, nil
}
func (ras *RaftAdminServer) LeadershipTransfer(ctx context.Context, request *protos.LeadershipTransferRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) LeadershipTransferToServer(ctx context.Context, request *protos.LeadershipTransferToServerRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) RemoveServer(ctx context.Context, request *protos.RemoveServerRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) Shutdown(ctx context.Context, request *protos.ShutdownRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) Snapshot(ctx context.Context, request *protos.SnapshotRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) State(ctx context.Context, request *protos.StateRequest) (*protos.StateResponse, error) {
	return nil, nil
}
func (ras *RaftAdminServer) Stats(ctx context.Context, request *protos.StatsRequest) (*protos.StatsResponse, error) {
	return nil, nil
}
func (ras *RaftAdminServer) VerifyLeader(ctx context.Context, request *protos.VerifyLeaderRequest) (*protos.Future, error) {
	return nil, nil
}
func (ras *RaftAdminServer) Await(ctx context.Context, request *protos.Future) (*protos.AwaitResponse, error) {
	return nil, nil
}
func (ras *RaftAdminServer) Forget(ctx context.Context, request *protos.Future) (*protos.ForgetResponse, error) {
	return nil, nil
}
