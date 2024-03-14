package main

import (
	"context"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
	"raftapp/protos"
	"sync"
	"time"
)

type RaftBridge struct {
	saddr          raft.ServerAddress
	rpcch          chan raft.RPC
	dialOptions    []grpc.DialOption
	clients        map[raft.ServerID]*raftNode
	lock           sync.Mutex
	handleHeartRPC func(raft.RPC)
}

type raftNode struct {
	conn      *grpc.ClientConn
	rpcClient protos.RaftTransportClient
}

func NewRaftBridge(addr string) *RaftBridge {
	return &RaftBridge{saddr: raft.ServerAddress(addr), rpcch: make(chan raft.RPC), dialOptions: []grpc.DialOption{grpc.WithInsecure()}, clients: make(map[raft.ServerID]*raftNode)}
}

func (bridge *RaftBridge) findPeer(id raft.ServerID, target raft.ServerAddress) (*raftNode, error) {
	defer bridge.lock.Unlock()
	bridge.lock.Lock()
	if p, ok := bridge.clients[id]; ok {
		return p, nil
	}
	p := &raftNode{}
	conn, err := grpc.Dial(string(target), bridge.dialOptions...)
	if err != nil {
		return nil, err
	}
	p.conn = conn
	p.rpcClient = protos.NewRaftTransportClient(conn)
	bridge.clients[id] = p
	return p, nil
}

func (rb *RaftBridge) Consumer() <-chan raft.RPC {
	//TODO implement me
	return rb.rpcch
}

func (rb *RaftBridge) LocalAddr() raft.ServerAddress {
	//TODO implement me
	return rb.saddr
}

func (rb *RaftBridge) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	peer, err := rb.findPeer(id, target)
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)
	stream, err := peer.rpcClient.AppendEntriesPipeline(ctx)
	if err != nil {
		cancel()
		logger.Info("AppendEntriesPipeline_cancel")
		return nil, err
	}
	rpa := &raftPipelineAPI{
		stream:     stream,
		cancel:     cancel,
		inflightCh: make(chan *appendFuture, 20),
		doneCh:     make(chan raft.AppendFuture, 20),
	}
	go rpa.receiver()
	return rpa, nil
}

func (rb *RaftBridge) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	peer, err := rb.findPeer(id, target)
	if err != nil {
		return err
	}
	ctx := context.Background()
	ret, err := peer.rpcClient.AppendEntries(ctx, encodeAppendEntriesRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeAppendEntriesResponse(ret)
	return nil
}

func (rb *RaftBridge) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	peer, err := rb.findPeer(id, target)
	if err != nil {
		return err
	}
	ret, err := peer.rpcClient.RequestVote(context.Background(), encodeRequestVoteRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeRequestVoteResponse(ret)
	logger.Info("received_RequestVote_result", zap.Reflect("payload", resp))
	return nil
}

func (rb *RaftBridge) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	peer, err := rb.findPeer(id, target)
	if err != nil {
		return err
	}
	stream, err := peer.rpcClient.InstallSnapshot(context.TODO())
	if err != nil {
		return err
	}
	if err := stream.Send(encodeInstallSnapshotRequest(args)); err != nil {
		return err
	}
	var buf [16384]byte
	for {
		n, err := data.Read(buf[:])
		if err == io.EOF || (err == nil && n == 0) {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&protos.InstallSnapshotRequest{
			Data: buf[:n],
		}); err != nil {
			return err
		}
	}
	ret, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	*resp = *decodeInstallSnapshotResponse(ret)
	return nil
}

func (rb *RaftBridge) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (rb *RaftBridge) DecodePeer(bytes []byte) raft.ServerAddress {
	return raft.ServerAddress(bytes)
}

func (rb *RaftBridge) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	rb.handleHeartRPC = cb
}

func (rb *RaftBridge) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	peer, err := rb.findPeer(id, target)
	if err != nil {
		return err
	}
	ret, err := peer.rpcClient.TimeoutNow(context.TODO(), encodeTimeoutNowRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeTimeoutNowResponse(ret)
	return nil
}

func (rb *RaftBridge) handleGrpcRequest(command interface{}, data io.Reader) (interface{}, error) {
	/*bytes, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}*/
	//logger.Info("handlerpc", zap.String("cmd", string(bytes)))
	ch := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  command,
		RespChan: ch,
		Reader:   data,
	}
	if isHeartbeat(command) && rb.handleHeartRPC != nil {
		rb.handleHeartRPC(rpc)
		//logger.Info("handle_heartbeat")
	} else {
		//写入请求给raft
		rb.rpcch <- rpc
	}
	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Response, nil
}

type raftPipelineAPI struct {
	stream        protos.RaftTransport_AppendEntriesPipelineClient
	cancel        func()
	inflightChMtx sync.Mutex
	inflightCh    chan *appendFuture
	doneCh        chan raft.AppendFuture
}

// AppendEntries is used to add another request to the pipeline.
// The send may block which is an effective form of back-pressure.
func (r *raftPipelineAPI) AppendEntries(req *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	//logger.Info("pipeline_append_entries", zap.Reflect("payload", req))
	af := &appendFuture{
		start:   time.Now(),
		request: req,
		done:    make(chan struct{}),
	}
	if err := r.stream.Send(encodeAppendEntriesRequest(req)); err != nil {
		logger.Error("stream_send_failed", zap.Error(err))
		return nil, err
	}
	r.inflightChMtx.Lock()
	select {
	case <-r.stream.Context().Done():
	default:
		r.inflightCh <- af
	}
	r.inflightChMtx.Unlock()
	return af, nil
}

// Consumer returns a channel that can be used to consume
// response futures when they are ready.
func (r *raftPipelineAPI) Consumer() <-chan raft.AppendFuture {
	return r.doneCh
}

// Close closes the pipeline and cancels all inflight RPCs
func (r *raftPipelineAPI) Close() error {
	r.cancel()
	r.inflightChMtx.Lock()
	close(r.inflightCh)
	r.inflightChMtx.Unlock()
	return nil
}

func (r *raftPipelineAPI) receiver() {
	for af := range r.inflightCh {
		msg, err := r.stream.Recv()
		if err != nil {
			af.err = err
			logger.Error("receiver_failer", zap.Error(err))
		} else {
			af.response = *decodeAppendEntriesResponse(msg)
			//logger.Info("receiver_ok", zap.Reflect("resp", af.response))
		}
		close(af.done)
		r.doneCh <- af
	}
}

type appendFuture struct {
	raft.AppendFuture

	start    time.Time
	request  *raft.AppendEntriesRequest
	response raft.AppendEntriesResponse
	err      error
	done     chan struct{}
}

// Error blocks until the future arrives and then
// returns the error status of the future.
// This may be called any number of times - all
// calls will return the same value.
// Note that it is not OK to call this method
// twice concurrently on the same Future instance.
func (f *appendFuture) Error() error {
	<-f.done
	return f.err
}

// Start returns the time that the append request was started.
// It is always OK to call this method.
func (f *appendFuture) Start() time.Time {
	return f.start
}

// Request holds the parameters of the AppendEntries call.
// It is always OK to call this method.
func (f *appendFuture) Request() *raft.AppendEntriesRequest {
	return f.request
}

// Response holds the results of the AppendEntries call.
// This method must only be called after the Error
// method returns, and will only be valid on success.
func (f *appendFuture) Response() *raft.AppendEntriesResponse {
	return &f.response
}
