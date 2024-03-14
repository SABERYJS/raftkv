package main

import (
	"errors"
	"flag"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"os"
	"path"
	"path/filepath"
	"raftapp/protos"
	"strconv"
)

var grpc_listen_port int
var bootstrap bool
var datadir string
var nodeid string

var logger *zap.Logger

func init() {
	flag.IntVar(&grpc_listen_port, "grpc_listen_port", 0, "set grpc_listen_port")
	flag.BoolVar(&bootstrap, "bootstrap", false, "raft bootstrap")
	flag.StringVar(&datadir, "datadir", "", "")
	flag.StringVar(&nodeid, "nodeid", "", "")
	logger, _ = zap.NewProduction()
}

func main() {
	flag.Parse()
	if grpc_listen_port == 0 || datadir == "" || nodeid == "" {
		panic("grpc_listen_port,datadir,nodeid required")
	}

	server_addr := "127.0.0.1:" + strconv.Itoa(grpc_listen_port)
	listen, err := net.Listen("tcp", server_addr)
	if err != nil {
		panic(err)
	}

	grpc_server := grpc.NewServer()

	kvstore := NewKvStore()

	raftHandler := NewRaftHandler()
	bridge := NewRaftBridge(server_addr)
	raftTransport := NewRaftTransportServer(raftHandler, bridge)

	raftnode, err := creatRaft(server_addr, kvstore, bridge)
	if err != nil {
		logger.Error("creat raft failed", zap.Error(err))
		return
	}

	raftAdminServer := NewRaftAdminServer(raftnode)

	protocolsService := NewProtocolsServices(kvstore, raftnode)
	protos.RegisterProtocolsServiceServer(grpc_server, protocolsService)
	protos.RegisterRaftTransportServer(grpc_server, raftTransport)
	protos.RegisterRaftAdminServer(grpc_server, raftAdminServer)

	err = grpc_server.Serve(listen)
	if err != nil {
		panic(err)
	}
}

func createIfNotExist(dir string) error {
	_, err := os.Stat(dir)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(dir, 0777)
		if err != nil {
			return err
		}
	}
	return nil
}

func creatRaft(saddr string, kvstore *kvStore, raftTransport *RaftBridge) (*raft.Raft, error) {
	defaultConfig := raft.DefaultConfig()
	defaultConfig.LocalID = raft.ServerID(nodeid)
	basedir := filepath.Join(datadir, nodeid)
	err := createIfNotExist(basedir)
	if err != nil {
		return nil, err
	}
	logstore, err := boltdb.NewBoltStore(path.Join(basedir, "logs.dat"))
	if err != nil {
		return nil, err
	}
	stablestore, err := boltdb.NewBoltStore(path.Join(basedir, "stable.dat"))
	if err != nil {
		return nil, err
	}
	snapshotstore, err := raft.NewFileSnapshotStore(basedir, 3, os.Stdout)
	if err != nil {
		return nil, err
	}
	raftnode, err := raft.NewRaft(defaultConfig, kvstore, logstore, stablestore, snapshotstore, raftTransport)
	if err != nil {
		return nil, err
	}
	exist, err := raft.HasExistingState(logstore, stablestore, snapshotstore)
	if err != nil {
		return nil, err
	}
	if !exist {
		if bootstrap {
			future := raftnode.BootstrapCluster(raft.Configuration{
				Servers: []raft.Server{
					{
						Suffrage: raft.Voter,
						ID:       raft.ServerID("r1"),
						Address:  raft.ServerAddress("127.0.0.1:8001"),
					},
					{
						Suffrage: raft.Voter,
						ID:       raft.ServerID("r2"),
						Address:  raft.ServerAddress("127.0.0.1:8002"),
					},
					{
						Suffrage: raft.Voter,
						ID:       raft.ServerID("r3"),
						Address:  raft.ServerAddress("127.0.0.1:8003"),
					},
				},
			})
			if future.Error() != nil {
				return nil, future.Error()
			}
		}
	}
	return raftnode, nil
}
