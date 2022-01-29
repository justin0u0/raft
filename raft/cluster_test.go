package raft

import (
	"context"
	"log"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/justin0u0/raft/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// cluster is a raft cluster for testing
type cluster struct {
	t           *testing.T
	logger      *zap.Logger
	rafts       map[uint32]*raft
	listerers   map[uint32]net.Listener
	servers     map[uint32]*grpc.Server
	cancelFuncs map[uint32]context.CancelFunc
}

func newCluster(t *testing.T, numNodes int) *cluster {
	c := cluster{
		t:           t,
		rafts:       make(map[uint32]*raft),
		listerers:   make(map[uint32]net.Listener),
		servers:     make(map[uint32]*grpc.Server),
		cancelFuncs: make(map[uint32]context.CancelFunc),
	}

	config := &Config{
		HeartbeatTimeout:  150 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal("fail to create logger:", err)
	}

	c.logger = logger

	for i := 0; i < numNodes; i++ {
		id := uint32(i + 1)

		lis, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Fatal("fail to setup network", err)
		}

		c.listerers[id] = lis

		// initialized peers without connection
		peers := make(map[uint32]Peer)
		for j := 0; j < numNodes; j++ {
			peerId := uint32(j + 1)
			if id != peerId {
				peers[peerId] = &peer{}
			}
		}

		raft := NewRaft(id, peers, config, logger)
		c.rafts[id] = raft

		grpcServer := grpc.NewServer()
		pb.RegisterRaftServer(grpcServer, raft)
		c.servers[id] = grpcServer

		go func(lis net.Listener) {
			if err := grpcServer.Serve(lis); err != nil {
				log.Fatal("fail to serve gRPC server:", err)
			}
		}(c.listerers[id])
	}

	for id, raft := range c.rafts {
		for peerId := range raft.peers {
			c.connect(id, peerId)
		}
	}

	for id, raft := range c.rafts {
		ctx, cancel := context.WithCancel(context.Background())

		go raft.Run(ctx)

		c.cancelFuncs[id] = cancel
	}

	c.warnNumberOfCPUs()

	return &c
}

func (c *cluster) shutdown() {
	for id := range c.rafts {
		c.servers[id].GracefulStop()

		cancel := c.cancelFuncs[id]
		cancel()
	}
}

func (c *cluster) connect(serverId, clientId uint32) {
	peers := c.rafts[serverId].peers
	peer := peers[clientId].(*peer)

	addr := c.listerers[clientId].Addr().String()

	c.logger.Debug("connect server with peer",
		zap.Uint32("server", serverId),
		zap.Uint32("client", clientId),
		zap.String("addr", addr))

	if err := peer.dial(addr, grpc.WithInsecure()); err != nil {
		c.t.Fatal("fail to connect to peer:", err)
	}
}

// disconnect disconnect connection from server to peer
func (c *cluster) disconnect(serverId, peerId uint32) {
	peers := c.rafts[serverId].peers
	peer := peers[peerId].(*peer)

	if err := peer.close(); err != nil {
		c.t.Fatal("fail to disconnect to peer:", err)
	}
}

// disconnect disconnect all connections from server to all its peers
func (c *cluster) disconnectAll(serverId uint32) {
	peers := c.rafts[serverId].peers

	for peerId := range peers {
		peer := peers[peerId].(*peer)

		if err := peer.close(); err != nil {
			c.t.Fatal("fail to disconnect to peer:", err)
		}
	}
}

// checkSingleLeader check if there is only one leader
// and returns the leader's ID and the leader's term
func (c *cluster) checkSingleLeader() (uint32, uint64) {
	var leaderId uint32
	var leaderTerm uint64

	for _, raft := range c.rafts {
		raft.mu.Lock()
		defer raft.mu.Unlock()

		if raft.state == Leader {
			if leaderId == 0 {
				leaderId = raft.id
				leaderTerm = raft.currentTerm
			} else {
				c.t.Fatalf("both %d and %d thinks they are leader", leaderId, raft.id)
			}
		}
	}

	if leaderId == 0 {
		c.t.Fatal("no leader found")
	}

	return leaderId, leaderTerm
}

func (c *cluster) warnNumberOfCPUs() {
	if runtime.NumCPU() < 2 {
		c.logger.Warn("number of CPUs < 2, may not test race condition of Raft algorithm")
	}
}
