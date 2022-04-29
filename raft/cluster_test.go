package raft

import (
	"bytes"
	"context"
	"log"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/justin0u0/raft/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// consumer consumes logs that are commited from the applyCh
type consumer struct {
	raft *Raft
	logs map[uint64]*pb.Entry
	mu   *sync.RWMutex
}

func newConsumer(raft *Raft) *consumer {
	return &consumer{
		raft: raft,
		logs: make(map[uint64]*pb.Entry),
		mu:   &sync.RWMutex{},
	}
}

func (c *consumer) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case e := <-c.raft.ApplyCh():
			c.mu.Lock()
			c.logs[e.Id] = e
			c.mu.Unlock()
		}
	}
}

func (c *consumer) getLog(id uint64) *pb.Entry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.logs[id]
}

// cluster is a raft cluster for testing
type cluster struct {
	t           *testing.T
	logger      *zap.Logger
	rafts       map[uint32]*Raft
	listerers   map[uint32]net.Listener
	servers     map[uint32]*grpc.Server
	cancelFuncs map[uint32]context.CancelFunc
	consumers   map[uint32]*consumer
	persisters  map[uint32]Persister
}

func newCluster(t *testing.T, numNodes int) *cluster {
	c := cluster{
		t:           t,
		rafts:       make(map[uint32]*Raft),
		listerers:   make(map[uint32]net.Listener),
		servers:     make(map[uint32]*grpc.Server),
		cancelFuncs: make(map[uint32]context.CancelFunc),
		consumers:   make(map[uint32]*consumer),
		persisters:  make(map[uint32]Persister),
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
		c.start(id, numNodes, config, logger)
	}

	for id := range c.rafts {
		c.connectAll(id)
	}

	for id, raft := range c.rafts {
		ctx, cancel := context.WithCancel(context.Background())

		go c.consumers[id].start(ctx)
		go raft.Run(ctx)

		c.cancelFuncs[id] = cancel
	}

	c.warnNumberOfCPUs()

	return &c
}

func (c *cluster) start(serverId uint32, numNodes int, config *Config, logger *zap.Logger) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		c.t.Fatal("fail to setup network", err)
	}

	c.listerers[serverId] = lis

	// initialized peers without connection
	peers := make(map[uint32]Peer)
	for j := 0; j < numNodes; j++ {
		peerId := uint32(j + 1)
		if serverId != peerId {
			peers[peerId] = &peer{}
		}
	}

	persister := c.persisters[serverId]
	if persister == nil {
		persister = newPersister()
	}

	raft := NewRaft(serverId, peers, persister, config, logger)
	c.rafts[serverId] = raft

	consumer := newConsumer(raft)
	c.consumers[serverId] = consumer

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, raft)
	c.servers[serverId] = grpcServer

	go func(lis net.Listener) {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal("fail to serve gRPC server:", err)
		}
	}(c.listerers[serverId])
}

// stopAll stops all raft servers
func (c *cluster) stopAll() {
	for id := range c.rafts {
		c.stop(id)
	}
}

// stop stops a raft server
func (c *cluster) stop(serverId uint32) {
	c.servers[serverId].GracefulStop()
	cancel := c.cancelFuncs[serverId]
	cancel()
}

// connect connects server to peer
func (c *cluster) connect(serverId, peerId uint32) {
	peers := c.rafts[serverId].peers
	peer := peers[peerId].(*peer)

	addr := c.listerers[peerId].Addr().String()

	c.logger.Debug("connect server with peer",
		zap.Uint32("server", serverId),
		zap.Uint32("peer", peerId),
		zap.String("addr", addr))

	if err := peer.dial(addr, grpc.WithInsecure()); err != nil {
		c.t.Fatal("fail to connect to peer:", err)
	}
}

// connect connects server to all peers
func (c *cluster) connectAll(serverId uint32) {
	peers := c.rafts[serverId].peers

	for peerId := range peers {
		c.connect(serverId, peerId)
	}
}

// disconnect disconnects connection from server to peer
func (c *cluster) disconnect(serverId, peerId uint32) {
	peers := c.rafts[serverId].peers
	peer := peers[peerId].(*peer)

	c.logger.Debug("disconnect server with peer",
		zap.Uint32("server", serverId),
		zap.Uint32("peer", peerId))

	if err := peer.close(); err != nil {
		c.t.Fatal("fail to disconnect to peer:", err)
	}
}

// disconnectAll disconnects all connections from server to all its peers
func (c *cluster) disconnectAll(serverId uint32) {
	peers := c.rafts[serverId].peers

	for peerId := range peers {
		c.disconnect(serverId, peerId)
	}
}

// checkSingleLeader checks if there is only one leader
// and returns the leader's ID and the leader's term
func (c *cluster) checkSingleLeader() (uint32, uint64) {
	var leaderId uint32
	var leaderTerm uint64

	for _, raft := range c.rafts {
		// lock raft state
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

func (c *cluster) applyCommand(id uint32, term uint64, data []byte) {
	ctx := context.Background()

	resp, err := c.rafts[id].ApplyCommand(ctx, &pb.ApplyCommandRequest{Data: data})
	if err != nil {
		c.t.Fatal("fail to apply command:", err)
	}

	if bytes.Compare(resp.GetEntry().GetData(), data) != 0 {
		c.t.Fatal("entry data mismatch given data")
	}

	if resp.GetEntry().GetTerm() != term {
		c.t.Fatal("entry term mismatch")
	}
}

func (c *cluster) warnNumberOfCPUs() {
	if runtime.NumCPU() < 2 {
		c.logger.Warn("number of CPUs < 2, may not test race condition of Raft algorithm")
	}
}
