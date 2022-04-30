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
	numNodes    int
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
		numNodes:    numNodes,
		rafts:       make(map[uint32]*Raft),
		listerers:   make(map[uint32]net.Listener),
		servers:     make(map[uint32]*grpc.Server),
		cancelFuncs: make(map[uint32]context.CancelFunc),
		consumers:   make(map[uint32]*consumer),
		persisters:  make(map[uint32]Persister),
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal("fail to create logger:", err)
	}

	c.logger = logger

	for i := 1; i <= numNodes; i++ {
		id := uint32(i)
		c.initialize(id)
	}

	for id := range c.rafts {
		c.connectAll(id)
	}

	for id := range c.rafts {
		c.start(id)
	}

	c.warnNumberOfCPUs()

	return &c
}

// initialize initializes raft and the raft RPC server
func (c *cluster) initialize(serverId uint32) {
	c.logger.Debug("initializing raft", zap.Uint32("id", serverId))

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		c.t.Fatal("fail to setup network", err)
	}

	c.listerers[serverId] = lis
	c.logger.Debug("setup listner",
		zap.Uint32("id", serverId),
		zap.String("addr", c.listerers[serverId].Addr().String()))

	// initialized peers without connection
	peers := make(map[uint32]Peer)
	for i := 1; i <= c.numNodes; i++ {
		peerId := uint32(i)
		if serverId != peerId {
			peers[peerId] = &peer{}
		}
	}

	persister := c.persisters[serverId]
	if persister == nil {
		persister = newPersister()
		c.persisters[serverId] = persister
	}

	config := &Config{
		HeartbeatTimeout:  150 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	}

	raft := NewRaft(serverId, peers, persister, config, c.logger)
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

// start starts raft main loop and consumer loop without connection
//
// Note that this function must be called after all peer connections are established
func (c *cluster) start(serverId uint32) {
	ctx, cancel := context.WithCancel(context.Background())

	raft := c.rafts[serverId]
	consumer := c.consumers[serverId]
	consumer.raft = raft

	go consumer.start(ctx)
	go raft.Run(ctx)

	c.cancelFuncs[serverId] = cancel
}

// stopAll stops all raft servers
func (c *cluster) stopAll() {
	for id := range c.rafts {
		c.stop(id)
	}
}

// stop stops a raft server
func (c *cluster) stop(serverId uint32) {
	if c.servers[serverId] == nil {
		return
	}

	c.servers[serverId].GracefulStop()
	cancel := c.cancelFuncs[serverId]
	cancel()

	c.cancelFuncs[serverId] = nil
	c.rafts[serverId] = nil
	delete(c.rafts, serverId)
	c.servers[serverId] = nil
}

// connect connects server to all peers
func (c *cluster) connectAll(serverId uint32) {
	peers := c.rafts[serverId].peers

	for peerId := range peers {
		c.connect(serverId, peerId)
	}
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
				leaderId, leaderTerm = raft.id, raft.currentTerm
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

// getCurrentLeader returns the leader with the greatest term
func (c *cluster) getCurrentLeader() (uint32, uint64) {
	var leaderId uint32
	var leaderTerm uint64

	for _, raft := range c.rafts {
		// lock raft state
		raft.mu.Lock()
		defer raft.mu.Unlock()

		if raft.state == Leader {
			if leaderId == 0 {
				leaderId, leaderTerm = raft.id, raft.currentTerm
			} else if leaderTerm == raft.currentTerm {
				c.t.Fatalf("both node %d and node %d are leader with the same term %d", leaderId, raft.id, leaderTerm)
			} else if leaderTerm < raft.currentTerm {
				leaderId, leaderTerm = raft.id, raft.currentTerm
			}
		}
	}

	if leaderId == 0 {
		c.t.Fatal("no leader found")
	}

	return leaderId, leaderTerm
}

func (c *cluster) applyCommand(id uint32, term uint64, data []byte) uint64 {
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

	return resp.Entry.GetId()
}

func (c *cluster) checkLog(serverId uint32, logId uint64, term uint64, data []byte) {
	l := c.consumers[serverId].getLog(logId)

	if l == nil {
		c.t.Fatalf("log %d at server %d is not commited", logId, serverId)
	}

	if l.GetTerm() != term {
		c.t.Fatalf("commited log %d at server %d has term mismatched the leader term", logId, serverId)
	}
	if data != nil && bytes.Compare(l.GetData(), data) != 0 {
		c.t.Fatalf("commited log %d at server %d has data mismatched the given data", logId, serverId)
	}
}

func (c *cluster) warnNumberOfCPUs() {
	if runtime.NumCPU() < 2 {
		c.logger.Warn("number of CPUs < 2, may not test race condition of Raft algorithm")
	}
}
