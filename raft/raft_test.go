package raft

import (
	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	time.Sleep(1 * time.Second)

	c.checkSingleLeader()
}

func TestElectionAfterLeaderDisconnect(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	time.Sleep(1 * time.Second)

	oldId, oldTerm := c.checkSingleLeader()

	c.disconnectAll(oldId)

	time.Sleep(1 * time.Second)

	newId, newTerm := c.checkSingleLeader()

	if newId == oldId {
		t.Fatal("should elect a new leader")
	}

	if newTerm <= oldTerm {
		t.Fatal("new term should be greater than the old term")
	}
}

func TestElectionAfterLeaderDisconnectLoop(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	time.Sleep(1 * time.Second)

	oldId, oldTerm := c.checkSingleLeader()

	for i := 0; i < 3; i++ {
		c.disconnectAll(oldId)
		time.Sleep(1 * time.Second)
		c.connectAll(oldId)

		newId, newTerm := c.checkSingleLeader()

		if newId == oldId {
			t.Fatal("should elect a new leader")
		}

		if newTerm <= oldTerm {
			t.Fatal("new term should be greater than the old term")
		}

		oldId, oldTerm = newId, newTerm
	}
}

func TestFollowerDisconnect(t *testing.T) {
	numNodes := 5

	c := newCluster(t, numNodes)
	defer c.shutdown()

	time.Sleep(1 * time.Second)

	leaderId, leaderTerm := c.checkSingleLeader()

	peerId := randomPeerId(leaderId, numNodes)
	c.disconnectAll(peerId)

	c.disconnect(leaderId, peerId)

	time.Sleep(1 * time.Second)

	if nowId, nowTerm := c.checkSingleLeader(); nowId != leaderId || nowTerm != leaderTerm {
		t.Fatal("should remains the same leader and the same term")
	}
}

func TestSingleLogReplication(t *testing.T) {
	numNodes := 5

	c := newCluster(t, numNodes)
	defer c.shutdown()

	time.Sleep(1 * time.Second)
	leaderId, leaderTerm := c.checkSingleLeader()

	data := []byte("command 1")
	c.applyCommand(leaderId, leaderTerm, data)

	time.Sleep(500 * time.Millisecond)

	for id := 1; id <= numNodes; id++ {
		checkLog(t, c, uint32(id), 1, leaderTerm, nil)
	}
}

func TestManyLogsReplication(t *testing.T) {
	numNodes := 3

	c := newCluster(t, numNodes)
	defer c.shutdown()

	time.Sleep(1 * time.Second)
	leaderId, leaderTerm := c.checkSingleLeader()

	numLogs := 3000

	for i := 1; i <= numLogs; i++ {
		data := []byte("command " + strconv.Itoa(i))

		go func() {
			c.applyCommand(leaderId, leaderTerm, data)
		}()
	}

	time.Sleep(2 * time.Second)

	for id := 1; id <= numNodes; id++ {
		for i := 1; i <= numLogs; i++ {
			checkLog(t, c, uint32(id), uint64(i), leaderTerm, nil)
		}
	}
}

func TestLogReplicationWithNodeFailure(t *testing.T) {
	numNodes := 5

	c := newCluster(t, numNodes)
	defer c.shutdown()

	time.Sleep(1 * time.Second)
	leaderId, leaderTerm := c.checkSingleLeader()

	peerId := randomPeerId(leaderId, numNodes)
	c.disconnectAll(peerId)

	numLogs := 10

	for i := 1; i <= numLogs; i++ {
		data := []byte("command " + strconv.Itoa(i))

		go func() {
			c.applyCommand(leaderId, leaderTerm, data)
		}()
	}

	time.Sleep(1 * time.Second)

	for id := 1; id <= numNodes; id++ {
		id := uint32(id)

		if id == peerId {
			continue
		}

		for i := 1; i <= numLogs; i++ {
			checkLog(t, c, uint32(id), uint64(i), leaderTerm, nil)
		}
	}
}

func TestLogReplicationWithLeaderFailover(t *testing.T) {
	numNodes := 5

	c := newCluster(t, numNodes)
	defer c.shutdown()

	time.Sleep(1 * time.Second)
	oldLeaderId, oldLeaderTerm := c.checkSingleLeader()

	var wg sync.WaitGroup
	numLogs := 100
	for i := 1; i <= numLogs/2; i++ {
		wg.Add(1)

		data := []byte("command " + strconv.Itoa(i))

		go func() {
			c.applyCommand(oldLeaderId, oldLeaderTerm, data)
			wg.Done()
		}()
	}

	wg.Wait()
	c.disconnectAll(oldLeaderId)

	time.Sleep(1 * time.Second)
	newLeaderId, newLeaderTerm := c.checkSingleLeader()
	c.connectAll(oldLeaderId)

	for i := numLogs/2 + 1; i <= numLogs; i++ {
		data := []byte("command " + strconv.Itoa(i))

		go func() {
			c.applyCommand(newLeaderId, newLeaderTerm, data)
		}()
	}

	time.Sleep(1 * time.Second)
	for id := numLogs / 2; id <= numNodes; id++ {
		id := uint32(id)
		for i := 1; i <= numLogs; i++ {
			checkLog(t, c, uint32(id), uint64(i), newLeaderTerm, nil)
		}
	}
}

func randomPeerId(serverId uint32, numNodes int) uint32 {
	peerId := serverId

	for peerId == serverId {
		peerId = rand.Uint32()%uint32(numNodes) + 1
	}

	return peerId
}

func checkLog(t *testing.T, c *cluster, nodeId uint32, logId uint64, term uint64, data []byte) {
	l := c.consumers[nodeId].getLog(logId)

	if l == nil {
		t.Fatalf("log %d at node %d is not commited", logId, nodeId)
	}

	if l.GetTerm() != term {
		t.Fatalf("commited log %d at node %d has term mismatched the leader term", logId, nodeId)
	}
	if data != nil && bytes.Compare(l.GetData(), data) != 0 {
		t.Fatalf("commited log %d at node %d has data mismatched the given data", logId, nodeId)
	}
}
