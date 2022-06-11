package raft

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stopAll()

	time.Sleep(1 * time.Second)

	c.checkSingleLeader()
}

func TestElectionAfterLeaderDisconnect(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stopAll()

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
	defer c.stopAll()

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
	defer c.stopAll()

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
	defer c.stopAll()

	time.Sleep(1 * time.Second)
	leaderId, leaderTerm := c.checkSingleLeader()

	data := []byte("command 1")
	c.applyCommand(leaderId, leaderTerm, data)

	time.Sleep(500 * time.Millisecond)

	for id := 1; id <= numNodes; id++ {
		id := uint32(id)
		c.checkLog(id, 1, leaderTerm, nil)
	}
}

func TestManyLogsReplication(t *testing.T) {
	numNodes := 3

	c := newCluster(t, numNodes)
	defer c.stopAll()

	time.Sleep(1 * time.Second)
	leaderId, leaderTerm := c.checkSingleLeader()

	numLogs := 2000

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 1; i <= numLogs; i++ {
			data := []byte("command " + strconv.Itoa(i))
			c.applyCommand(leaderId, leaderTerm, data)
		}
		wg.Done()
	}()

	wg.Wait()
	time.Sleep(2 * time.Second)

	for id := 1; id <= numNodes; id++ {
		id := uint32(id)
		for i := 1; i <= numLogs; i++ {
			data := []byte("command " + strconv.Itoa(i))
			c.checkLog(id, uint64(i), leaderTerm, data)
		}
	}
}

func TestLogReplicationWithFollowerFailure(t *testing.T) {
	numNodes := 5

	c := newCluster(t, numNodes)
	defer c.stopAll()

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
			c.checkLog(id, uint64(i), leaderTerm, nil)
		}
	}

	// follower comes back from network partition, should catch up all missing logs
	c.connectAll(peerId)

	time.Sleep(1 * time.Second)
	for i := 1; i <= numLogs; i++ {
		c.checkLog(peerId, uint64(i), leaderTerm, nil)
	}
}

func TestLogReplicationWithLeaderFailover(t *testing.T) {
	numNodes := 5

	c := newCluster(t, numNodes)
	defer c.stopAll()

	time.Sleep(1 * time.Second)
	oldLeaderId, oldLeaderTerm := c.checkSingleLeader()

	numLogs := 2000

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for i := 1; i <= numLogs/2; i++ {
			data := []byte("command " + strconv.Itoa(i))
			c.applyCommand(oldLeaderId, oldLeaderTerm, data)
		}

		wg.Done()
	}()

	wg.Wait()
	c.disconnectAll(oldLeaderId)

	time.Sleep(1 * time.Second)
	newLeaderId, newLeaderTerm := c.checkSingleLeader()
	c.connectAll(oldLeaderId)

	mustLogIds := make(chan uint64, numLogs)
	wg.Add(1)
	go func() {
		for i := numLogs/2 + 1; i <= numLogs; i++ {
			data := []byte("command " + strconv.Itoa(i))
			mustLogIds <- c.applyCommand(newLeaderId, newLeaderTerm, data)
		}

		wg.Done()
	}()

	wg.Wait()
	close(mustLogIds)

	logIds := make([]uint64, 0, numLogs)
	for logId := range mustLogIds {
		logIds = append(logIds, logId)
	}

	time.Sleep(1 * time.Second)

	for i := 1; i <= numNodes; i++ {
		id := uint32(i)
		for _, logId := range logIds {
			c.checkLog(id, logId, newLeaderTerm, nil)
		}
	}
}

func TestOnlyUpToDateCandidateWinLeaderElection(t *testing.T) {
	numNodes := 5

	c := newCluster(t, numNodes)
	defer c.stopAll()

	time.Sleep(1 * time.Second)
	oldLeaderId, oldLeaderTerm := c.checkSingleLeader()

	// log 1 are replicated on all followers
	data1 := []byte("command 1")
	c.applyCommand(oldLeaderId, oldLeaderTerm, data1)
	time.Sleep(500 * time.Millisecond)

	peerId1 := randomPeerId(oldLeaderId, numNodes)
	peerId2 := peerId1
	for peerId2 == peerId1 {
		peerId2 = randomPeerId(oldLeaderId, numNodes)
	}

	// stop 2 followers
	c.stop(peerId1)
	c.stop(peerId2)

	// log 2 are not replicated on peerId1 and peerId2
	data2 := []byte("command 2")
	c.applyCommand(oldLeaderId, oldLeaderTerm, data2)

	time.Sleep(1 * time.Second)
	// leader failover
	c.stop(oldLeaderId)

	// log 1 are replicated on all followers,
	// log 2 are replicated on all followers except peerId1 and peerId2
	for i := 1; i <= numNodes; i++ {
		id := uint32(i)

		c.checkLog(id, 1, oldLeaderTerm, data1)

		if id == peerId1 || id == peerId2 {
			if l := c.consumers[id].getLog(2); l != nil {
				t.Fatalf("node %d is already stopped, should not receive the log", id)
			}
		} else {
			c.checkLog(id, 2, oldLeaderTerm, data2)
		}
	}

	// restart the 2 followers
	c.initialize(peerId1)
	c.initialize(peerId2)
	for i := 1; i <= numNodes; i++ {
		id := uint32(i)
		if id != oldLeaderId {
			c.connectAll(id)
		}
	}
	c.start(peerId1)
	c.start(peerId2)

	// new leader should not be the 2 followers that are stopped before
	time.Sleep(1 * time.Second)
	newLeaderId, newLeaderTerm := c.getCurrentLeader()

	if newLeaderId == oldLeaderId {
		t.Fatalf("invalid leader, node %d already stop", oldLeaderId)
	}
	if newLeaderId == peerId1 || newLeaderId == peerId2 {
		t.Fatalf("invalid leader, node %d does not contain up-to-date logs", newLeaderId)
	}
	if newLeaderTerm <= oldLeaderTerm {
		t.Fatalf("new leader %d should have term %d greater than the old term %d", newLeaderId, newLeaderTerm, oldLeaderTerm)
	}

	// log 2 are finally replicated on the 2 followers that are stopped before
	for i := 1; i <= numNodes; i++ {
		id := uint32(i)
		if id != oldLeaderId {
			c.checkLog(id, 1, oldLeaderTerm, data1)
			c.checkLog(id, 2, oldLeaderTerm, data2)
		}
	}

	// restart old leader
	c.initialize(oldLeaderId)
	for i := 1; i <= numNodes; i++ {
		id := uint32(i)
		c.connectAll(id)
	}
	c.start(oldLeaderId)

	time.Sleep(500 * time.Millisecond)

	leaderId, leaderTerm := c.checkSingleLeader()
	if leaderId != newLeaderId || leaderTerm != newLeaderTerm {
		t.Fatalf("leader come back should not affect the current leader")
	}
	c.checkLog(oldLeaderId, 1, oldLeaderTerm, data1)
	c.checkLog(oldLeaderId, 2, oldLeaderTerm, data2)
}

func TestCannotCommitLogIfTermMismatch(t *testing.T) {
	// note that the test is explained in the Raft paper figure 8
	numNodes := 5

	c := newCluster(t, numNodes)
	defer c.stopAll()

	time.Sleep(1 * time.Second)
	oldLeaderId, oldLeaderTerm := c.checkSingleLeader()

	// log 1 are replicated on all followers
	data1 := []byte("command 1")
	c.applyCommand(oldLeaderId, oldLeaderTerm, data1)
	time.Sleep(500 * time.Millisecond)

	// log 2 is replicated on no follower
	c.disconnectAll(oldLeaderId)
	data2 := []byte("command 2")
	log2Id := c.applyCommand(oldLeaderId, oldLeaderTerm, data2)

	time.Sleep(1 * time.Second)
	newLeaderId, newLeaderTerm := c.checkSingleLeader()
	if newLeaderId == oldLeaderId {
		t.Fatalf("invalid leader, node %d already stop", oldLeaderId)
	}
	if newLeaderTerm <= oldLeaderTerm {
		t.Fatalf("new leader %d should have term %d greater than the old term %d", newLeaderId, newLeaderTerm, oldLeaderTerm)
	}

	// old leader is back from the network partition
	c.connectAll(oldLeaderId)
	time.Sleep(1 * time.Second)
	leaderId, leaderTerm := c.checkSingleLeader()
	if newLeaderId != leaderId || newLeaderTerm != leaderTerm {
		t.Fatal("new leader should not be affected when the old leader come back")
	}

	// we disconnect the current leader from the old leader, thus forces the old leader
	// to timeout and increase term and become the next leader
	c.disconnect(leaderId, oldLeaderId)
	time.Sleep(1 * time.Second)
	c.connect(leaderId, oldLeaderId)

	leaderId, leaderTerm = c.checkSingleLeader()
	if leaderId != oldLeaderId {
		t.Fatal("leader should go back to the old leader after our manually operation")
	}
	if leaderTerm <= newLeaderTerm {
		t.Fatalf("the old leader should have term %d greater than the old term", newLeaderTerm)
	}

	// log 2 should be replicated to all servers now but cannot commit since the log does not match current term
	for id, raft := range c.rafts {
		raft.mu.Lock()
		if raft.getLog(log2Id) == nil {
			t.Fatalf("log 2 does not replicated on server %d", raft.id)
		}
		raft.mu.Unlock()

		if c.consumers[id].getLog(log2Id) != nil {
			t.Fatalf("log 2 is committed on server %d", raft.id)
		}
	}

	// log 3 is replicated on all servers
	// then all logs are committed
	data3 := []byte("command 3")
	c.applyCommand(leaderId, leaderTerm, data3)

	time.Sleep(500 * time.Millisecond)
	for i := 1; i <= numNodes; i++ {
		id := uint32(i)
		c.checkLog(id, 1, oldLeaderTerm, data1)
		c.checkLog(id, 2, oldLeaderTerm, data2)
		c.checkLog(id, 3, leaderTerm, data3)
	}
}

func randomPeerId(serverId uint32, numNodes int) uint32 {
	peerId := serverId

	for peerId == serverId {
		peerId = rand.Uint32()%uint32(numNodes) + 1
	}

	return peerId
}
