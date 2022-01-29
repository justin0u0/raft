package raft

import (
	"math/rand"
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

func randomPeerId(serverId uint32, numNodes int) uint32 {
	peerId := serverId

	for peerId == serverId {
		peerId = rand.Uint32()%uint32(numNodes) + 1
	}

	return peerId
}
