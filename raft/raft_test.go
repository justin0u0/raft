package raft

import (
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	time.Sleep(1 * time.Second)

	c.checkSingleLeader()
}
