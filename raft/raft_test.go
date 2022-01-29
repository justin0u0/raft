package raft

import (
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 5)

	time.Sleep(1 * time.Second)

	c.checkSingleLeader()
}
