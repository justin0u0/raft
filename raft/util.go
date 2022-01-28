package raft

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	extra := time.Duration(rand.Int63n(int64(minVal)))

	return time.After(minVal + extra)
}
