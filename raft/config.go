package raft

import "time"

type Config struct {
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
}
