package raft

import (
	"github.com/justin0u0/raft/pb"
)

type peer struct {
	id uint32
	pb.RaftClient
}

func newPeer(id uint32, client pb.RaftClient) *peer {
	return &peer{
		id:         id,
		RaftClient: client,
	}
}
