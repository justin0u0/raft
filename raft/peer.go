package raft

import (
	"github.com/justin0u0/raft/pb"
	"google.golang.org/grpc"
)

// Peer provides an interface to allow Raft to commnuncate with other nodes
type Peer interface {
	pb.RaftClient
}

type peer struct {
	conn *grpc.ClientConn
	pb.RaftClient
}

var _ Peer = (*peer)(nil)

func (p *peer) dial(addr string, opts ...grpc.DialOption) error {
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return err
	}

	p.conn = conn
	p.RaftClient = pb.NewRaftClient(conn)

	return nil
}

func (p *peer) close() error {
	return p.conn.Close()
}
