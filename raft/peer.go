package raft

import (
	"context"
	"sync"

	"github.com/justin0u0/raft/pb"
	"google.golang.org/grpc"
)

// Peer provides an interface to allow Raft to commnuncate with other nodes
type Peer interface {
	pb.RaftClient
}

// peer is the implementation of Peer for testing.
//
// Note that in normal implementation, you do not need to lock before request.
type peer struct {
	pb.RaftClient

	conn *grpc.ClientConn
	mu   sync.Mutex
}

var _ Peer = (*peer)(nil)

func (p *peer) ApplyCommand(ctx context.Context, in *pb.ApplyCommandRequest, opts ...grpc.CallOption) (*pb.ApplyCommandResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.RaftClient.ApplyCommand(ctx, in, opts...)
}

func (p *peer) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest, opts ...grpc.CallOption) (*pb.AppendEntriesResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.RaftClient.AppendEntries(ctx, in, opts...)
}

func (p *peer) RequestVote(ctx context.Context, in *pb.RequestVoteRequest, opts ...grpc.CallOption) (*pb.RequestVoteResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.RaftClient.RequestVote(ctx, in, opts...)
}

func (p *peer) dial(addr string, opts ...grpc.DialOption) error {
	p.mu.Lock()
	defer p.mu.Unlock()

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
