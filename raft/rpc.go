package raft

import (
	"context"
	"errors"
	"fmt"

	"github.com/justin0u0/raft/pb"
)

type rpcResponse struct {
	resp interface{}
	err  error
}

type rpc struct {
	req    interface{}
	respCh chan<- *rpcResponse
}

func (r *rpc) respond(resp interface{}, err error) {
	r.respCh <- &rpcResponse{resp: resp, err: err}
}

var (
	errRPCTimeout           = errors.New("rpc timeout")
	errResponseTypeMismatch = errors.New("response type mismatch")
	errInvalidRPCType       = errors.New("invalid rpc type")
	errNotLeader            = errors.New("not leader")
)

func (r *Raft) ApplyCommand(ctx context.Context, req *pb.ApplyCommandRequest) (*pb.ApplyCommandResponse, error) {
	rpcResp, err := r.dispatchRPCRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	resp, ok := rpcResp.(*pb.ApplyCommandResponse)
	if !ok {
		return nil, errResponseTypeMismatch
	}

	if err := r.saveRaftState(r.persister); err != nil {
		return nil, fmt.Errorf("fail to save raft state: %w", err)
	}

	return resp, nil
}

func (r *Raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	rpcResp, err := r.dispatchRPCRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	resp, ok := rpcResp.(*pb.AppendEntriesResponse)
	if !ok {
		return nil, errResponseTypeMismatch
	}

	if err := r.saveRaftState(r.persister); err != nil {
		return nil, fmt.Errorf("fail to save raft state: %w", err)
	}

	return resp, nil
}

func (r *Raft) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	rpcResp, err := r.dispatchRPCRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	resp, ok := rpcResp.(*pb.RequestVoteResponse)
	if !ok {
		return nil, errResponseTypeMismatch
	}

	if err := r.saveRaftState(r.persister); err != nil {
		return nil, fmt.Errorf("fail to save raft state: %w", err)
	}

	return resp, nil
}

func (r *Raft) dispatchRPCRequest(ctx context.Context, req interface{}) (interface{}, error) {
	respCh := make(chan *rpcResponse, 1)
	r.rpcCh <- &rpc{req: req, respCh: respCh}

	select {
	case <-ctx.Done():
		return nil, errRPCTimeout
	case rpcResp := <-respCh:
		if err := rpcResp.err; err != nil {
			return nil, err
		}

		return rpcResp.resp, nil
	}
}

func (r *Raft) handleRPCRequest(rpc *rpc) {
	switch req := rpc.req.(type) {
	case *pb.ApplyCommandRequest:
		rpc.respond(r.applyCommand(req))
	case *pb.AppendEntriesRequest:
		rpc.respond(r.appendEntries(req))
	case *pb.RequestVoteRequest:
		rpc.respond(r.requestVote(req))
	default:
		rpc.respond(nil, errInvalidRPCType)
	}
}
