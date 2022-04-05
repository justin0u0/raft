package raft

import (
	"context"
	"time"

	"github.com/justin0u0/raft/pb"
	"go.uber.org/zap"
)

type raft struct {
	pb.UnimplementedRaftServer

	*raftState

	id    uint32
	peers map[uint32]Peer

	config *Config
	logger *zap.Logger

	lastHeartbeat time.Time

	// rpcCh stores incoming RPCs
	rpcCh chan *rpc
	// applyCh stores logs that can be applied
	applyCh chan *pb.Entry
}

var _ pb.RaftServer = (*raft)(nil)

func NewRaft(id uint32, peers map[uint32]Peer, config *Config, logger *zap.Logger) *raft {
	raftState := &raftState{
		state:       Follower,
		currentTerm: 0,
		votedFor:    0,
		logs:        make([]*pb.Entry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[uint32]uint64),
		matchIndex:  make(map[uint32]uint64),
	}

	return &raft{
		raftState:     raftState,
		id:            id,
		peers:         peers,
		config:        config,
		logger:        logger.With(zap.Uint32("id", id)),
		lastHeartbeat: time.Now(),
		rpcCh:         make(chan *rpc),
		applyCh:       make(chan *pb.Entry),
	}
}

func (r *raft) applyCommand(req *pb.ApplyCommandRequest) (*pb.ApplyCommandResponse, error) {
	if r.state != Leader {
		return nil, errNotLeader
	}

	lastLogId, _ := r.getLastLog()
	e := &pb.Entry{Id: lastLogId + 1, Term: r.currentTerm, Data: req.GetData()}
	r.appendLogs([]*pb.Entry{e})

	return &pb.ApplyCommandResponse{Entry: e}, nil
}

func (r *raft) appendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if req.GetTerm() < r.currentTerm {
		r.logger.Info("reject append entries since current term is older")

		return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}, nil
	}

	r.lastHeartbeat = time.Now()

	// increase term if receive a newer one
	if req.GetTerm() > r.currentTerm {
		r.toFollower(req.GetTerm())
		r.logger.Info("increase term since receive a newer one", zap.Uint64("term", r.currentTerm))
	}

	if req.GetTerm() == r.currentTerm && r.state != Follower {
		r.toFollower(req.GetTerm())
		r.logger.Info("receive request from leader, fallback to follower", zap.Uint64("term", r.currentTerm))
	}

	// verify the last log entry
	prevLogId := req.GetPrevLogId()
	prevLogTerm := req.GetPrevLogTerm()
	if prevLogId != 0 && prevLogTerm != 0 {
		log := r.getLog(prevLogId)

		if prevLogTerm != log.GetTerm() {
			r.logger.Info("the given previous log from leader is missing or mismatched",
				zap.Uint64("leaderLogTerm", prevLogTerm),
				zap.Uint64("currentLogTerm", log.GetTerm()))

			return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}, nil
		}
	}

	if len(req.GetEntries()) != 0 {
		// delete entries after previous log
		r.deleteLogs(prevLogId)

		// append new entries
		r.appendLogs(req.GetEntries())
	}

	if req.GetLeaderCommitId() > r.commitIndex {
		lastLogId, _ := r.getLastLog()
		if req.GetLeaderCommitId() < lastLogId {
			r.setCommitIndex(req.GetLeaderCommitId())
		} else {
			r.setCommitIndex(lastLogId)
		}

		go r.applyLogs(r.applyCh)
	}

	return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
}

func (r *raft) requestVote(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// reject if current term is older
	if req.GetTerm() < r.currentTerm {
		r.logger.Info("reject request vote since current term is older")

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}

	// increase term if receive a newer one
	if req.GetTerm() > r.currentTerm {
		r.toFollower(req.GetTerm())
		r.logger.Info("increase term since receive a newer one", zap.Uint64("term", r.currentTerm))
	}

	// reject if already vote for another candidate
	if r.votedFor != 0 && r.votedFor != req.GetCandidateId() {
		r.logger.Info("reject since already vote for another candidate",
			zap.Uint64("term", r.currentTerm),
			zap.Uint32("votedFor", r.votedFor))

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}

	lastLogId, lastLogTerm := r.getLastLog()

	// reject if last log entry is more up-to-date
	if lastLogTerm > req.GetLastLogTerm() || (lastLogTerm == req.GetLastLogTerm() && lastLogId > req.GetLastLogId()) {
		r.logger.Info("reject since last entry is more up-to-date")

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}

	r.voteFor(req.GetCandidateId(), false)
	r.lastHeartbeat = time.Now()
	r.logger.Info("vote for another candidate", zap.Uint32("votedFor", r.votedFor))

	return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: true}, nil
}

func (r *raft) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		switch r.state {
		case Follower:
			r.runFollower(ctx)
		case Candidate:
			r.runCandidate(ctx)
		case Leader:
			r.runLeader(ctx)
		}
	}
}

// followers related

func (r *raft) runFollower(ctx context.Context) {
	r.logger.Info("running follower")

	timeoutCh := randomTimeout(r.config.HeartbeatTimeout)

	for r.state == Follower {
		select {
		case <-ctx.Done():
			return

		case <-timeoutCh:
			timeoutCh = randomTimeout(r.config.HeartbeatTimeout)

			if time.Now().Sub(r.lastHeartbeat) > r.config.HeartbeatTimeout {
				r.handleFollowerHeartbeatTimeout()
			}

		case rpc := <-r.rpcCh:
			r.handleRPCRequest(rpc)
		}
	}
}

func (r *raft) handleFollowerHeartbeatTimeout() {
	r.toCandidate()

	r.logger.Info("heartbeat timeout, change state from follower to candidate")
}

// candidates related

type voteResult struct {
	*pb.RequestVoteResponse
	peerId uint32
}

func (r *raft) runCandidate(ctx context.Context) {
	r.logger.Info("running candidate")

	grantedVotes := 0
	votesNeeded := (len(r.peers)+1)/2 + 1

	// vote for itself
	r.voteForSelf(&grantedVotes)

	// request votes from peers
	voteCh := make(chan *voteResult, len(r.peers))
	r.broadcastRequestVote(ctx, voteCh)

	// wait until:
	// 1. it wins the election
	// 2. another server establishes itself as leader (see AppendEntries)
	// 3. election timeout

	timeoutCh := randomTimeout(r.config.ElectionTimeout)

	for r.state == Candidate {
		select {
		case <-ctx.Done():
			return

		case vote := <-voteCh:
			r.handleVoteResult(vote, &grantedVotes, votesNeeded)

		case <-timeoutCh:
			r.logger.Info("election timeout reached, restarting election")
			return

		case rpc := <-r.rpcCh:
			r.handleRPCRequest(rpc)
		}
	}
}

func (r *raft) voteForSelf(grantedVotes *int) {
	r.voteFor(r.id, true)
	(*grantedVotes)++

	r.logger.Info("vote for self", zap.Uint64("term", r.currentTerm))
}

func (r *raft) broadcastRequestVote(ctx context.Context, voteCh chan *voteResult) {
	r.logger.Info("broadcast request vote", zap.Uint64("term", r.currentTerm))

	lastLogId, lastLogTerm := r.getLastLog()

	req := &pb.RequestVoteRequest{
		Term:        r.currentTerm,
		CandidateId: r.id,
		LastLogId:   lastLogId,
		LastLogTerm: lastLogTerm,
	}

	for peerId, peer := range r.peers {
		peerId := peerId
		peer := peer

		go func() {
			resp, err := peer.RequestVote(ctx, req)
			if err != nil {
				r.logger.Error("fail to send RequestVote RPC", zap.Error(err), zap.Uint32("peer", peerId))
				return
			}

			voteCh <- &voteResult{RequestVoteResponse: resp, peerId: peerId}
		}()
	}
}

func (r *raft) handleVoteResult(vote *voteResult, grantedVotes *int, votesNeeded int) {
	if vote.GetTerm() > r.currentTerm {
		r.toFollower(vote.GetTerm())
		r.logger.Info("receive new term on RequestVote response, fallback to follower", zap.Uint32("peer", vote.peerId))

		return
	}

	if vote.VoteGranted {
		(*grantedVotes)++
		r.logger.Info("vote granted", zap.Uint32("peer", vote.peerId), zap.Int("grantedVote", (*grantedVotes)))
	}

	if (*grantedVotes) >= votesNeeded {
		r.toLeader()
		r.logger.Info("election won", zap.Int("grantedVote", (*grantedVotes)), zap.Uint64("term", r.currentTerm))
	}
}

// leaders related

type appendEntriesResult struct {
	*pb.AppendEntriesResponse
	req    *pb.AppendEntriesRequest
	peerId uint32
}

func (r *raft) runLeader(ctx context.Context) {
	timeoutCh := randomTimeout(r.config.HeartbeatInterval)

	appendEntriesResultCh := make(chan *appendEntriesResult, len(r.peers))

	// reset `nextIndex` and `matchIndex`
	lastLogId, _ := r.getLastLog()
	for peerId := range r.peers {
		r.nextIndex[peerId] = lastLogId + 1
		r.matchIndex[peerId] = 0
	}

	for r.state == Leader {
		select {
		case <-ctx.Done():
			return

		case <-timeoutCh:
			timeoutCh = randomTimeout(r.config.HeartbeatInterval)

			r.broadcastAppendEntries(ctx, appendEntriesResultCh)

		case result := <-appendEntriesResultCh:
			r.handleAppendEntriesResult(result)

		case rpc := <-r.rpcCh:
			r.handleRPCRequest(rpc)
		}
	}
}

func (r *raft) broadcastAppendEntries(ctx context.Context, appendEntriesResultCh chan *appendEntriesResult) {
	r.logger.Info("broadcast append entries")

	for peerId, peer := range r.peers {
		peerId := peerId
		peer := peer

		prevLog := r.getLog(r.nextIndex[peerId] - 1)
		entries := r.getLogs(r.nextIndex[peerId])

		req := &pb.AppendEntriesRequest{
			Term:           r.currentTerm,
			LeaderId:       r.id,
			LeaderCommitId: r.commitIndex,
			PrevLogId:      prevLog.GetId(),
			PrevLogTerm:    prevLog.GetTerm(),
			Entries:        entries,
		}

		go func() {
			resp, err := peer.AppendEntries(ctx, req)
			if err != nil {
				r.logger.Error("fail to send AppendEntries RPC", zap.Error(err), zap.Uint32("peer", peerId))
			}

			appendEntriesResultCh <- &appendEntriesResult{
				AppendEntriesResponse: resp,
				req:                   req,
				peerId:                peerId,
			}
		}()
	}
}

func (r *raft) handleAppendEntriesResult(result *appendEntriesResult) {
	if result.GetTerm() > r.currentTerm {
		r.toFollower(result.GetTerm())
		r.logger.Info("receive new term on AppendEntries response, fallback to follower", zap.Uint32("peer", result.peerId))

		return
	}

	resp := result.AppendEntriesResponse
	req := result.req
	peerId := result.peerId
	entries := req.GetEntries()

	// request is not for heartbeat
	if len(entries) != 0 {
		if resp.Success {
			// if successful, update `matchIndex` and `nextIndex` for the follower

			matchIndex := entries[len(entries)-1].GetId()
			nextIndex := matchIndex + 1
			r.setNextAndMatchIndex(peerId, nextIndex, matchIndex)

			r.logger.Info("append entries successfully, set next index and match index",
				zap.Uint32("peer", peerId),
				zap.Uint64("nextIndex", nextIndex),
				zap.Uint64("matchIndex", matchIndex))
		} else {
			// if failed, decrease `nextIndex` and retry
			nextIndex := r.nextIndex[peerId] - 1
			matchIndex := r.matchIndex[peerId]

			r.setNextAndMatchIndex(peerId, nextIndex, matchIndex)

			r.logger.Info("append entries failed, decrease next index",
				zap.Uint32("peer", peerId),
				zap.Uint64("nextIndex", nextIndex),
				zap.Uint64("matchIndex", matchIndex))
		}
	}

	replicasNeeded := (len(r.peers)+1)/2 + 1

	logs := r.getLogs(r.commitIndex + 1)
	for i := len(logs) - 1; i >= 0; i-- {
		log := logs[i]
		if log.GetTerm() != r.currentTerm {
			continue
		}

		replicas := 1
		for peerId := range r.peers {
			if r.matchIndex[peerId] >= log.GetId() {
				replicas++
			}
		}

		if replicas >= replicasNeeded {
			r.setCommitIndex(log.GetId())

			r.logger.Info("found new logs committed, apply new logs",
				zap.Uint32("id", r.id),
				zap.Uint64("commitIndex", r.commitIndex))
			go r.applyLogs(r.applyCh)

			break
		}
	}
}
