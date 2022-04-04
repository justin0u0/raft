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

	// rpcCh buffers incoming RPCs
	rpcCh chan *rpc
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
		nextIndex:   make(map[uint32]int64),
		matchIndex:  make(map[uint32]int64),
	}

	return &raft{
		raftState:     raftState,
		id:            id,
		peers:         peers,
		config:        config,
		logger:        logger.With(zap.Uint32("id", id)),
		lastHeartbeat: time.Now(),
		rpcCh:         make(chan *rpc),
	}
}

func (r *raft) appendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	if req.GetTerm() < r.currentTerm {
		return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}

	r.lastHeartbeat = time.Now()

	if req.GetTerm() >= r.currentTerm {
		if r.state == Candidate {
			r.logger.Info("receive request from leader, fallback to follower")
		}

		r.toFollower(req.GetTerm())
	}

	return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: true}
}

func (r *raft) requestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	// reject if current term is older
	if req.GetTerm() < r.currentTerm {
		r.logger.Info("reject since current term is older")

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
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

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}

	lastLogId, lastLogTerm := r.getLastLog()

	// reject if last log entry is more up-to-date
	if lastLogTerm > req.GetLastLogTerm() || (lastLogTerm == req.GetLastLogTerm() && lastLogId > req.GetLastLogId()) {
		r.logger.Info("reject since last entry is more up-to-date")

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}

	r.voteFor(req.GetCandidateId(), false)
	r.lastHeartbeat = time.Now()
	r.logger.Info("vote for another candidate", zap.Uint32("votedFor", r.votedFor))

	return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: true}
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
			r.logger.Info("election timeout eached, restarting election")
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

type heartbeatResult struct {
	*pb.AppendEntriesResponse
	peerId uint32
}

func (r *raft) runLeader(ctx context.Context) {
	timeoutCh := randomTimeout(r.config.HeartbeatInterval)

	heartbeatCh := make(chan *heartbeatResult, len(r.peers))

	for r.state == Leader {
		select {
		case <-ctx.Done():
			return

		case <-timeoutCh:
			timeoutCh = randomTimeout(r.config.HeartbeatInterval)

			r.broadcastHeartbeat(ctx, heartbeatCh)

		case heartbeat := <-heartbeatCh:
			r.handleHeartbeatResult(heartbeat)

		case rpc := <-r.rpcCh:
			r.handleRPCRequest(rpc)
		}
	}
}

func (r *raft) broadcastHeartbeat(ctx context.Context, heartbeatCh chan *heartbeatResult) {
	r.logger.Info("broadcast heartbeat")

	// since we send empty entries for heartbeat, so prevLogId, which is index of the
	// log entry immediately preceding the new entries, is equals to the last entry id.
	lastLogId, lastLogTerm := r.getLastLog()

	req := &pb.AppendEntriesRequest{
		Term:           r.currentTerm,
		LeaderId:       r.id,
		LeaderCommitId: r.commitIndex,
		PrevLogId:      lastLogId,
		PrevLogTerm:    lastLogTerm,
		Entries:        []*pb.Entry{},
	}

	for peerId, peer := range r.peers {
		peerId := peerId
		peer := peer

		go func() {
			resp, err := peer.AppendEntries(ctx, req)
			if err != nil {
				r.logger.Error("fail to send AppendEntries RPC", zap.Error(err), zap.Uint32("peer", peerId))
			}

			heartbeatCh <- &heartbeatResult{AppendEntriesResponse: resp, peerId: peerId}
		}()
	}
}

func (r *raft) handleHeartbeatResult(heartbeat *heartbeatResult) {
	if heartbeat.GetTerm() > r.currentTerm {
		r.toFollower(heartbeat.GetTerm())

		r.logger.Info("receive new term on AppendEntries response, fallback to follower", zap.Uint32("peer", heartbeat.peerId))
	}
}
