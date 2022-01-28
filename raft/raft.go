package raft

import (
	"context"
	"sync"
	"time"

	"github.com/justin0u0/raft/pb"
	"go.uber.org/zap"
)

type raft struct {
	pb.UnimplementedRaftServer

	raftState

	id    uint32
	peers []*peer

	mu     sync.Mutex
	config *Config
	logger *zap.Logger

	lastHeartbeat time.Time
}

func NewRaft(id uint32, peers []*peer, config *Config, logger *zap.Logger) pb.RaftServer {
	return &raft{
		id:     id,
		peers:  peers,
		config: config,
		logger: logger.With(zap.Uint32("id", id)),
	}
}

func (r *raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	// AppendEntries may changed raft state
	// it is nesessary to protect code section where Raft state changed
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.GetTerm() < r.currentTerm {
		return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}, nil
	}

	r.lastHeartbeat = time.Now()

	if req.GetTerm() >= r.currentTerm {
		if r.state == Candidate {
			r.logger.Info("receive request from leader, fallback to follower")
		}

		r.toFollower(req.GetTerm())
	}

	return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
}

func (r *raft) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// RequestVote may changed raft state
	// it is nesessary to protect code section where Raft state changed
	r.mu.Lock()
	defer r.mu.Unlock()

	// reject if current term is older
	if req.GetTerm() < r.currentTerm {
		r.logger.Info("reject since current term is older")

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}

	// increase term if receive a newer one
	if req.GetTerm() > r.currentTerm {
		r.toFollower(req.GetTerm())

		r.logger.Info("increase term since receive a newer one")
	}

	// reject if already vote for another candidate
	if r.votedFor != 0 && r.votedFor != req.GetCandidateId() {
		r.logger.Info("reject since already vote for another candidate")

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}

	lastLogId, lastLogTerm := r.getLastLog()

	// reject if last log entry is more up-to-date
	if lastLogTerm > req.GetLastLogTerm() || (lastLogTerm == req.GetLastLogTerm() && lastLogId > req.GetLastLogId()) {
		r.logger.Info("reject since last entry is more up-to-date")

		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}

	r.votedFor = req.GetCandidateId()
	r.lastHeartbeat = time.Now()
	r.logger.Info("vote for another candidate", zap.Uint32("votedFor", r.votedFor))

	return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: true}, nil
}

func (r *raft) run(ctx context.Context) {
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

			if time.Now().Sub(r.lastHeartbeat) < r.config.HeartbeatTimeout {
				r.handleFollowerHeartbeatTimeout()
			}
		}
	}
}

func (r *raft) handleFollowerHeartbeatTimeout() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = Candidate

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
	r.currentTerm++
	r.votedFor = r.id
	grantedVotes++

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
		}
	}
}

func (r *raft) broadcastRequestVote(ctx context.Context, voteCh chan *voteResult) {
	r.logger.Info("broadcast request vote")

	lastLogId, lastLogTerm := r.getLastLog()

	req := &pb.RequestVoteRequest{
		Term:        r.currentTerm,
		CandidateId: r.id,
		LastLogId:   lastLogId,
		LastLogTerm: lastLogTerm,
	}

	for _, peer := range r.peers {
		peer := peer

		go func() {
			resp, err := peer.RequestVote(ctx, req)
			if err != nil {
				r.logger.Error("fail to send RequestVote RPC", zap.Error(err), zap.Uint32("peer", peer.id))
				return
			}

			voteCh <- &voteResult{RequestVoteResponse: resp, peerId: peer.id}
		}()
	}
}

func (r *raft) handleVoteResult(vote *voteResult, grantedVotes *int, votesNeeded int) {
	// since all RPC response runs in another goroutine
	// it is necessary to protect code section where raft state changed
	r.mu.Lock()
	defer r.mu.Unlock()

	if vote.GetTerm() > r.currentTerm {
		r.state = Follower
		r.currentTerm = vote.GetTerm()

		r.logger.Info("receive new term on RequestVote response, fallback to follower", zap.Uint32("peer", vote.peerId))
		return
	}

	if vote.VoteGranted {
		(*grantedVotes)++

		r.logger.Info("vote granted", zap.Uint32("peer", vote.peerId), zap.Int("grantedVote", (*grantedVotes)))
	}

	if (*grantedVotes) >= votesNeeded {
		r.state = Leader

		r.logger.Info("election won", zap.Int("grantedVote", (*grantedVotes)))
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
			timeoutCh = randomTimeout(r.config.HeartbeatTimeout)

			r.broadcastHeartbeat(ctx, heartbeatCh)

		case heartbeat := <-heartbeatCh:
			r.handleHeartbeatResult(heartbeat)
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

	for _, peer := range r.peers {
		peer := peer

		go func() {
			resp, err := peer.AppendEntries(ctx, req)
			if err != nil {
				r.logger.Error("fail to send AppendEntries RPC", zap.Error(err), zap.Uint32("peer", peer.id))
			}

			heartbeatCh <- &heartbeatResult{AppendEntriesResponse: resp, peerId: peer.id}
		}()
	}
}

func (r *raft) handleHeartbeatResult(heartbeat *heartbeatResult) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if heartbeat.GetTerm() > r.currentTerm {
		r.toFollower(heartbeat.GetTerm())

		r.logger.Info("receive new term on AppendEntries response, fallback to follower", zap.Uint32("peer", heartbeat.peerId))

		return
	}
}
