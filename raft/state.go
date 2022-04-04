package raft

import (
	"sync"

	"github.com/justin0u0/raft/pb"
)

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unkwown"
	}
}

type raftState struct {
	// raft state
	state RaftState

	// persistent state on all servers

	currentTerm uint64
	votedFor    uint32
	logs        []*pb.Entry

	// volatile state on all servers

	commitIndex uint64
	lastApplied uint64

	// volatile state on leader

	nextIndex  map[uint32]uint64
	matchIndex map[uint32]uint64

	mu sync.Mutex
}

// getLastLog gets last log id and last log term and returns 0, 0 if not found
func (rs *raftState) getLastLog() (id, term uint64) {
	if len(rs.logs) == 0 {
		return 0, 0
	}

	log := rs.logs[len(rs.logs)-1]

	return log.GetId(), log.GetTerm()
}

// getLog gets the log by the given log id and returns nil if not found
func (rs *raftState) getLog(id uint64) *pb.Entry {
	for i := len(rs.logs) - 1; i >= 0; i-- {
		if log := rs.logs[i]; log.GetId() == id {
			return log
		}
	}

	return nil
}

// appendLogs appends logs to the raft state
func (rs *raftState) appendLogs(logs []*pb.Entry) {
	rs.logs = append(rs.logs, logs...)
}

// deleteLogs deletes all logs after the given log id
func (rs *raftState) deleteLogs(id uint64) {
	index := -1

	// find the smallest log index that is greater then or equal to the given log id
	for i := len(rs.logs) - 1; i >= 0; i-- {
		if rs.logs[i].GetId() >= id {
			index = i
		}
	}

	// deletes all logs after that log
	if index != -1 {
		rs.logs = rs.logs[:index]
	}
}

func (rs *raftState) applyLogs(applyCh chan<- *pb.Entry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	for i := rs.lastApplied + 1; i <= rs.commitIndex; i++ {
		log := rs.getLog(i)
		applyCh <- log

		rs.lastApplied = log.GetId()
	}
}

func (rs *raftState) toFollower(term uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.state = Follower

	if rs.currentTerm < term {
		rs.currentTerm = term
		rs.votedFor = 0
	}
}

func (rs *raftState) toCandidate() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.state = Candidate
}

func (rs *raftState) toLeader() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.state = Leader
}

func (rs *raftState) voteFor(id uint32, voteForSelf bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// if vote for self, increase current term
	if voteForSelf {
		rs.currentTerm++
	}

	rs.votedFor = id
}

func (rs *raftState) setCommitIndex(index uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.commitIndex = index
}
