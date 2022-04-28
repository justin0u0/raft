package raft

import (
	"bytes"
	"encoding/gob"
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

// persistence

func (rs *raftState) saveRaftState(p Persister) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	enc.Encode(rs.currentTerm)
	enc.Encode(rs.votedFor)
	enc.Encode(rs.logs)

	if err := p.SaveRaftState(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func (rs *raftState) loadRaftState(p Persister) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	raftState, err := p.LoadRaftState()
	if err != nil {
		return err
	}

	if raftState != nil {
		dec := gob.NewDecoder(bytes.NewBuffer(raftState))
		dec.Decode(&rs.currentTerm)
		dec.Decode(&rs.votedFor)
		dec.Decode(&rs.logs)
	}

	return nil
}

// getLastLog gets last log id and last log term and returns zero-values if not found
func (rs *raftState) getLastLog() (id, term uint64) {
	if len(rs.logs) == 0 {
		return 0, 0
	}

	log := rs.logs[len(rs.logs)-1]

	return log.GetId(), log.GetTerm()
}

// getLog gets the log by the given log id and returns nil if not found
func (rs *raftState) getLog(id uint64) *pb.Entry {
	logs := rs.getLogs(id)
	if len(logs) != 0 {
		return logs[0]
	}

	return nil
}

// getLogs gets all logs from the start id to the end and returns empty list if not found
func (rs *raftState) getLogs(startId uint64) []*pb.Entry {
	if len(rs.logs) == 0 {
		return []*pb.Entry{}
	}

	lastLog := rs.logs[len(rs.logs)-1]
	logIdDiff := int(lastLog.GetId() - startId)
	if len(rs.logs)-1-logIdDiff < 0 {
		return []*pb.Entry{}
	}

	return rs.logs[len(rs.logs)-1-logIdDiff:]
}

// appendLogs appends logs to the raft state
func (rs *raftState) appendLogs(logs []*pb.Entry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.logs = append(rs.logs, logs...)
}

// deleteLogs deletes all logs after the given log id
func (rs *raftState) deleteLogs(id uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	index := -1

	// find the smallest log index that is greater then or equal to the given log id
	for i := len(rs.logs) - 1; i >= 0; i-- {
		if rs.logs[i].GetId() >= id {
			index = i
		}
	}

	// deletes all logs after that log
	if index != -1 {
		rs.logs = rs.logs[:index+1]
	}
}

// applyLogs applies logs between (lastApplied, commitIndex]
func (rs *raftState) applyLogs(applyCh chan<- *pb.Entry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	logs := rs.getLogs(rs.lastApplied + 1)
	for _, log := range logs {
		if log.GetId() > rs.commitIndex {
			break
		}

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

func (rs *raftState) setNextAndMatchIndex(peerId uint32, nextIndex uint64, matchIndex uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.nextIndex[peerId] = nextIndex
	rs.matchIndex[peerId] = matchIndex
}
