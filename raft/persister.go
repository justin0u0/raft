package raft

import "sync"

type Persister interface {
	SaveRaftState(raftState []byte) error
	LoadRaftState() ([]byte, error)
}

type persister struct {
	raftState []byte
	mu        *sync.Mutex
}

var _ Persister = (*persister)(nil)

func newPersister() *persister {
	return &persister{
		mu: &sync.Mutex{},
	}
}

func (p *persister) SaveRaftState(raftState []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.raftState = raftState

	return nil
}

func (p *persister) LoadRaftState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.raftState, nil
}
