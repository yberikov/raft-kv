package storage

import "raft-kv/raft"

// pendingFsync is one appended batch waiting to become durable: index is the
// LastIndex() value once it's covered, dueAt is the tick it becomes durable.
type pendingFsync struct {
	index int
	dueAt int
}

type InMemoryStorage struct {
	Term      uint64
	VotedFor  int
	log       []raft.Entry
	staleness int
	now       int
	durable   int
	pending   []pendingFsync
}

func NewInMemoryStorage(staleness int) *InMemoryStorage {
	return &InMemoryStorage{log: []raft.Entry{{Cmd: nil, Term: 0}}, staleness: staleness}
}

func (s *InMemoryStorage) PersistState(term uint64, votedFor int) error {
	s.Term = term
	s.VotedFor = votedFor
	return nil
}

func (s *InMemoryStorage) AppendEntries(entries []raft.Entry) error {
	s.log = append(s.log, entries...)
	s.pending = append(s.pending, pendingFsync{index: s.LastIndex(), dueAt: s.now + s.staleness})
	return nil
}

func (s *InMemoryStorage) LastFsyncedIndex() int {
	return s.durable
}

func (s *InMemoryStorage) Tick() {
	s.now++
	for len(s.pending) > 0 && s.pending[0].dueAt <= s.now {
		if s.pending[0].index > s.durable {
			s.durable = s.pending[0].index
		}
		s.pending = s.pending[1:]
	}
}

func (s *InMemoryStorage) Entries(lo, hi int) []raft.Entry {
	return s.log[lo:hi]
}

func (s *InMemoryStorage) LastIndex() int {
	return len(s.log) - 1
}

func (s *InMemoryStorage) TermAt(index int) uint64 {
	return s.log[index].Term
}

func (s *InMemoryStorage) TruncateFrom(index int) error {
	s.log = s.log[:index]
	if s.durable > index-1 {
		s.durable = index - 1
	}
	kept := s.pending[:0]
	for _, p := range s.pending {
		if p.index < index {
			kept = append(kept, p)
		}
	}
	s.pending = kept
	return nil
}
