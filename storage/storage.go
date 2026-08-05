package storage

import "raft-kv/raft"

type Storage interface {
	PersistState(term uint64, votedFor int) error
	AppendEntries(entries []raft.Entry) error
	LastFsyncedIndex() int
	Entries(lo, hi int) []raft.Entry
	LastIndex() int
	TermAt(index int) uint64
	TruncateFrom(index int) error

	Tick()
}
