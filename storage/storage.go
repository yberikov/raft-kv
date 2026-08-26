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

	State() (term uint64, votedFor int)

	PersistSnapshot(index int, term uint64, data any) error
	SnapshotState() (index int, term uint64, data any)

	Tick()

	ReclaimSegments() error
}
