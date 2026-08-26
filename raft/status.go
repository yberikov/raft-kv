package raft

type Status struct {
	Id          int
	Term        uint64
	State       stateType
	CommitIndex int
	Log         []Entry
	// StartIndex is the logical index Log[0] represents. It's 0 until the
	// log has been compacted via CompactLog/InstallSnapshot, after which
	// Log[i] corresponds to logical index StartIndex+i.
	StartIndex int
}
