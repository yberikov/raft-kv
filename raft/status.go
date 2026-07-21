package raft

type Status struct {
	Id          int
	Term        uint64
	State       stateType
	CommitIndex int
	Log         []Entry
}
