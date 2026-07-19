package raft

type Status struct {
	Id          uint64
	Term        uint64
	State       stateType
	CommitIndex int
}
