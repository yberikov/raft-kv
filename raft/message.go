package raft

type Message struct {
	Term         uint64
	FromId       int
	ToId         int
	LastLogIndex int
	LastLogTerm  uint64

	Type    MsgType
	Success bool // result to RequestVote

	Entries     []Entry
	CommitIndex int

	ProposeCmd []Command

	SnapshotIndex int
	SnapshotTerm  uint64
	SnapshotData  any
}

var (
	MsgVoteRequest             MsgType = "vote_request"
	MsgVoteResponse            MsgType = "vote_response"
	MsgAppendRequest           MsgType = "append_request"
	MsgAppendResponse          MsgType = "append_response"
	MsgProposeRequest          MsgType = "propose_request"
	MsgInstallSnapshotRequest  MsgType = "install_snapshot_request"
	MsgInstallSnapshotResponse MsgType = "install_snapshot_response"
)
