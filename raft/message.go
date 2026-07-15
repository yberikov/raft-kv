package raft

type Message struct {
	Term         uint64
	FromId       uint64
	ToId         uint64
	LastLogIndex int
	LastLogTerm  uint64

	Type    MsgType
	Success bool // result to RequestVote

	Entries     []Entry
	CommitIndex int
}

var (
	MsgVoteRequest    MsgType = "vote_request"
	MsgVoteResponse   MsgType = "vote_response"
	MsgAppendRequest  MsgType = "append_request"
	MsgAppendResponse MsgType = "append_response"
)
