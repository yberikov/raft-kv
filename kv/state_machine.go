package kv

import (
	"bytes"
	"encoding/gob"
	"raft-kv/raft"
)

type snapshotPayload struct {
	Data     map[string]string
	Sessions map[string]session
}

type session struct {
	Seq    uint64
	Result Result
}

type Result struct {
	Value string
	Found bool // Get, Del: key existed
	Ok    bool // Cas: the swap happened
	Err   string
}

type StateMachine struct {
	mp       map[string]string
	sessions map[string]session
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		mp:       make(map[string]string),
		sessions: make(map[string]session),
	}
}

func (st *StateMachine) Apply(entries []raft.Entry) []Result {
	out := make([]Result, len(entries))
	for i, msg := range entries {
		if msg.Cmd.ClientID != "" {
			if s, ok := st.sessions[msg.Cmd.ClientID]; ok && s.Seq >= msg.Cmd.Seq {
				if s.Seq == msg.Cmd.Seq {
					out[i] = s.Result
				} else {
					out[i] = Result{Err: "session too old"}
				}
				continue
			}
		}

		var res Result
		switch msg.Cmd.Op {
		case raft.OpGet:
			if cur, ok := st.mp[msg.Cmd.Key]; ok {
				res.Value = cur
				res.Found = true
			}
		case raft.OpPut:
			st.mp[msg.Cmd.Key] = msg.Cmd.Value
		case raft.OpDel:
			if _, ok := st.mp[msg.Cmd.Key]; ok {
				delete(st.mp, msg.Cmd.Key)
				res.Found = true
			}
		case raft.OpCas:
			if cur, ok := st.mp[msg.Cmd.Key]; ok && cur == msg.Cmd.Expected {
				st.mp[msg.Cmd.Key] = msg.Cmd.Value
				res.Ok = true
			}
		}
		if msg.Cmd.ClientID != "" {
			st.sessions[msg.Cmd.ClientID] = session{
				Seq:    msg.Cmd.Seq,
				Result: res,
			}
		}
		out[i] = res
	}
	return out
}

func (st *StateMachine) Snapshot() any {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(snapshotPayload{st.mp, st.sessions})
	return buf.Bytes()
}

func (st *StateMachine) Restore(data any) {
	var payload snapshotPayload
	gob.NewDecoder(bytes.NewReader(data.([]byte))).Decode(&payload)
	st.mp = payload.Data
	st.sessions = payload.Sessions
}
