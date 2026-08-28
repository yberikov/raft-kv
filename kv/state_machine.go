package kv

import (
	"bytes"
	"encoding/gob"
	"raft-kv/raft"
)

type snapshotPayload struct {
	Data    map[string]string
	LastSeq map[string]uint64
}

type StateMachine struct {
	mp      map[string]string
	lastSeq map[string]uint64
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		mp:      make(map[string]string),
		lastSeq: make(map[string]uint64),
	}
}

func (st *StateMachine) Apply(msgs []raft.Entry) {

	for _, msg := range msgs {
		if msg.Cmd.ClientID != "" {
			if lastSeq, ok := st.lastSeq[msg.Cmd.ClientID]; ok && lastSeq >= msg.Cmd.Seq {
				continue
			}
			st.lastSeq[msg.Cmd.ClientID] = msg.Cmd.Seq
		}
		switch msg.Cmd.Op {
		case raft.OpPut:
			st.mp[msg.Cmd.Key] = msg.Cmd.Value
		case raft.OpDel:
			delete(st.mp, msg.Cmd.Key)
		case raft.OpCas:
			if cur, ok := st.mp[msg.Cmd.Key]; ok && cur == msg.Cmd.Expected {
				st.mp[msg.Cmd.Key] = msg.Cmd.Value
			}
		}
	}
}

func (st *StateMachine) Get(key string) (string, bool) {
	if cur, ok := st.mp[key]; ok {
		return cur, true
	}
	return "", false
}

func (st *StateMachine) Snapshot() any {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(snapshotPayload{st.mp, st.lastSeq})
	return buf.Bytes()
}

func (st *StateMachine) Restore(data any) {
	var payload snapshotPayload
	gob.NewDecoder(bytes.NewReader(data.([]byte))).Decode(&payload)
	st.mp = payload.Data
	st.lastSeq = payload.LastSeq
}
