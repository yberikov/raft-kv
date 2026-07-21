package harness

import (
	"fmt"
	"raft-kv/raft"
)

type CommittedLog struct {
	log map[int]raft.Entry
}

func NewCommittedLog() CommittedLog {
	log := make(map[int]raft.Entry)
	return CommittedLog{log: log}
}

func (c CommittedLog) Merge(statuses []raft.Status) error {
	for _, status := range statuses {
		for i := 0; i <= status.CommitIndex && i < len(status.Log); i++ {
			if existing, ok := c.log[i]; ok {
				if existing.Term != status.Log[i].Term || existing.Cmd != status.Log[i].Cmd {
					return fmt.Errorf("state machine safety violated at index %d: %v != %v", i, existing, status.Log[i])
				}

			}
			c.log[i] = status.Log[i]
		}
	}
	return nil
}

func (c CommittedLog) CheckLeaderCompleteness(statuses []raft.Status) error {
	for _, status := range statuses {
		if status.State != raft.LeaderState {
			continue
		}
		for i, entry := range c.log {

			if i >= len(status.Log) {
				return fmt.Errorf("LeaderCompleteness violated at index %d, index does not exist in leader", i)
			}
			if entry.Term != status.Log[i].Term || entry.Cmd != status.Log[i].Cmd {
				return fmt.Errorf("LeaderCompleteness violated at index %d: %v != %v", i, entry, status.Log[i])
			}

		}
	}
	return nil
}

func CheckElectionSafety(statuses []raft.Status) error {
	mp := make(map[uint64]raft.Status)

	for _, status := range statuses {
		if status.State == raft.LeaderState {
			if _, ok := mp[status.Term]; ok {
				return fmt.Errorf("term %d already has a leader", status.Term)
			}
			mp[status.Term] = status
		}
	}
	return nil
}

func CheckLogMatching(statuses []raft.Status) error {
	for _, statusA := range statuses {
		for _, statusB := range statuses {
			if statusA.Id >= statusB.Id {
				continue
			}

			n := min(len(statusA.Log), len(statusB.Log)) - 1
			for i := 0; i <= n; i++ {
				if statusA.Log[i].Term == statusB.Log[i].Term && statusA.Log[i].Cmd != statusB.Log[i].Cmd {
					return fmt.Errorf("log mismatch at index %d between node %d and %d: %v != %v",
						i, statusA.Id, statusB.Id, statusA.Log[i], statusB.Log[i])
				}
			}
		}
	}
	return nil
}
