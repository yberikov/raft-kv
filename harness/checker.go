package harness

import (
	"fmt"
	"raft-kv/raft"
)

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
