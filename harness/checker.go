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
