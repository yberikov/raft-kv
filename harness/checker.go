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

// firstVerifiableIndex returns the lowest logical index in status.Log whose
// Cmd can be trusted for comparison. Log[0] represents logical index
// StartIndex; once StartIndex > 0 (the log has been compacted), that
// position is a synthetic boundary placeholder carrying only a Term, not
// the original Cmd, so it's excluded rather than compared as real content.
func firstVerifiableIndex(status raft.Status) int {
	if status.StartIndex > 0 {
		return status.StartIndex + 1
	}
	return status.StartIndex
}

func (c CommittedLog) Merge(statuses []raft.Status) error {
	for _, status := range statuses {
		lo := firstVerifiableIndex(status)
		hi := status.CommitIndex
		if last := status.StartIndex + len(status.Log) - 1; hi > last {
			hi = last
		}
		for i := lo; i <= hi; i++ {
			entry := status.Log[i-status.StartIndex]
			if existing, ok := c.log[i]; ok {
				if existing.Term != entry.Term || existing.Cmd != entry.Cmd {
					return fmt.Errorf("state machine safety violated at index %d: %v != %v", i, existing, entry)
				}

			}
			c.log[i] = entry
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
			if entry.Term > status.Term {
				continue
			}
			// Compacted away on the leader before this check ran — trust
			// the compaction rather than the (no longer present) raw entry.
			if i < firstVerifiableIndex(status) {
				continue
			}
			pos := i - status.StartIndex
			if pos >= len(status.Log) {
				return fmt.Errorf("LeaderCompleteness violated at index %d, index does not exist in leader", i)
			}
			if entry.Term != status.Log[pos].Term || entry.Cmd != status.Log[pos].Cmd {
				return fmt.Errorf("LeaderCompleteness violated at index %d: %v != %v", i, entry, status.Log[pos])
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

			lo := max(firstVerifiableIndex(statusA), firstVerifiableIndex(statusB))
			lastA := statusA.StartIndex + len(statusA.Log) - 1
			lastB := statusB.StartIndex + len(statusB.Log) - 1
			hi := min(lastA, lastB)
			for i := lo; i <= hi; i++ {
				entryA := statusA.Log[i-statusA.StartIndex]
				entryB := statusB.Log[i-statusB.StartIndex]
				if entryA.Term == entryB.Term && entryA.Cmd != entryB.Cmd {
					return fmt.Errorf("log mismatch at index %d between node %d and %d: %v != %v",
						i, statusA.Id, statusB.Id, entryA, entryB)
				}
			}
		}
	}
	return nil
}
