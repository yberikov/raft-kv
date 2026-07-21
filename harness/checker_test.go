package harness

import (
	"testing"

	"raft-kv/raft"
)

func TestCheckElectionSafety(t *testing.T) {
	tests := []struct {
		name      string
		statuses  []raft.Status
		wantError bool
	}{
		{
			name: "two leaders in the same term is a violation",
			statuses: []raft.Status{
				{Id: 1, Term: 3, State: raft.LeaderState},
				{Id: 2, Term: 3, State: raft.LeaderState},
				{Id: 3, Term: 3, State: raft.FollowerState},
			},
			wantError: true,
		},
		{
			name: "single leader is fine",
			statuses: []raft.Status{
				{Id: 1, Term: 3, State: raft.LeaderState},
				{Id: 2, Term: 3, State: raft.FollowerState},
				{Id: 3, Term: 3, State: raft.FollowerState},
			},
			wantError: false,
		},
		{
			name: "no leader yet is fine",
			statuses: []raft.Status{
				{Id: 1, Term: 3, State: raft.CandidateState},
				{Id: 2, Term: 3, State: raft.FollowerState},
				{Id: 3, Term: 3, State: raft.FollowerState},
			},
			wantError: false,
		},
		{
			name: "leaders in different terms is fine (a stale leader hasn't stepped down yet)",
			statuses: []raft.Status{
				{Id: 1, Term: 3, State: raft.LeaderState},
				{Id: 2, Term: 4, State: raft.LeaderState},
				{Id: 3, Term: 4, State: raft.FollowerState},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckElectionSafety(tt.statuses)
			if tt.wantError && err == nil {
				t.Fatalf("expected an election safety violation, got nil")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("expected no violation, got: %v", err)
			}
		})
	}
}

func TestCheckLogMatching(t *testing.T) {
	dummy := raft.Entry{Cmd: nil, Term: 0}

	tests := []struct {
		name      string
		statuses  []raft.Status
		wantError bool
	}{
		{
			name: "identical logs are fine",
			statuses: []raft.Status{
				{Id: 1, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}, {Cmd: "b", Term: 2}}},
				{Id: 2, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}, {Cmd: "b", Term: 2}}},
			},
			wantError: false,
		},
		{
			name: "a shorter log that matches the common prefix is fine (still catching up)",
			statuses: []raft.Status{
				{Id: 1, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}, {Cmd: "b", Term: 2}}},
				{Id: 2, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}}},
			},
			wantError: false,
		},
		{
			name: "a diverged tail from an older term is fine — it's normal pre-repair state, not a violation",
			statuses: []raft.Status{
				{Id: 1, Log: []raft.Entry{dummy, {Cmd: "a", Term: 5}, {Cmd: "b", Term: 5}}},
				{Id: 2, Log: []raft.Entry{dummy, {Cmd: "x", Term: 3}, {Cmd: "y", Term: 3}, {Cmd: "z", Term: 4}}},
			},
			wantError: false,
		},
		{
			name: "same index and term but different command is a violation",
			statuses: []raft.Status{
				{Id: 1, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}, {Cmd: "b", Term: 2}, {Cmd: "c", Term: 2}}},
				{Id: 2, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}, {Cmd: "b", Term: 2}, {Cmd: "X", Term: 2}}},
			},
			wantError: true,
		},
		{
			name: "violation surfaces across a three-node cluster even when only one pair disagrees",
			statuses: []raft.Status{
				{Id: 1, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}}},
				{Id: 2, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}}},
				{Id: 3, Log: []raft.Entry{dummy, {Cmd: "X", Term: 1}}},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckLogMatching(tt.statuses)
			if tt.wantError && err == nil {
				t.Fatalf("expected a log matching violation, got nil")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("expected no violation, got: %v", err)
			}
		})
	}
}

func TestCommittedLogMerge(t *testing.T) {
	dummy := raft.Entry{Cmd: nil, Term: 0}

	tests := []struct {
		name      string
		statuses  []raft.Status
		wantError bool
	}{
		{
			name: "agreeing committed entries across nodes are fine",
			statuses: []raft.Status{
				{Id: 1, CommitIndex: 2, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}, {Cmd: "b", Term: 2}}},
				{Id: 2, CommitIndex: 2, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}, {Cmd: "b", Term: 2}}},
			},
			wantError: false,
		},
		{
			name: "two nodes committing different commands at the same index is a violation",
			statuses: []raft.Status{
				{Id: 1, CommitIndex: 1, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}}},
				{Id: 2, CommitIndex: 1, Log: []raft.Entry{dummy, {Cmd: "X", Term: 1}}},
			},
			wantError: true,
		},
		{
			name: "a mismatch past CommitIndex is ignored — uncommitted tail can disagree",
			statuses: []raft.Status{
				{Id: 1, CommitIndex: 0, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}}},
				{Id: 2, CommitIndex: 0, Log: []raft.Entry{dummy, {Cmd: "X", Term: 1}}},
			},
			wantError: false,
		},
		{
			name: "the commitIndex entry itself is included, not just the entries before it",
			statuses: []raft.Status{
				{Id: 1, CommitIndex: 1, Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}}},
				{Id: 2, CommitIndex: 1, Log: []raft.Entry{dummy, {Cmd: "X", Term: 1}}},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := NewCommittedLog()
			err := cl.Merge(tt.statuses)
			if tt.wantError && err == nil {
				t.Fatalf("expected a state machine safety violation, got nil")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("expected no violation, got: %v", err)
			}
		})
	}
}

func TestCheckLeaderCompleteness(t *testing.T) {
	dummy := raft.Entry{Cmd: nil, Term: 0}
	canonical := map[int]raft.Entry{
		0: dummy,
		1: {Cmd: "a", Term: 1},
		5: {Cmd: "z", Term: 3},
	}

	tests := []struct {
		name      string
		status    raft.Status
		wantError bool
	}{
		{
			name: "a follower is never checked, regardless of its log",
			status: raft.Status{
				Id: 1, State: raft.FollowerState,
				Log: []raft.Entry{dummy},
			},
			wantError: false,
		},
		{
			name: "a leader missing a committed entry entirely is a violation",
			status: raft.Status{
				Id: 1, State: raft.LeaderState,
				Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}},
			},
			wantError: true,
		},
		{
			name: "a leader with a conflicting entry at a committed index is a violation",
			status: raft.Status{
				Id: 1, State: raft.LeaderState,
				Log: []raft.Entry{dummy, {Cmd: "X", Term: 1}, {}, {}, {}, {Cmd: "z", Term: 3}},
			},
			wantError: true,
		},
		{
			name: "a leader holding every committed entry is fine",
			status: raft.Status{
				Id: 1, State: raft.LeaderState,
				Log: []raft.Entry{dummy, {Cmd: "a", Term: 1}, {}, {}, {}, {Cmd: "z", Term: 3}},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := NewCommittedLog()
			for i, e := range canonical {
				cl.log[i] = e
			}
			err := cl.CheckLeaderCompleteness(tt.status)
			if tt.wantError && err == nil {
				t.Fatalf("expected a leader completeness violation, got nil")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("expected no violation, got: %v", err)
			}
		})
	}
}
