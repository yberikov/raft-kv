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
