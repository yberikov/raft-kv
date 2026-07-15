package raft

import "testing"

func TestHandleVoteRequest(t *testing.T) {
	tests := []struct {
		name         string
		core         func(t *testing.T) *Core
		req          Message
		wantSuccess  bool
		wantTerm     uint64
		wantVotedFor uint64
		wantState    stateType
	}{
		{
			name: "rejects stale term",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 1, []uint64{1, 2, 3}, withTerm(5))
			},
			req:          Message{Type: MsgVoteRequest, FromId: 2, Term: 3},
			wantSuccess:  false,
			wantTerm:     5,
			wantVotedFor: 0,
			wantState:    FollowerState,
		},
		{
			name: "grants vote to up-to-date candidate with empty log",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 1, []uint64{1, 2, 3})
			},
			req:          Message{Type: MsgVoteRequest, FromId: 2, Term: 1},
			wantSuccess:  true,
			wantTerm:     1,
			wantVotedFor: 2,
			wantState:    FollowerState,
		},
		{
			name: "steps down and votes when candidate's term is higher, even from a leader",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 1, []uint64{1, 2, 3}, withTerm(2), withState(LeaderState))
			},
			req:          Message{Type: MsgVoteRequest, FromId: 2, Term: 5},
			wantSuccess:  true,
			wantTerm:     5,
			wantVotedFor: 2,
			wantState:    FollowerState,
		},
		{
			name: "refuses a second vote to a different candidate in the same term",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 1, []uint64{1, 2, 3}, withTerm(3), withVotedFor(2))
			},
			req:          Message{Type: MsgVoteRequest, FromId: 4, Term: 3},
			wantSuccess:  false,
			wantTerm:     3,
			wantVotedFor: 2,
			wantState:    FollowerState,
		},
		{
			name: "re-grants to the same candidate already voted for in this term",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 1, []uint64{1, 2, 3}, withTerm(3), withVotedFor(2))
			},
			req:          Message{Type: MsgVoteRequest, FromId: 2, Term: 3},
			wantSuccess:  true,
			wantTerm:     3,
			wantVotedFor: 2,
			wantState:    FollowerState,
		},
		{
			name: "rejects a candidate whose log ends in an older term",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 1, []uint64{1, 2, 3}, withTerm(3), withLog(Entry{term: 2}, Entry{term: 3}))
			},
			req:          Message{Type: MsgVoteRequest, FromId: 2, Term: 3, LastLogIndex: 2, LastLogTerm: 2},
			wantSuccess:  false,
			wantTerm:     3,
			wantVotedFor: 0,
			wantState:    FollowerState,
		},
		{
			name: "rejects a candidate with the same last term but a shorter log",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 1, []uint64{1, 2, 3}, withTerm(3), withLog(Entry{term: 2}, Entry{term: 3}))
			},
			req:          Message{Type: MsgVoteRequest, FromId: 2, Term: 3, LastLogIndex: 1, LastLogTerm: 3},
			wantSuccess:  false,
			wantTerm:     3,
			wantVotedFor: 0,
			wantState:    FollowerState,
		},
		{
			name: "grants a candidate with the same last term and an equal-or-longer log",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 1, []uint64{1, 2, 3}, withTerm(3), withLog(Entry{term: 2}, Entry{term: 3}))
			},
			req:          Message{Type: MsgVoteRequest, FromId: 2, Term: 3, LastLogIndex: 2, LastLogTerm: 3},
			wantSuccess:  true,
			wantTerm:     3,
			wantVotedFor: 2,
			wantState:    FollowerState,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.core(t)
			resp := stepAndGetResponse(t, c, tc.req)

			if resp.Type != MsgVoteResponse {
				t.Fatalf("response Type = %v, want MsgVoteResponse", resp.Type)
			}
			if resp.Success != tc.wantSuccess {
				t.Fatalf("Success = %v, want %v", resp.Success, tc.wantSuccess)
			}
			if resp.Term != tc.wantTerm {
				t.Fatalf("response Term = %d, want %d", resp.Term, tc.wantTerm)
			}
			if c.votedFor != tc.wantVotedFor {
				t.Fatalf("votedFor = %d, want %d", c.votedFor, tc.wantVotedFor)
			}
			if c.state != tc.wantState {
				t.Fatalf("state = %v, want %v", c.state, tc.wantState)
			}
			if c.currentTerm != tc.wantTerm {
				t.Fatalf("currentTerm = %d, want %d", c.currentTerm, tc.wantTerm)
			}
		})
	}
}

func TestHandleVoteResponse(t *testing.T) {
	t.Run("majority of grants elects the candidate and initializes leader state", func(t *testing.T) {
		c := newTestCore(t, 1, []uint64{1, 2, 3}, withState(CandidateState), withTerm(1), withVotesGranted(1))
		c.Step(Message{Type: MsgVoteResponse, FromId: 2, Term: 1, Success: true})

		if c.state != LeaderState {
			t.Fatalf("state = %v, want LeaderState", c.state)
		}
		for _, peer := range []uint64{2, 3} {
			if got, want := c.nextIndex[peer], c.lastIndex()+1; got != want {
				t.Fatalf("nextIndex[%d] = %d, want %d (Figure 2: reinitialized to leader's last log index + 1)", peer, got, want)
			}
			if got := c.matchIndex[peer]; got != 0 {
				t.Fatalf("matchIndex[%d] = %d, want 0", peer, got)
			}
		}
	})

	t.Run("a single vote (self only) is not a majority of 3", func(t *testing.T) {
		c := newTestCore(t, 1, []uint64{1, 2, 3}, withState(CandidateState), withTerm(1), withVotesGranted(1))
		if c.state != CandidateState {
			t.Fatalf("state = %v, want CandidateState (self-vote alone must not win a 3-node election)", c.state)
		}
	})

	t.Run("a denied vote is not counted", func(t *testing.T) {
		c := newTestCore(t, 1, []uint64{1, 2, 3}, withState(CandidateState), withTerm(1), withVotesGranted(1))
		c.Step(Message{Type: MsgVoteResponse, FromId: 2, Term: 1, Success: false})
		if c.state != CandidateState {
			t.Fatalf("state = %v, want CandidateState", c.state)
		}
		if len(c.votesGranted) != 1 {
			t.Fatalf("votesGranted = %d, want 1 (denied vote must not be counted)", len(c.votesGranted))
		}
	})

	t.Run("a response from an election we've already abandoned (stale term) is ignored", func(t *testing.T) {
		c := newTestCore(t, 1, []uint64{1, 2, 3}, withState(CandidateState), withTerm(6), withVotesGranted(1))
		c.Step(Message{Type: MsgVoteResponse, FromId: 2, Term: 5, Success: true})
		if c.state != CandidateState {
			t.Fatalf("state = %v, want CandidateState (pitfall #6: stale-term reply must not be treated as current)", c.state)
		}
		if len(c.votesGranted) != 1 {
			t.Fatalf("votesGranted = %d, want 1", len(c.votesGranted))
		}
	})

	t.Run("a higher-term response steps the candidate down", func(t *testing.T) {
		c := newTestCore(t, 1, []uint64{1, 2, 3}, withState(CandidateState), withTerm(1), withVotesGranted(1))
		c.Step(Message{Type: MsgVoteResponse, FromId: 2, Term: 5, Success: false})
		if c.state != FollowerState {
			t.Fatalf("state = %v, want FollowerState", c.state)
		}
		if c.currentTerm != 5 {
			t.Fatalf("currentTerm = %d, want 5", c.currentTerm)
		}
	})
}
