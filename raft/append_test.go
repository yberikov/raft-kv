package raft

import "testing"

func TestHandleAppendEntriesRequest(t *testing.T) {
	tests := []struct {
		name             string
		core             func(t *testing.T) *Core
		req              Message
		wantSuccess      bool
		wantTerm         uint64
		wantLastLogIndex int
		check            func(t *testing.T, c *Core)
	}{
		{
			name: "rejects stale term",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 2, []int{1, 2, 3}, withTerm(5))
			},
			req:         Message{Type: MsgAppendRequest, FromId: 1, Term: 3},
			wantSuccess: false,
			wantTerm:    5,
		},
		{
			name: "rejects when the log is shorter than prevLogIndex",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 2, []int{1, 2, 3}, withTerm(1))
			},
			req:         Message{Type: MsgAppendRequest, FromId: 1, Term: 1, LastLogIndex: 5, LastLogTerm: 1},
			wantSuccess: false,
			wantTerm:    1,
		},
		{
			name: "rejects when the term at prevLogIndex doesn't match",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 2, []int{1, 2, 3}, withTerm(2), withLog(Entry{Term: 1})) // log: [dummy@0, e@1(term1)]
			},
			req:         Message{Type: MsgAppendRequest, FromId: 1, Term: 2, LastLogIndex: 1, LastLogTerm: 2},
			wantSuccess: false,
			wantTerm:    2,
		},
		{
			name: "accepts and appends onto a matching prefix",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 2, []int{1, 2, 3}, withTerm(1))
			},
			req: Message{Type: MsgAppendRequest, FromId: 1, Term: 1,
				Entries: []Entry{{Cmd: "x", Term: 1}}},
			wantSuccess:      true,
			wantTerm:         1,
			wantLastLogIndex: 1,
			check: func(t *testing.T, c *Core) {
				if len(c.log) != 2 || c.log[1].Cmd != "x" {
					t.Fatalf("log = %+v, want dummy + {x,1}", c.log)
				}
			},
		},
		{
			name: "truncates a conflicting suffix and appends the leader's entries",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 2, []int{1, 2, 3}, withTerm(3),
					withLog(Entry{Cmd: "old1", Term: 2}, Entry{Cmd: "old2", Term: 2}))
			},
			req: Message{Type: MsgAppendRequest, FromId: 1, Term: 3,
				Entries: []Entry{{Cmd: "new1", Term: 3}}},
			wantSuccess:      true,
			wantTerm:         3,
			wantLastLogIndex: 1,
			check: func(t *testing.T, c *Core) {
				if len(c.log) != 2 || c.log[1].Cmd != "new1" {
					t.Fatalf("log = %+v, want conflicting suffix replaced with {new1,3}", c.log)
				}
			},
		},
		{
			name: "advances commitIndex to min(leaderCommit, lastNewEntryIndex)",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 2, []int{1, 2, 3}, withTerm(1))
			},
			req: Message{Type: MsgAppendRequest, FromId: 1, Term: 1,
				Entries: []Entry{{Cmd: "x", Term: 1}}, CommitIndex: 99},
			wantSuccess:      true,
			wantTerm:         1,
			wantLastLogIndex: 1,
			check: func(t *testing.T, c *Core) {
				if c.commitIndex != 1 {
					t.Fatalf("commitIndex = %d, want min(99,1)=1", c.commitIndex)
				}
			},
		},
		{
			name: "a candidate steps down to follower on a valid append from the current term's leader",
			core: func(t *testing.T) *Core {
				return newTestCore(t, 2, []int{1, 2, 3}, withTerm(1), withState(CandidateState))
			},
			req:         Message{Type: MsgAppendRequest, FromId: 1, Term: 1},
			wantSuccess: true,
			wantTerm:    1,
			check: func(t *testing.T, c *Core) {
				if c.state != FollowerState {
					t.Fatalf("state = %v, want FollowerState", c.state)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.core(t)
			resp := stepAndGetResponse(t, c, tc.req)

			if resp.Type != MsgAppendResponse {
				t.Fatalf("response Type = %v, want MsgAppendResponse", resp.Type)
			}
			if resp.Success != tc.wantSuccess {
				t.Fatalf("Success = %v, want %v", resp.Success, tc.wantSuccess)
			}
			if resp.Term != tc.wantTerm {
				t.Fatalf("response Term = %d, want %d", resp.Term, tc.wantTerm)
			}
			if tc.wantSuccess && resp.LastLogIndex != tc.wantLastLogIndex {
				t.Fatalf("LastLogIndex = %d, want %d", resp.LastLogIndex, tc.wantLastLogIndex)
			}
			if tc.check != nil {
				tc.check(t, c)
			}
		})
	}
}

func TestHandleAppendEntriesResponse(t *testing.T) {
	t.Run("advances nextIndex/matchIndex on success", func(t *testing.T) {
		c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(1), withState(LeaderState),
			withLog(Entry{Term: 1}, Entry{Term: 1}, Entry{Term: 1}), // log: [dummy@0..e@3], so index 3 is valid
			withNextIndex(2, 1), withMatchIndex(2, 0))
		c.Step(Message{Type: MsgAppendResponse, FromId: 2, Term: 1, Success: true, LastLogIndex: 3})

		if c.nextIndex[2] != 4 {
			t.Fatalf("nextIndex[2] = %d, want 4", c.nextIndex[2])
		}
		if c.matchIndex[2] != 3 {
			t.Fatalf("matchIndex[2] = %d, want 3", c.matchIndex[2])
		}
	})

	t.Run("decrements nextIndex on rejection, floored at 0", func(t *testing.T) {
		c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(1), withState(LeaderState), withNextIndex(2, 0))
		c.Step(Message{Type: MsgAppendResponse, FromId: 2, Term: 1, Success: false})
		if c.nextIndex[2] != 0 {
			t.Fatalf("nextIndex[2] = %d, want 0 (must not go negative)", c.nextIndex[2])
		}
	})

	t.Run("commits a current-term entry once a majority has acked it", func(t *testing.T) {
		c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(2), withState(LeaderState),
			withLog(Entry{Term: 1}, Entry{Term: 2})) // log: [dummy@0, e@1(term1), e@2(term2=current)]
		c.Step(Message{Type: MsgAppendResponse, FromId: 2, Term: 2, Success: true, LastLogIndex: 2})
		if c.commitIndex != 2 {
			t.Fatalf("commitIndex = %d, want 2", c.commitIndex)
		}
	})

	t.Run("§5.4.2: a majority on an OLD-term entry alone must not commit it (Figure 8)", func(t *testing.T) {
		c := newTestCore(t, 3, []int{1, 2, 3}, withTerm(3), withState(LeaderState),
			withLog(Entry{Cmd: "a", Term: 1}, Entry{Cmd: "b", Term: 2})) // log: [dummy@0, a@1(term1), b@2(term2)]
		// self (id 3) + peer 2 is already 2 of 3 = majority, but entry at
		// index 2 is from term 2, not the leader's current term 3.
		c.Step(Message{Type: MsgAppendResponse, FromId: 2, Term: 3, Success: true, LastLogIndex: 2})
		if c.commitIndex != 0 {
			t.Fatalf("commitIndex = %d, want 0 — committed an entry (term %d) predating the leader's current term (%d); "+
				"delete the c.log[index].Term == c.currentTerm guard and this test should fail",
				c.commitIndex, c.log[2].Term, c.currentTerm)
		}
	})

	t.Run("commitIndex never regresses", func(t *testing.T) {
		c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(1), withState(LeaderState), withCommitIndex(5))
		c.Step(Message{Type: MsgAppendResponse, FromId: 2, Term: 1, Success: true, LastLogIndex: 2})
		if c.commitIndex != 5 {
			t.Fatalf("commitIndex = %d, want 5 (must never move backwards)", c.commitIndex)
		}
	})

	t.Run("a higher-term response steps the leader down", func(t *testing.T) {
		c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(1), withState(LeaderState))
		c.Step(Message{Type: MsgAppendResponse, FromId: 2, Term: 5, Success: false})
		if c.state != FollowerState {
			t.Fatalf("state = %v, want FollowerState", c.state)
		}
		if c.currentTerm != 5 {
			t.Fatalf("currentTerm = %d, want 5", c.currentTerm)
		}
	})

	t.Run("pitfall #6: a response from an abandoned term must not be applied", func(t *testing.T) {
		// Simulates a duplicated/delayed response from a request the leader
		// sent back when it was still in term 2; it has since moved on to
		// term 5 (e.g. stepped down and been re-elected). A stale response
		// must be a no-op, not treated as a same-term rejection — folding
		// `m.Term != c.currentTerm` into the `!m.Success` branch still
		// decrements nextIndex for a response that isn't really about the
		// current replication attempt at all.
		c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(5), withState(LeaderState), withNextIndex(2, 1))
		c.Step(Message{Type: MsgAppendResponse, FromId: 2, Term: 2, Success: true, LastLogIndex: 9})
		if c.nextIndex[2] != 1 {
			t.Fatalf("nextIndex[2] = %d, want 1 unchanged — a stale term-2 response was applied while leader is at term 5",
				c.nextIndex[2])
		}
	})
}
