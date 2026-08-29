package raft

import "testing"

// TestProposeReturnsIndexOfFirstEntry pins down the contract the driver's
// pending-waiter registry depends on: the returned index is where cmds[0]
// landed, and cmds[i] follows at index+i. An off-by-one here is silent and
// dangerous -- a waiter keyed one slot early resolves against a real
// committed entry with a plausible term and hands the client someone else's
// result.
func TestProposeReturnsIndexOfFirstEntry(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3},
		withTerm(3), withState(LeaderState),
		withNextIndex(2, 1), withNextIndex(3, 1))

	index, term, ok := c.Propose([]Command{{Key: "a"}, {Key: "b"}, {Key: "c"}})
	if !ok {
		t.Fatal("Propose on a leader returned ok=false")
	}
	if index != 1 || term != 3 {
		t.Fatalf("Propose = (%d, %d), want (1, 3)", index, term)
	}

	for i, want := range []string{"a", "b", "c"} {
		entry := c.log[index+i-c.startIndex]
		if entry.Cmd.Key != want || entry.Term != 3 {
			t.Fatalf("log[%d] = %+v, want Key=%q Term=3 -- batch entries must occupy consecutive indices at one term",
				index+i, entry, want)
		}
	}
}

// TestProposeOnFollowerIsRejectedAndHintsAtTheLeader covers the redirect path:
// a rejected proposal must leave the log untouched and leave behind a usable
// hint for the client to retry against.
func TestProposeOnFollowerIsRejectedAndHintsAtTheLeader(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(2))

	// Learn about the leader the ordinary way, via a heartbeat from node 3.
	c.Step(Message{
		FromId: 3, ToId: 1, Type: MsgAppendRequest, Term: 2,
		LastLogIndex: 0, LastLogTerm: 0,
	})

	before := len(c.log)
	index, term, ok := c.Propose([]Command{{Key: "a"}})
	if ok {
		t.Fatalf("Propose on a follower returned ok=true (index=%d term=%d)", index, term)
	}
	if len(c.log) != before {
		t.Fatalf("log grew from %d to %d entries -- a rejected proposal must append nothing", before, len(c.log))
	}
	if got := c.Status().LeaderId; got != 3 {
		t.Fatalf("Status().LeaderId = %d, want 3 -- a rejected proposal must leave a redirect hint", got)
	}
}

func TestProposeOfEmptyBatchIsRejected(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(3), withState(LeaderState))

	before := len(c.log)
	if index, term, ok := c.Propose(nil); ok {
		t.Fatalf("Propose(nil) = (%d, %d, true), want ok=false -- there is no index to report", index, term)
	}
	if len(c.log) != before {
		t.Fatalf("log grew from %d to %d entries on an empty proposal", before, len(c.log))
	}
}

// TestProposeReplicatesImmediately covers the latency choice made in Propose:
// entries go out on the proposal itself rather than waiting for the next
// replication tick.
func TestProposeReplicatesImmediately(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3},
		withTerm(3), withState(LeaderState),
		withNextIndex(2, 1), withNextIndex(3, 1))

	c.Propose([]Command{{Key: "a"}})

	msgs := c.Ready().Messages
	if len(msgs) != 2 {
		t.Fatalf("got %d messages after Propose, want 2 (one per follower): %+v", len(msgs), msgs)
	}
	for _, m := range msgs {
		if m.Type != MsgAppendRequest {
			t.Fatalf("message type = %q, want %q", m.Type, MsgAppendRequest)
		}
		if len(m.Entries) != 1 || m.Entries[0].Cmd.Key != "a" {
			t.Fatalf("message to %d carried %+v, want the proposed entry", m.ToId, m.Entries)
		}
	}
}

// TestLeaderIdIsSetByRejectedAppendEntries is the subtle case: the three
// log-consistency rejections in handleAppendEntriesRequest are not authority
// rejections. The sender is still a legitimate current-term leader probing
// backwards for the divergence point, and a follower undergoing log repair
// must not report "leader unknown" for the whole repair window -- which is
// exactly when clients are retrying hardest.
func TestLeaderIdIsSetByRejectedAppendEntries(t *testing.T) {
	tests := []struct {
		name string
		req  Message
	}{
		{
			name: "prevLogTerm mismatch",
			req:  Message{FromId: 2, ToId: 1, Type: MsgAppendRequest, Term: 4, LastLogIndex: 1, LastLogTerm: 9},
		},
		{
			name: "prevLogIndex past the end of the log",
			req:  Message{FromId: 2, ToId: 1, Type: MsgAppendRequest, Term: 4, LastLogIndex: 7, LastLogTerm: 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCore(t, 1, []int{1, 2, 3},
				withTerm(4), withLog(Entry{Cmd: Command{Key: "a"}, Term: 1}))

			resp := stepAndGetResponse(t, c, tt.req)
			if resp.Success {
				t.Fatalf("expected the append to be rejected, got Success=true")
			}
			if c.leaderId != 2 {
				t.Fatalf("leaderId = %d, want 2 -- a log-consistency rejection does not mean the sender is not the leader", c.leaderId)
			}
		})
	}
}

// TestLeaderIdSurvivesAStepUpToTheSendersTerm is a regression test for
// ordering: both handleAppendEntriesRequest and handleInstallSnapshotRequest
// call becomeFollower (which clears the hint) when the sender's term is
// higher, so the hint must be recorded after that call, not before. The
// snapshot case is the one that bites -- a follower far enough behind to need
// a snapshot is usually behind because it missed an election, so the snapshot
// almost always arrives from a higher-term leader.
func TestLeaderIdSurvivesAStepUpToTheSendersTerm(t *testing.T) {
	tests := []struct {
		name string
		req  Message
	}{
		{
			name: "append entries",
			req:  Message{FromId: 2, ToId: 1, Type: MsgAppendRequest, Term: 5, LastLogIndex: 0, LastLogTerm: 0},
		},
		{
			name: "install snapshot",
			req: Message{FromId: 2, ToId: 1, Type: MsgInstallSnapshotRequest, Term: 5,
				SnapshotIndex: 4, SnapshotTerm: 5, SnapshotData: []byte("state-as-of-4")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(1))

			c.Step(tt.req)

			if c.currentTerm != 5 {
				t.Fatalf("currentTerm = %d, want 5", c.currentTerm)
			}
			if c.leaderId != 2 {
				t.Fatalf("leaderId = %d, want 2 -- becomeFollower clears the hint, so it must be recorded afterwards", c.leaderId)
			}
		})
	}
}

// TestLeaderIdIsNotSetByAStaleTermLeader is the one case where the sender
// really is not a leader we recognize.
func TestLeaderIdIsNotSetByAStaleTermLeader(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(5))

	c.Step(Message{FromId: 2, ToId: 1, Type: MsgAppendRequest, Term: 3, LastLogIndex: 0, LastLogTerm: 0})

	if c.leaderId != 0 {
		t.Fatalf("leaderId = %d, want 0 -- a message from an older term must not update the hint", c.leaderId)
	}
}

func TestLeaderIdIsClearedWhenStartingAnElection(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(4))
	c.leaderId = 2

	c.electionElapsed = c.electionTimeout + 1
	c.Tick()

	if c.state != CandidateState {
		t.Fatalf("state = %q, want %q", c.state, CandidateState)
	}
	if c.leaderId != 0 {
		t.Fatalf("leaderId = %d, want 0 -- a node campaigning for a new term knows of no leader", c.leaderId)
	}
}

func TestLeaderIdIsClearedOnSteppingDownToAHigherTerm(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withTerm(4))
	c.leaderId = 2

	c.Step(Message{FromId: 3, ToId: 1, Type: MsgVoteRequest, Term: 9, LastLogIndex: 0, LastLogTerm: 0})

	if c.leaderId != 0 {
		t.Fatalf("leaderId = %d, want 0 -- the term advanced and its leader is not yet known", c.leaderId)
	}
}

func TestLeaderIdPointsAtSelfAfterWinningAnElection(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3},
		withTerm(4), withState(CandidateState), withVotedFor(1), withVotesGranted(1))

	c.Step(Message{FromId: 2, ToId: 1, Type: MsgVoteResponse, Term: 4, Success: true})

	if c.state != LeaderState {
		t.Fatalf("state = %q, want %q", c.state, LeaderState)
	}
	if c.leaderId != 1 {
		t.Fatalf("leaderId = %d, want 1 -- a leader is its own redirect target", c.leaderId)
	}
}
