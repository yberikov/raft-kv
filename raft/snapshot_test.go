package raft

import (
	"fmt"
	"testing"
)

func TestCompactLogTrimsAndSetsBoundary(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withLog(
		Entry{Cmd: "a", Term: 1},
		Entry{Cmd: "b", Term: 1},
		Entry{Cmd: "c", Term: 2},
	))
	// log: [dummy@0(term0), a@1(term1), b@2(term1), c@3(term2)]

	c.CompactLog(2, 1, "snapshot-data")

	if c.startIndex != 2 || c.startTerm != 1 {
		t.Fatalf("startIndex/startTerm = %d/%d, want 2/1", c.startIndex, c.startTerm)
	}
	if c.snapshotData != "snapshot-data" {
		t.Fatalf("snapshotData = %v, want %q", c.snapshotData, "snapshot-data")
	}
	if c.lastIndex() != 3 {
		t.Fatalf("lastIndex() = %d, want 3", c.lastIndex())
	}
	if c.lastTerm() != 2 {
		t.Fatalf("lastTerm() = %d, want 2", c.lastTerm())
	}
	status := c.Status()
	if status.StartIndex != 2 {
		t.Fatalf("Status().StartIndex = %d, want 2", status.StartIndex)
	}
	if len(status.Log) != 2 || status.Log[0].Cmd != nil || status.Log[1].Cmd != "c" {
		t.Fatalf("Status().Log = %+v, want [boundary@2, c@3]", status.Log)
	}
}

func TestCompactLogIsNoOpWhenAlreadyCompactedOrOutOfRange(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withLog(Entry{Cmd: "a", Term: 1}))

	c.CompactLog(1, 1, "d1")
	if c.startIndex != 1 {
		t.Fatalf("startIndex = %d, want 1", c.startIndex)
	}

	c.CompactLog(1, 1, "d2") // already compacted to 1 — no-op
	if c.snapshotData != "d1" {
		t.Fatalf("snapshotData = %v, want d1 (no-op must not overwrite)", c.snapshotData)
	}

	c.CompactLog(99, 1, "d3") // beyond lastIndex() — no-op
	if c.snapshotData != "d1" {
		t.Fatalf("snapshotData = %v, want d1 (out-of-range compact must not apply)", c.snapshotData)
	}
}

func TestHandleAppendEntriesRequestBehindCompactionReportsStartIndex(t *testing.T) {
	c := newTestCore(t, 2, []int{1, 2, 3}, withLog(Entry{Cmd: "a", Term: 1}, Entry{Cmd: "b", Term: 1}))
	c.CompactLog(2, 1, "snap") // startIndex=2 now, entry "a" no longer exists

	resp := stepAndGetResponse(t, c, Message{
		FromId: 1, ToId: 2, Type: MsgAppendRequest, Term: 1,
		LastLogIndex: 1, LastLogTerm: 1,
	})
	if resp.Success {
		t.Fatalf("expected Success=false for a prevLogIndex before startIndex, got true")
	}
	if resp.LastLogIndex != 2 {
		t.Fatalf("resp.LastLogIndex = %d, want startIndex 2", resp.LastLogIndex)
	}
}

func TestHandleInstallSnapshotRequestInstallsFreshSnapshot(t *testing.T) {
	c := newTestCore(t, 2, []int{1, 2, 3}, withLog(Entry{Cmd: "x", Term: 1}))

	resp := stepAndGetResponse(t, c, Message{
		FromId: 1, ToId: 2, Type: MsgInstallSnapshotRequest, Term: 1,
		SnapshotIndex: 5, SnapshotTerm: 3, SnapshotData: "state-as-of-5",
	})
	if resp.LastLogIndex != 5 {
		t.Fatalf("resp.LastLogIndex = %d, want 5", resp.LastLogIndex)
	}
	if c.startIndex != 5 || c.startTerm != 3 {
		t.Fatalf("startIndex/startTerm = %d/%d, want 5/3", c.startIndex, c.startTerm)
	}
	if c.snapshotData != "state-as-of-5" {
		t.Fatalf("snapshotData = %v, want state-as-of-5", c.snapshotData)
	}
	if c.commitIndex != 5 {
		t.Fatalf("commitIndex = %d, want 5 (snapshot implies committed)", c.commitIndex)
	}
	if c.lastIndex() != 5 {
		t.Fatalf("lastIndex() = %d, want 5", c.lastIndex())
	}
}

func TestHandleInstallSnapshotRequestKeepsConsistentSuffix(t *testing.T) {
	c := newTestCore(t, 2, []int{1, 2, 3}, withLog(
		Entry{Cmd: "a", Term: 1},
		Entry{Cmd: "b", Term: 2},
		Entry{Cmd: "c", Term: 2},
	))
	// log: [dummy@0, a@1(t1), b@2(t2), c@3(t2)]

	resp := stepAndGetResponse(t, c, Message{
		FromId: 1, ToId: 2, Type: MsgInstallSnapshotRequest, Term: 2,
		SnapshotIndex: 2, SnapshotTerm: 2, SnapshotData: "state-as-of-2",
	})
	if resp.LastLogIndex != 3 {
		t.Fatalf("resp.LastLogIndex = %d, want 3 (kept entry c beyond the snapshot)", resp.LastLogIndex)
	}
	if c.startIndex != 2 {
		t.Fatalf("startIndex = %d, want 2", c.startIndex)
	}
	if c.lastIndex() != 3 || c.lastTerm() != 2 {
		t.Fatalf("lastIndex/lastTerm = %d/%d, want 3/2", c.lastIndex(), c.lastTerm())
	}
	status := c.Status()
	if len(status.Log) != 2 || status.Log[1].Cmd != "c" {
		t.Fatalf("Status().Log = %+v, want [boundary@2, c@3]", status.Log)
	}
}

func TestHandleInstallSnapshotRequestIgnoresStaleSnapshot(t *testing.T) {
	c := newTestCore(t, 2, []int{1, 2, 3}, withLog(Entry{Cmd: "a", Term: 1}, Entry{Cmd: "b", Term: 1}))
	c.CompactLog(2, 1, "already-have-this")

	resp := stepAndGetResponse(t, c, Message{
		FromId: 1, ToId: 2, Type: MsgInstallSnapshotRequest, Term: 1,
		SnapshotIndex: 1, SnapshotTerm: 1, SnapshotData: "stale",
	})
	if c.snapshotData != "already-have-this" {
		t.Fatalf("snapshotData = %v, a stale snapshot must not overwrite it", c.snapshotData)
	}
	if resp.LastLogIndex != c.lastIndex() {
		t.Fatalf("resp.LastLogIndex = %d, want current lastIndex %d", resp.LastLogIndex, c.lastIndex())
	}
}

func TestHandleInstallSnapshotResponseUpdatesLeaderTracking(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withState(LeaderState), withTerm(4))
	c.nextIndex[2] = 1
	c.matchIndex[2] = 0

	c.Step(Message{FromId: 2, ToId: 1, Type: MsgInstallSnapshotResponse, Term: 4, LastLogIndex: 10})

	if c.nextIndex[2] != 11 {
		t.Fatalf("nextIndex[2] = %d, want 11", c.nextIndex[2])
	}
	if c.matchIndex[2] != 10 {
		t.Fatalf("matchIndex[2] = %d, want 10", c.matchIndex[2])
	}
}

func TestReplicateLogSendsInstallSnapshotWhenPeerBehindCompaction(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withState(LeaderState), withTerm(3),
		withLog(Entry{Cmd: "a", Term: 1}, Entry{Cmd: "b", Term: 2}, Entry{Cmd: "c", Term: 3}))
	c.CompactLog(2, 2, "snap-data")
	c.nextIndex[2] = 1 // peer needs index 1, which we've compacted away
	c.nextIndex[3] = 4
	c.matchIndex[2] = 0
	c.matchIndex[3] = 3

	c.replicateLog()

	var toTwo *Message
	for i := range c.msgs {
		if c.msgs[i].ToId == 2 {
			toTwo = &c.msgs[i]
		}
	}
	if toTwo == nil {
		t.Fatalf("expected a message sent to node 2")
	}
	if toTwo.Type != MsgInstallSnapshotRequest {
		t.Fatalf("message to node 2 = %s, want %s", toTwo.Type, MsgInstallSnapshotRequest)
	}
	if toTwo.SnapshotIndex != 2 || toTwo.SnapshotTerm != 2 || toTwo.SnapshotData != "snap-data" {
		t.Fatalf("snapshot message = %+v, want index=2 term=2 data=snap-data", toTwo)
	}
}

func TestReadySurfacesSnapshotAfterCompactLog(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withLog(
		Entry{Cmd: "a", Term: 1},
		Entry{Cmd: "b", Term: 1},
		Entry{Cmd: "c", Term: 2},
	))
	c.Ready() // drain the initial log so only the compaction is under test

	c.CompactLog(2, 1, "snapshot-data")

	ready := c.Ready()
	if ready.SnapshotIndex != 2 || ready.SnapshotTerm != 1 {
		t.Fatalf("SnapshotIndex/SnapshotTerm = %d/%d, want 2/1", ready.SnapshotIndex, ready.SnapshotTerm)
	}
	if ready.SnapshotData != "snapshot-data" {
		t.Fatalf("SnapshotData = %v, want snapshot-data", ready.SnapshotData)
	}

	// A second Ready() with no further compaction must not re-report it.
	ready = c.Ready()
	if ready.SnapshotIndex != 0 {
		t.Fatalf("expected no snapshot re-reported, got SnapshotIndex=%d", ready.SnapshotIndex)
	}
}

func TestReadySurfacesSnapshotAfterInstallSnapshot(t *testing.T) {
	c := newTestCore(t, 2, []int{1, 2, 3}, withLog(Entry{Cmd: "x", Term: 1}))
	c.Ready()

	// Step directly (not via stepAndGetResponse, which drains Ready() itself
	// and would report the snapshot before this test gets to check it).
	c.Step(Message{
		FromId: 1, ToId: 2, Type: MsgInstallSnapshotRequest, Term: 1,
		SnapshotIndex: 5, SnapshotTerm: 3, SnapshotData: "state-as-of-5",
	})

	ready := c.Ready()
	if ready.SnapshotIndex != 5 || ready.SnapshotTerm != 3 || ready.SnapshotData != "state-as-of-5" {
		t.Fatalf("ready snapshot fields = %d/%d/%v, want 5/3/state-as-of-5",
			ready.SnapshotIndex, ready.SnapshotTerm, ready.SnapshotData)
	}
}

// TestReadyReportsSnapshotAndEntriesTogether covers the case where a
// compaction and freshly appended entries beyond it both need reporting in
// the same Ready() call.
func TestReadyReportsSnapshotAndEntriesTogether(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withLog(
		Entry{Cmd: "a", Term: 1},
		Entry{Cmd: "b", Term: 1},
	))
	c.Ready()

	c.CompactLog(1, 1, "snap")
	c.log = append(c.log, Entry{Cmd: "c", Term: 1})

	ready := c.Ready()
	if ready.SnapshotIndex != 1 {
		t.Fatalf("SnapshotIndex = %d, want 1", ready.SnapshotIndex)
	}
	if len(ready.EntriesToPersist) != 1 || ready.EntriesToPersist[0].Cmd != "c" {
		t.Fatalf("EntriesToPersist = %+v, want [c]", ready.EntriesToPersist)
	}
}

// TestTrioCatchesUpFollowerViaInstallSnapshot drives the full protocol (not
// just a direct Step() call) to prove a follower that needs entries the
// leader has already compacted away gets caught up correctly and ends up
// with a log and StartIndex that matches the leader's.
func TestTrioCatchesUpFollowerViaInstallSnapshot(t *testing.T) {
	seed := testSeed(t)
	d := newTrio(t, seed)

	// Establish node 1 as leader directly (as TestTrioRepairsDivergedFollower
	// does) — this test is about snapshot catch-up, not election.
	leader := d.nodes[1]
	leader.currentTerm = 5
	leader.state = LeaderState
	for i := 1; i <= 6; i++ {
		leader.log = append(leader.log, Entry{Cmd: fmt.Sprintf("op-%d", i), Term: 5})
	}
	for _, id := range d.ids {
		leader.nextIndex[id] = len(leader.log)
		leader.matchIndex[id] = 0
	}
	leader.commitIndex = 6

	// node 3 is already caught up, so it can't be recruited into a
	// competing election while node 2 catches up.
	d.nodes[3].currentTerm = 5
	for i := 1; i <= 6; i++ {
		d.nodes[3].log = append(d.nodes[3].log, Entry{Cmd: fmt.Sprintf("op-%d", i), Term: 5})
	}
	d.nodes[3].commitIndex = 6

	// node 2 never replicated anything and stays that way until caught up
	// by the leader below.
	d.nodes[2].currentTerm = 5

	// Compact the leader's log up to index 5 — op-6 stays uncompacted so we
	// also exercise "peer needs the boundary entry plus something after it."
	leader.CompactLog(5, 5, "state-as-of-5")
	leader.nextIndex[2] = 1 // node 2 needs index 1, long gone from the leader's log

	var inbox []Message
	for round := 0; round < 50; round++ {
		inbox = d.round(inbox)
	}

	follower := d.nodes[2]
	if follower.startIndex != leader.startIndex {
		t.Fatalf("seed=%d: follower startIndex = %d, want %d", seed, follower.startIndex, leader.startIndex)
	}
	if follower.snapshotData != "state-as-of-5" {
		t.Fatalf("seed=%d: follower snapshotData = %v, want state-as-of-5", seed, follower.snapshotData)
	}
	if fmt.Sprint(follower.log) != fmt.Sprint(leader.log) {
		t.Fatalf("seed=%d: follower log did not converge with leader:\n  follower: %v\n  leader: %v",
			seed, follower.log, leader.log)
	}
	if follower.commitIndex < 5 {
		t.Fatalf("seed=%d: follower commitIndex = %d, want at least 5", seed, follower.commitIndex)
	}
}
