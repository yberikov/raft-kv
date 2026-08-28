package raft

import "testing"

// TestReadySurfacesEntriesToApplyUpToCommitIndex covers the basic case: only
// committed entries are handed back for application, never anything still
// uncommitted.
func TestReadySurfacesEntriesToApplyUpToCommitIndex(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withLog(
		Entry{Cmd: Command{Key: "a"}, Term: 1},
		Entry{Cmd: Command{Key: "b"}, Term: 1},
		Entry{Cmd: Command{Key: "c"}, Term: 1},
	), withCommitIndex(2))

	ready := c.Ready()
	if len(ready.EntriesToApply) != 2 || ready.EntriesToApply[0].Cmd != (Command{Key: "a"}) || ready.EntriesToApply[1].Cmd != (Command{Key: "b"}) {
		t.Fatalf("EntriesToApply = %+v, want [a b]", ready.EntriesToApply)
	}

	// A second Ready() with no further commit advance must not re-report
	// already-applied entries.
	ready = c.Ready()
	if len(ready.EntriesToApply) != 0 {
		t.Fatalf("expected no entries re-reported, got %+v", ready.EntriesToApply)
	}
}

// TestReadyOnlyReportsNewlyCommittedEntries covers the incremental case: once
// some entries have already been reported, a later Ready() must report only
// the entries newly covered by an advancing commitIndex.
func TestReadyOnlyReportsNewlyCommittedEntries(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withLog(
		Entry{Cmd: Command{Key: "a"}, Term: 1},
		Entry{Cmd: Command{Key: "b"}, Term: 1},
		Entry{Cmd: Command{Key: "c"}, Term: 1},
	), withCommitIndex(1))
	c.Ready() // drain [a]

	c.commitIndex = 3
	ready := c.Ready()
	if len(ready.EntriesToApply) != 2 || ready.EntriesToApply[0].Cmd != (Command{Key: "b"}) || ready.EntriesToApply[1].Cmd != (Command{Key: "c"}) {
		t.Fatalf("EntriesToApply = %+v, want [b c]", ready.EntriesToApply)
	}
}

// TestReadyClampsAppliedIndexAfterInstallSnapshot is the critical safety case
// for the clamp in Ready(): a follower that has applied nothing past index 1
// receives a snapshot boundary at index 5. Without clamping appliedIndex up
// to the new startIndex, the next EntriesToApply slice would be computed with
// a negative offset (indices already folded into the snapshot must never be
// handed back individually).
func TestReadyClampsAppliedIndexAfterInstallSnapshot(t *testing.T) {
	c := newTestCore(t, 2, []int{1, 2, 3}, withLog(Entry{Cmd: Command{Key: "a"}, Term: 1}), withCommitIndex(1))
	c.Ready() // applies "a", appliedIndex now 1

	// Leader sends a snapshot far ahead of anything this follower has applied.
	c.Step(Message{
		FromId: 1, ToId: 2, Type: MsgInstallSnapshotRequest, Term: 1,
		SnapshotIndex: 5, SnapshotTerm: 2, SnapshotData: "state-as-of-5",
	})
	c.log = append(c.log, Entry{Cmd: Command{Key: "f"}, Term: 2})
	c.commitIndex = 6

	ready := c.Ready()
	if len(ready.EntriesToApply) != 1 || ready.EntriesToApply[0].Cmd != (Command{Key: "f"}) {
		t.Fatalf("EntriesToApply = %+v, want [f] -- indices covered by the snapshot must not be replayed individually", ready.EntriesToApply)
	}
}

// TestReadyReportsSnapshotAndApplicableEntriesTogether covers CompactLog and
// newly committed entries beyond it both needing to be reported in the same
// Ready() call, mirroring TestReadyReportsSnapshotAndEntriesTogether for the
// persistence side.
func TestReadyReportsSnapshotAndApplicableEntriesTogether(t *testing.T) {
	c := newTestCore(t, 1, []int{1, 2, 3}, withLog(
		Entry{Cmd: Command{Key: "a"}, Term: 1},
		Entry{Cmd: Command{Key: "b"}, Term: 1},
	), withCommitIndex(2))
	c.Ready() // applies [a b], appliedIndex now 2

	c.CompactLog(1, 1, "snap")
	c.log = append(c.log, Entry{Cmd: Command{Key: "c"}, Term: 1})
	c.commitIndex = 3

	ready := c.Ready()
	if ready.SnapshotIndex != 1 {
		t.Fatalf("SnapshotIndex = %d, want 1", ready.SnapshotIndex)
	}
	if len(ready.EntriesToApply) != 1 || ready.EntriesToApply[0].Cmd != (Command{Key: "c"}) {
		t.Fatalf("EntriesToApply = %+v, want [c]", ready.EntriesToApply)
	}
}
