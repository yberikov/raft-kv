package raft

import (
	"math/rand"
	"testing"
)

func TestRestoreAppliesRecoveredState(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewSource(seed))
	c := NewCore(1, []int{1, 2, 3}, 10, 20, rng, 3)

	recoveredLog := []Entry{
		{Cmd: nil, Term: 0},
		{Cmd: "a", Term: 1},
		{Cmd: "b", Term: 2},
	}
	c.Restore(2, 3, recoveredLog, 0, 0, nil)

	if c.currentTerm != 2 {
		t.Fatalf("currentTerm = %d, want 2", c.currentTerm)
	}
	if c.votedFor != 3 {
		t.Fatalf("votedFor = %d, want 3", c.votedFor)
	}
	if len(c.log) != 3 {
		t.Fatalf("log length = %d, want 3", len(c.log))
	}
	if c.lastIndex() != 2 {
		t.Fatalf("lastIndex() = %d, want 2", c.lastIndex())
	}
}

// After Restore, Ready() must not treat already-durable recovered state or
// entries as newly dirty — otherwise the driver would re-append them to the
// WAL, duplicating already-persisted data.
func TestRestoreDoesNotReReportRecoveredState(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewSource(seed))
	c := NewCore(1, []int{1, 2, 3}, 10, 20, rng, 3)

	recoveredLog := []Entry{
		{Cmd: nil, Term: 0},
		{Cmd: "a", Term: 1},
	}
	c.Restore(1, 0, recoveredLog, 0, 0, nil)

	ready := c.Ready()
	if ready.StateChanged {
		t.Fatalf("expected no state change reported right after Restore, got StateChanged=true")
	}
	if len(ready.EntriesToPersist) != 0 {
		t.Fatalf("expected no entries re-reported after Restore, got %+v", ready.EntriesToPersist)
	}

	c.log = append(c.log, Entry{Cmd: "b", Term: 1})
	ready = c.Ready()
	if len(ready.EntriesToPersist) != 1 || ready.EntriesToPersist[0].Cmd != "b" {
		t.Fatalf("expected only the newly appended entry to be reported, got %+v", ready.EntriesToPersist)
	}
}

// TestRestoreAppliesRecoveredSnapshotBoundary covers recovering a node whose
// storage already held a persisted snapshot: startIndex/startTerm/data must
// come back, and Ready() must not re-report the boundary as new.
func TestRestoreAppliesRecoveredSnapshotBoundary(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewSource(seed))
	c := NewCore(1, []int{1, 2, 3}, 10, 20, rng, 3)

	recoveredLog := []Entry{
		{Cmd: nil, Term: 3}, // boundary placeholder for logical index 5
		{Cmd: "c", Term: 3},
	}
	c.Restore(4, 0, recoveredLog, 5, 3, "state-as-of-5")

	if c.startIndex != 5 || c.startTerm != 3 {
		t.Fatalf("startIndex/startTerm = %d/%d, want 5/3", c.startIndex, c.startTerm)
	}
	if c.snapshotData != "state-as-of-5" {
		t.Fatalf("snapshotData = %v, want state-as-of-5", c.snapshotData)
	}
	if c.lastIndex() != 6 {
		t.Fatalf("lastIndex() = %d, want 6", c.lastIndex())
	}

	ready := c.Ready()
	if ready.SnapshotIndex != 0 {
		t.Fatalf("expected no snapshot re-reported after Restore, got SnapshotIndex=%d", ready.SnapshotIndex)
	}
	if len(ready.EntriesToPersist) != 0 {
		t.Fatalf("expected no entries re-reported after Restore, got %+v", ready.EntriesToPersist)
	}
}
