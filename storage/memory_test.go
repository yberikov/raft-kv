package storage

import (
	"testing"

	"raft-kv/raft"
)

// TestInMemoryStoragePersistSnapshotBeyondCurrentLog covers the case where an
// InstallSnapshot-driven snapshot lands past everything the node currently
// holds (e.g. a crash-recovered node too far behind to have any of it) --
// PersistSnapshot must wholesale-replace the log with just the boundary
// placeholder rather than slicing past the end of it.
func TestInMemoryStoragePersistSnapshotBeyondCurrentLog(t *testing.T) {
	s := NewInMemoryStorage(3)
	if err := s.AppendEntries([]raft.Entry{{Cmd: raft.Command{Key: "a"}, Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	// Log only reaches index 1; snapshot at index 5 is entirely beyond it.
	if err := s.PersistSnapshot(5, 3, "state-as-of-5"); err != nil {
		t.Fatalf("PersistSnapshot: %v", err)
	}

	if got := s.LastIndex(); got != 5 {
		t.Fatalf("LastIndex = %d, want 5", got)
	}
	if got := s.TermAt(5); got != 3 {
		t.Fatalf("TermAt(5) = %d, want 3", got)
	}
	index, term, data := s.SnapshotState()
	if index != 5 || term != 3 || data != "state-as-of-5" {
		t.Fatalf("SnapshotState = %d/%d/%v, want 5/3/state-as-of-5", index, term, data)
	}
}
