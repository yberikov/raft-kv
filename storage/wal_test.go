package storage

import (
	"testing"
	"time"

	"raft-kv/raft"
)

func TestWALPersistStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	if err := w.PersistState(7, 3); err != nil {
		t.Fatalf("PersistState: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL on reopen: %v", err)
	}
	defer reopened.Close()

	term, votedFor := reopened.State()
	if term != 7 || votedFor != 3 {
		t.Fatalf("got term=%d votedFor=%d, want term=7 votedFor=3", term, votedFor)
	}
}

func TestWALAppendEntriesSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	entries := []raft.Entry{{Cmd: "a", Term: 1}, {Cmd: "b", Term: 1}}
	if err := w.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	// Not yet synced (n=100, m=1h) — nothing durable yet.
	if got := w.LastFsyncedIndex(); got != 0 {
		t.Fatalf("LastFsyncedIndex before sync: got %d, want 0", got)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL on reopen: %v", err)
	}
	defer reopened.Close()

	if got := reopened.LastIndex(); got != 2 {
		t.Fatalf("LastIndex: got %d, want 2", got)
	}
	got := reopened.Entries(1, 3)
	if len(got) != 2 || got[0].Cmd != "a" || got[1].Cmd != "b" {
		t.Fatalf("Entries: got %+v", got)
	}
	// Close() closes without an explicit sync, but the file was written and
	// the OS-buffered bytes made it back on reopen — durability after replay
	// tracks whatever was actually readable.
	if got := reopened.LastFsyncedIndex(); got != 2 {
		t.Fatalf("LastFsyncedIndex after replay: got %d, want 2", got)
	}
}

func TestWALInlineSyncAtNThreshold(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 2, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer w.Close()

	if err := w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if got := w.LastFsyncedIndex(); got != 0 {
		t.Fatalf("after 1st append: got %d, want 0", got)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "b", Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if got := w.LastFsyncedIndex(); got != 2 {
		t.Fatalf("after 2nd append (n=2 threshold): got %d, want 2", got)
	}
}

func TestWALBackgroundSyncAtMDeadline(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 100, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer w.Close()

	if err := w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if got := w.LastFsyncedIndex(); got != 0 {
		t.Fatalf("immediately after append: got %d, want 0", got)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if w.LastFsyncedIndex() == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("background sync never advanced LastFsyncedIndex within 1s")
}

func TestWALTruncateFromSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}, {Cmd: "b", Term: 1}, {Cmd: "c", Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := w.TruncateFrom(2); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "b2", Term: 2}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL on reopen: %v", err)
	}
	defer reopened.Close()

	if got := reopened.LastIndex(); got != 2 {
		t.Fatalf("LastIndex: got %d, want 2", got)
	}
	got := reopened.Entries(1, 3)
	if len(got) != 2 || got[0].Cmd != "a" || got[1].Cmd != "b2" {
		t.Fatalf("Entries after truncate+reappend replay: got %+v", got)
	}
}

func TestWALPoisonsAfterSyncError(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 1, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer w.Close()

	// Force a real sync failure by closing the underlying file out from
	// under the WAL, then trigger the inline sync path (n=1).
	if err := w.segments.active.Close(); err != nil {
		t.Fatalf("closing active file for the test: %v", err)
	}

	err = w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}})
	if err == nil {
		t.Fatalf("expected AppendEntries to surface the sync error, got nil")
	}

	// The WAL should now be poisoned: subsequent calls return the same error
	// instead of pretending to keep working.
	if err2 := w.PersistState(1, 1); err2 == nil {
		t.Fatalf("expected PersistState to return the poisoned error, got nil")
	}
}
