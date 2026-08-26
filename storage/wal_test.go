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

func TestWALPersistSnapshotSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}, {Cmd: "b", Term: 1}, {Cmd: "c", Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	// Not yet synced (n=100, m=1h) — PersistSnapshot must still fsync
	// immediately and cover everything through index 3, regardless.
	// Index 3 ("c") becomes the boundary placeholder; only "d" (appended
	// below, after the snapshot) survives as a real entry.
	if err := w.PersistSnapshot(3, 1, "state-as-of-3"); err != nil {
		t.Fatalf("PersistSnapshot: %v", err)
	}
	if got := w.LastFsyncedIndex(); got != 3 {
		t.Fatalf("LastFsyncedIndex right after PersistSnapshot: got %d, want 3 (immediate sync)", got)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "d", Term: 1}}); err != nil {
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

	index, term, data := reopened.SnapshotState()
	if index != 3 || term != 1 || data != "state-as-of-3" {
		t.Fatalf("SnapshotState = %d/%d/%v, want 3/1/state-as-of-3", index, term, data)
	}
	if got := reopened.LastIndex(); got != 4 {
		t.Fatalf("LastIndex: got %d, want 4", got)
	}
	got := reopened.Entries(3, 5)
	if len(got) != 2 || got[0].Cmd != nil || got[1].Cmd != "d" {
		t.Fatalf("Entries after snapshot replay: got %+v, want [boundary@3, d@4]", got)
	}
	if gotTerm := reopened.TermAt(3); gotTerm != 1 {
		t.Fatalf("TermAt(3) = %d, want 1 (boundary term)", gotTerm)
	}
}

// TestWALPersistSnapshotBeyondCurrentLog covers a snapshot boundary landing
// entirely past the log we currently hold -- e.g. a node that fell far
// enough behind (or crash-recovered down to almost nothing) that an
// InstallSnapshot's index exceeds its own LastIndex(). PersistSnapshot must
// wholesale-replace the log with just the boundary placeholder rather than
// slicing past the end of it, and that must survive replay on reopen too.
func TestWALPersistSnapshotBeyondCurrentLog(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	// Snapshot at index 5 is entirely beyond the 1-entry log we hold.
	if err := w.PersistSnapshot(5, 3, "state-as-of-5"); err != nil {
		t.Fatalf("PersistSnapshot: %v", err)
	}
	if got := w.LastIndex(); got != 5 {
		t.Fatalf("LastIndex right after PersistSnapshot: got %d, want 5", got)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewWAL(dir, 100, time.Hour)
	if err != nil {
		t.Fatalf("NewWAL on reopen: %v", err)
	}
	defer reopened.Close()

	index, term, data := reopened.SnapshotState()
	if index != 5 || term != 3 || data != "state-as-of-5" {
		t.Fatalf("SnapshotState = %d/%d/%v, want 5/3/state-as-of-5", index, term, data)
	}
	if got := reopened.LastIndex(); got != 5 {
		t.Fatalf("LastIndex after replay: got %d, want 5", got)
	}
	if gotTerm := reopened.TermAt(5); gotTerm != 3 {
		t.Fatalf("TermAt(5) after replay: got %d, want 3", gotTerm)
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

// TestWALReclaimSegmentsDeletesFilesBeforeSnapshot forces one segment file
// per record (maxBytes=1) so a real multi-file directory builds up, then
// checks that ReclaimSegments deletes exactly the segments made obsolete by
// the snapshot boundary, and that a fresh WAL on the shrunk directory still
// recovers full, correct state.
func TestWALReclaimSegmentsDeletesFilesBeforeSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 100, time.Hour, 1)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}

	if err := w.PersistState(1, 0); err != nil { // segment 1
		t.Fatalf("PersistState: %v", err)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}}); err != nil { // segment 2
		t.Fatalf("AppendEntries a: %v", err)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "b", Term: 1}}); err != nil { // segment 3
		t.Fatalf("AppendEntries b: %v", err)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "c", Term: 1}}); err != nil { // segment 4
		t.Fatalf("AppendEntries c: %v", err)
	}
	if err := w.PersistSnapshot(2, 1, "snap-at-2"); err != nil { // segments 5 (boundary) + 6 (survivor "c")
		t.Fatalf("PersistSnapshot: %v", err)
	}
	if err := w.AppendEntries([]raft.Entry{{Cmd: "d", Term: 1}}); err != nil { // segment 7
		t.Fatalf("AppendEntries d: %v", err)
	}

	idsBefore, err := listSegmentIDs(dir)
	if err != nil {
		t.Fatalf("listSegmentIDs before reclaim: %v", err)
	}
	if len(idsBefore) != 7 {
		t.Fatalf("expected 7 segments before reclaim, got %v", idsBefore)
	}

	if err := w.ReclaimSegments(); err != nil {
		t.Fatalf("ReclaimSegments: %v", err)
	}

	// PersistSnapshot already wrote segment 5 (boundary) + 6 (survivor "c")
	// as a self-sufficient pair, so ReclaimSegments only needs to re-assert
	// state (landing wherever is currently active, segment 8) and delete
	// everything strictly before the snapshot's own segment (1-4).
	idsAfter, err := listSegmentIDs(dir)
	if err != nil {
		t.Fatalf("listSegmentIDs after reclaim: %v", err)
	}
	if want := []int{5, 6, 7, 8}; !equalInts(idsAfter, want) {
		t.Fatalf("segments after reclaim = %v, want %v", idsAfter, want)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := newWAL(dir, 100, time.Hour, 1)
	if err != nil {
		t.Fatalf("newWAL on reopen: %v", err)
	}
	defer reopened.Close()

	if term, votedFor := reopened.State(); term != 1 || votedFor != 0 {
		t.Fatalf("State after reclaim+reopen = %d/%d, want 1/0", term, votedFor)
	}
	if index, snapTerm, data := reopened.SnapshotState(); index != 2 || snapTerm != 1 || data != "snap-at-2" {
		t.Fatalf("SnapshotState after reclaim+reopen = %d/%d/%v, want 2/1/snap-at-2", index, snapTerm, data)
	}
	if got := reopened.LastIndex(); got != 4 {
		t.Fatalf("LastIndex after reclaim+reopen: got %d, want 4", got)
	}
	got := reopened.Entries(2, 5)
	if len(got) != 3 || got[0].Cmd != nil || got[1].Cmd != "c" || got[2].Cmd != "d" {
		t.Fatalf("Entries after reclaim+reopen: got %+v, want [boundary@2, c@3, d@4]", got)
	}
}

func TestWALReclaimSegmentsNoopBeforeAnySnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 100, time.Hour, 1)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}
	defer w.Close()

	if err := w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	before, err := listSegmentIDs(dir)
	if err != nil {
		t.Fatalf("listSegmentIDs: %v", err)
	}
	if err := w.ReclaimSegments(); err != nil {
		t.Fatalf("ReclaimSegments before any snapshot should be a no-op, got error: %v", err)
	}
	after, err := listSegmentIDs(dir)
	if err != nil {
		t.Fatalf("listSegmentIDs: %v", err)
	}
	if !equalInts(before, after) {
		t.Fatalf("ReclaimSegments before any snapshot changed segments: before=%v after=%v", before, after)
	}
}

// TestWALReclaimSegmentsIsIdempotent covers calling ReclaimSegments twice in
// a row with no new snapshot in between: the second call has nothing left to
// delete (the first call already removed everything below the floor) and
// must not error just because those files are already gone.
func TestWALReclaimSegmentsIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := newWAL(dir, 100, time.Hour, 1)
	if err != nil {
		t.Fatalf("newWAL: %v", err)
	}
	defer w.Close()

	if err := w.AppendEntries([]raft.Entry{{Cmd: "a", Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := w.PersistSnapshot(1, 1, "snap-at-1"); err != nil {
		t.Fatalf("PersistSnapshot: %v", err)
	}

	if err := w.ReclaimSegments(); err != nil {
		t.Fatalf("first ReclaimSegments: %v", err)
	}
	if err := w.ReclaimSegments(); err != nil {
		t.Fatalf("second ReclaimSegments (idempotent call) should not error: %v", err)
	}

	index, term, data := w.SnapshotState()
	if index != 1 || term != 1 || data != "snap-at-1" {
		t.Fatalf("SnapshotState after idempotent reclaim = %d/%d/%v, want 1/1/snap-at-1", index, term, data)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
