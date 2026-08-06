package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := newSegmentSet(dir, 1<<20)
	if err != nil {
		t.Fatalf("newSegmentSet: %v", err)
	}

	want := []string{"first", "second", "third"}
	for _, payload := range want {
		if err := s.append([]byte(payload)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := s.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	var got []string
	err = s.replay(func(payload []byte) error {
		got = append(got, string(payload))
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestSegmentRollsOverAtMaxBytes(t *testing.T) {
	dir := t.TempDir()
	// Small enough that each record forces a new segment.
	s, err := newSegmentSet(dir, 1)
	if err != nil {
		t.Fatalf("newSegmentSet: %v", err)
	}

	for _, payload := range []string{"a", "b", "c"} {
		if err := s.append([]byte(payload)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	names, err := s.list()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(names) != 3 {
		t.Fatalf("got %d segments, want 3: %v", len(names), names)
	}
	want := []string{"00000001.wal", "00000002.wal", "00000003.wal"}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("got %v, want %v", names, want)
		}
	}
}

func TestSegmentReopenPicksUpExistingFiles(t *testing.T) {
	dir := t.TempDir()
	s, err := newSegmentSet(dir, 1<<20)
	if err != nil {
		t.Fatalf("newSegmentSet: %v", err)
	}
	if err := s.append([]byte("persisted")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := newSegmentSet(dir, 1<<20)
	if err != nil {
		t.Fatalf("newSegmentSet on reopen: %v", err)
	}

	var got []string
	err = reopened.replay(func(payload []byte) error {
		got = append(got, string(payload))
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 1 || got[0] != "persisted" {
		t.Fatalf("got %v, want [persisted]", got)
	}

	// Appending after reopen should continue in the existing active
	// segment, not start a fresh 00000001.
	if err := reopened.append([]byte("more")); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	names, err := reopened.list()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(names) != 1 || names[0] != "00000001.wal" {
		t.Fatalf("got %v, want [00000001.wal]", names)
	}
}

func TestSegmentReplayStopsAtTornRecord(t *testing.T) {
	dir := t.TempDir()
	s, err := newSegmentSet(dir, 1<<20)
	if err != nil {
		t.Fatalf("newSegmentSet: %v", err)
	}
	if err := s.append([]byte("good")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a crash mid-write: append a truncated frame directly.
	path := filepath.Join(dir, "00000001.wal")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	frame := encodeRecord([]byte("torn"))
	if _, err := f.Write(frame[:len(frame)-2]); err != nil {
		t.Fatalf("write torn frame: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := newSegmentSet(dir, 1<<20)
	if err != nil {
		t.Fatalf("newSegmentSet: %v", err)
	}
	var got []string
	err = reopened.replay(func(payload []byte) error {
		got = append(got, string(payload))
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 1 || got[0] != "good" {
		t.Fatalf("got %v, want [good]", got)
	}
}
