package storage

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"raft-kv/raft"
)

func testSeed(t *testing.T) int64 {
	t.Helper()
	if s := os.Getenv("RAFT_TEST_SEED"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			t.Logf("seed=%d (from RAFT_TEST_SEED)", v)
			return v
		}
	}
	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	return seed
}

// walSnapshot is everything an outside caller can observe about a WAL's
// recovered state -- used to compare a crash-recovered WAL against the live
// WAL's state at the moment a candidate crash point was recorded.
type walSnapshot struct {
	term       uint64
	votedFor   int
	startIndex int
	snapTerm   uint64
	snapData   any
	lastIndex  int
	entries    []raft.Entry
}

func captureWAL(w *WAL) walSnapshot {
	term, votedFor := w.State()
	startIndex, snapTerm, snapData := w.SnapshotState()
	lastIndex := w.LastIndex()
	return walSnapshot{
		term:       term,
		votedFor:   votedFor,
		startIndex: startIndex,
		snapTerm:   snapTerm,
		snapData:   snapData,
		lastIndex:  lastIndex,
		entries:    w.Entries(startIndex, lastIndex+1),
	}
}

// dirSnapshot is the full byte content of every segment file that currently
// exists, keyed by segment ID. Unlike a single file's size, this survives
// ReclaimSegments deleting old segments out from under a later checkpoint --
// each checkpoint keeps its own copy of exactly the bytes that existed at
// that moment, regardless of what happens to those files afterward.
func dirSnapshot(t *testing.T, dir string) map[int][]byte {
	t.Helper()
	ids, err := listSegmentIDs(dir)
	if err != nil {
		t.Fatalf("listSegmentIDs: %v", err)
	}
	out := make(map[int][]byte, len(ids))
	for _, id := range ids {
		b, err := os.ReadFile(filepath.Join(dir, segmentName(id)))
		if err != nil {
			t.Fatalf("reading segment %d: %v", id, err)
		}
		out[id] = b
	}
	return out
}

func writeDirSnapshot(t *testing.T, dir string, segments map[int][]byte) {
	t.Helper()
	for id, b := range segments {
		if err := os.WriteFile(filepath.Join(dir, segmentName(id)), b, 0o644); err != nil {
			t.Fatalf("writing segment %d: %v", id, err)
		}
	}
}

// TestFuzzWALCrashRecovery drives a real on-disk WAL through a randomized
// sequence of operations, recording the full directory contents (and the
// live WAL's observable state) after each one. It then simulates crashing at
// many different points -- both exactly at a completed op and mid-way
// through the next one's active-file growth (a torn write), when that next
// op didn't roll or delete anything -- by replaying a copy of the exact
// historical bytes into a fresh directory and reopening a WAL on it. Every
// crash point must recover to exactly the state of the last op that fully
// completed; a torn tail must be silently dropped, never panic, and never
// corrupt anything before it. This also covers ReclaimSegments deleting
// segments mid-run: a checkpoint taken before a later deletion must still be
// recoverable from its own preserved copy of those since-deleted bytes.
func TestFuzzWALCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping WAL crash fuzz in short mode")
	}

	base := testSeed(t)
	const iterations = 50

	for iter := 0; iter < iterations; iter++ {
		seed := base + int64(iter)
		rng := rand.New(rand.NewSource(seed))

		dir := t.TempDir()
		w, err := NewWAL(dir, 100, time.Hour)
		if err != nil {
			t.Fatalf("seed=%d: NewWAL: %v", seed, err)
		}

		type checkpoint struct {
			activeID int
			segments map[int][]byte
			state    walSnapshot
			// multiRecord is true when the op that produced this checkpoint
			// could have written more than one record before its own sync
			// (PersistSnapshot, when anything survives the boundary, writes
			// the snapshot record plus an explicit survivors record). A torn
			// write landing between those two records produces a state that
			// matches neither the previous nor this checkpoint, so such
			// checkpoints are excluded from torn-write simulation below.
			multiRecord bool
		}
		snapshot := func(multiRecord bool) checkpoint {
			return checkpoint{activeID: w.segments.activeID, segments: dirSnapshot(t, dir), state: captureWAL(w), multiRecord: multiRecord}
		}
		checkpoints := []checkpoint{snapshot(false)}

		nextTerm := uint64(1)
		const numOps = 30
		for i := 0; i < numOps; i++ {
			multiRecord := false
			switch rng.Intn(5) {
			case 0: // AppendEntries
				n := 1 + rng.Intn(3)
				entries := make([]raft.Entry, n)
				for j := range entries {
					entries[j] = raft.Entry{Cmd: raft.Command{Key: strconv.Itoa(rng.Intn(1000))}, Term: nextTerm}
				}
				if err := w.AppendEntries(entries); err != nil {
					t.Fatalf("seed=%d op=%d: AppendEntries: %v", seed, i, err)
				}
			case 1: // PersistState
				if rng.Intn(2) == 0 {
					nextTerm++
				}
				if err := w.PersistState(nextTerm, rng.Intn(4)); err != nil {
					t.Fatalf("seed=%d op=%d: PersistState: %v", seed, i, err)
				}
			case 2: // TruncateFrom
				startIndex, _, _ := w.SnapshotState()
				last := w.LastIndex()
				if last <= startIndex {
					continue
				}
				idx := startIndex + 1 + rng.Intn(last-startIndex)
				if err := w.TruncateFrom(idx); err != nil {
					t.Fatalf("seed=%d op=%d: TruncateFrom: %v", seed, i, err)
				}
			case 3: // PersistSnapshot -- occasionally beyond the current log
				// entirely, exercising the far-behind-follower / wholesale
				// replace path.
				startIndex, _, _ := w.SnapshotState()
				last := w.LastIndex()
				idx := startIndex + 1 + rng.Intn(last-startIndex+3)
				if err := w.PersistSnapshot(idx, nextTerm, "snap-"+strconv.Itoa(idx)); err != nil {
					t.Fatalf("seed=%d op=%d: PersistSnapshot: %v", seed, i, err)
				}
				// PersistSnapshot writes a second, explicit survivors record
				// exactly when something survives past the boundary.
				multiRecord = w.LastIndex() > idx
			case 4: // ReclaimSegments
				if err := w.ReclaimSegments(); err != nil {
					t.Fatalf("seed=%d op=%d: ReclaimSegments: %v", seed, i, err)
				}
			}
			checkpoints = append(checkpoints, snapshot(multiRecord))
		}

		if err := w.Close(); err != nil {
			t.Fatalf("seed=%d: Close: %v", seed, err)
		}

		const trials = 8
		for trial := 0; trial < trials; trial++ {
			cpIdx := rng.Intn(len(checkpoints))
			cp := checkpoints[cpIdx]
			crashSegments := cp.segments

			// Only simulate a torn write when the next op purely grew the
			// same active file (no roll, no deletion) and wrote exactly one
			// record -- the one case where "mid-way through this file's
			// growth" has an unambiguous meaning. Ops that roll or delete
			// (like ReclaimSegments) or that can write a second record
			// (PersistSnapshot with survivors) are only exercised at clean
			// checkpoint boundaries here, since a torn cut between two
			// records in the same op matches neither checkpoint.
			if cpIdx+1 < len(checkpoints) {
				next := checkpoints[cpIdx+1]
				sameFileGrewOnly := next.activeID == cp.activeID && len(next.segments) == len(cp.segments) && !next.multiRecord
				if sameFileGrewOnly && rng.Intn(2) == 0 {
					before := cp.segments[cp.activeID]
					after := next.segments[next.activeID]
					gap := int64(len(after) - len(before))
					if gap > 1 {
						cut := int64(len(before)) + 1 + rng.Int63n(gap-1)
						torn := make(map[int][]byte, len(cp.segments))
						for id, b := range cp.segments {
							torn[id] = b
						}
						torn[cp.activeID] = after[:cut]
						crashSegments = torn
					}
				}
			}

			crashDir := t.TempDir()
			writeDirSnapshot(t, crashDir, crashSegments)

			recovered, err := NewWAL(crashDir, 100, time.Hour)
			if err != nil {
				t.Fatalf("seed=%d trial=%d: NewWAL on crashed dir (checkpoint %d): %v", seed, trial, cpIdx, err)
			}
			got := captureWAL(recovered)
			recovered.Close()

			if !reflect.DeepEqual(got, cp.state) {
				t.Fatalf("seed=%d trial=%d: crash at checkpoint %d recovered to\n  %+v\nwant\n  %+v",
					seed, trial, cpIdx, got, cp.state)
			}
		}
	}
}
