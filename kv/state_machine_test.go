package kv

import (
	"fmt"
	"testing"

	"raft-kv/raft"
)

func put(key, value string) raft.Entry {
	return raft.Entry{Cmd: raft.Command{Op: raft.OpPut, Key: key, Value: value}}
}

func del(key string) raft.Entry {
	return raft.Entry{Cmd: raft.Command{Op: raft.OpDel, Key: key}}
}

func cas(key, expected, value string) raft.Entry {
	return raft.Entry{Cmd: raft.Command{Op: raft.OpCas, Key: key, Expected: expected, Value: value}}
}

func get(key string) raft.Entry {
	return raft.Entry{Cmd: raft.Command{Op: raft.OpGet, Key: key}}
}

// apply1 applies a single entry and returns its result. Every read in these
// tests goes through an OpGet entry rather than a back-door accessor, so the
// assertions exercise the same path a real client's read takes -- and so a
// result that never makes it into Apply's return value cannot pass unnoticed.
func apply1(t *testing.T, st *StateMachine, e raft.Entry) Result {
	t.Helper()
	out := st.Apply([]raft.Entry{e})
	if len(out) != 1 {
		t.Fatalf("Apply returned %d results for 1 entry", len(out))
	}
	return out[0]
}

func mustRead(t *testing.T, st *StateMachine, key, want string) {
	t.Helper()
	r := apply1(t, st, get(key))
	if !r.Found || r.Value != want {
		t.Fatalf("get(%q) = %+v, want Value=%q Found=true", key, r, want)
	}
}

func mustMiss(t *testing.T, st *StateMachine, key string) {
	t.Helper()
	r := apply1(t, st, get(key))
	if r.Found || r.Value != "" {
		t.Fatalf("get(%q) = %+v, want Found=false and an empty Value", key, r)
	}
}

func TestApplyReturnsOneResultPerEntryInOrder(t *testing.T) {
	st := NewStateMachine()
	out := st.Apply([]raft.Entry{
		put("k", "v"),
		get("k"),
		cas("k", "v", "w"),
		get("k"),
		del("k"),
		get("k"),
	})

	if len(out) != 6 {
		t.Fatalf("Apply returned %d results for 6 entries", len(out))
	}
	if out[1].Value != "v" || !out[1].Found {
		t.Fatalf("out[1] (get after put) = %+v, want Value=v Found=true", out[1])
	}
	if !out[2].Ok {
		t.Fatalf("out[2] (successful cas) = %+v, want Ok=true", out[2])
	}
	if out[3].Value != "w" || !out[3].Found {
		t.Fatalf("out[3] (get after cas) = %+v, want Value=w Found=true", out[3])
	}
	if !out[4].Found {
		t.Fatalf("out[4] (delete of a present key) = %+v, want Found=true", out[4])
	}
	if out[5].Found {
		t.Fatalf("out[5] (get after delete) = %+v, want Found=false", out[5])
	}
}

func TestApplyPut(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1")})

	mustRead(t, st, "a", "1")
	mustMiss(t, st, "missing")
}

func TestApplyPutOverwrites(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1"), put("a", "2")})

	mustRead(t, st, "a", "2")
}

func TestApplyDelete(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1")})

	if r := apply1(t, st, del("a")); !r.Found {
		t.Fatalf("delete of a present key = %+v, want Found=true", r)
	}
	mustMiss(t, st, "a")
}

// TestApplyDeleteOfMissingKeyIsNoOpAndDoesNotStopTheBatch is a regression
// test: a delete of a key that was never set is a no-op, not an error, and
// must not stop the rest of the batch from being applied.
func TestApplyDeleteOfMissingKeyIsNoOpAndDoesNotStopTheBatch(t *testing.T) {
	st := NewStateMachine()
	out := st.Apply([]raft.Entry{del("never-set"), put("a", "1"), put("b", "2")})

	if out[0].Found {
		t.Fatalf("delete of an absent key = %+v, want Found=false", out[0])
	}
	mustRead(t, st, "a", "1")
	mustRead(t, st, "b", "2")
}

func TestApplyCasSucceedsWhenCurrentValueMatchesExpected(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1")})

	if r := apply1(t, st, cas("a", "1", "2")); !r.Ok {
		t.Fatalf("cas with a matching Expected = %+v, want Ok=true", r)
	}
	mustRead(t, st, "a", "2")
}

func TestApplyCasFailsWhenCurrentValueDoesNotMatchExpected(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1")})

	if r := apply1(t, st, cas("a", "wrong", "2")); r.Ok {
		t.Fatalf("cas with a stale Expected = %+v, want Ok=false", r)
	}
	mustRead(t, st, "a", "1")
}

func TestApplyCasFailsOnMissingKey(t *testing.T) {
	st := NewStateMachine()

	if r := apply1(t, st, cas("never-set", "", "2")); r.Ok {
		t.Fatalf("cas on a missing key = %+v, want Ok=false", r)
	}
	mustMiss(t, st, "never-set")
}

// TestApplyDedupSuppressesRetryOfClientsFirstCommand is a regression test: the
// session entry must be recorded even the very first time a client is seen, or
// a retried first request slips past dedup and re-applies.
func TestApplyDedupSuppressesRetryOfClientsFirstCommand(t *testing.T) {
	st := NewStateMachine()
	first := raft.Entry{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "first", ClientID: "c1", Seq: 1}}
	retry := raft.Entry{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "second", ClientID: "c1", Seq: 1}}

	st.Apply([]raft.Entry{first})
	st.Apply([]raft.Entry{retry})

	mustRead(t, st, "k", "first")
}

// TestApplyDedupReturnsTheCachedResultToARetry is the point of caching the
// response alongside the sequence number: the client that retried never saw
// the first response, so the retry must be answered with it rather than
// silently swallowed.
func TestApplyDedupReturnsTheCachedResultToARetry(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("k", "v")})

	read := raft.Entry{Cmd: raft.Command{Op: raft.OpGet, Key: "k", ClientID: "c1", Seq: 1}}

	original := apply1(t, st, read)
	if original.Value != "v" || !original.Found {
		t.Fatalf("original read = %+v, want Value=v Found=true", original)
	}

	// Same ClientID and Seq: an at-least-once resend of the same request.
	if retried := apply1(t, st, read); retried != original {
		t.Fatalf("retry returned %+v, want the cached %+v -- a suppressed duplicate still owes the client its answer", retried, original)
	}
}

// TestApplyDedupCachesCasOutcome covers the case where the cached answer is
// the only record of what happened: a CAS that lost the race must keep
// reporting Ok=false to its retries, even though re-running it now would be
// evaluated against different state.
func TestApplyDedupCachesCasOutcome(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("k", "v")})

	swap := raft.Entry{Cmd: raft.Command{Op: raft.OpCas, Key: "k", Expected: "wrong", Value: "w", ClientID: "c1", Seq: 1}}
	if r := apply1(t, st, swap); r.Ok {
		t.Fatalf("cas with a stale Expected = %+v, want Ok=false", r)
	}

	// Make the same CAS a winner if it were re-evaluated now.
	st.Apply([]raft.Entry{put("k", "wrong")})

	if r := apply1(t, st, swap); r.Ok {
		t.Fatalf("retry of a failed cas = %+v, want the cached Ok=false -- a duplicate must be answered from the cache, not re-evaluated", r)
	}
	mustRead(t, st, "k", "wrong")
}

func TestApplyDedupAllowsNewSeqFromSameClient(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{
		{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "v1", ClientID: "c1", Seq: 1}},
		{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "v2", ClientID: "c1", Seq: 2}},
	})

	mustRead(t, st, "k", "v2")
}

func TestApplyDedupIsPerClient(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{
		{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "from-c1", ClientID: "c1", Seq: 1}},
		{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "from-c2", ClientID: "c2", Seq: 1}},
	})

	mustRead(t, st, "k", "from-c2")
}

func TestApplyWithoutClientIDBypassesDedup(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("k", "v1"), put("k", "v2")})

	// Sessionless commands carry no Seq, so identical ones must not be
	// mistaken for duplicates of each other.
	mustRead(t, st, "k", "v2")
	if len(st.sessions) != 0 {
		t.Fatalf("sessions = %+v, want empty -- a command with no ClientID must not create a session", st.sessions)
	}
}

// TestApplyStaleSeqIsNeitherReappliedNorMisanswered covers the duplicate the
// session cache can no longer answer: a delayed proposal from before the
// client's current request, committed late. It must not re-apply (the command
// already took effect once), and it must not be handed the newer request's
// cached result. Distinguishing this from a genuine miss needs a dedicated
// error field -- a zero Result reads as "key not found" for a Get.
func TestApplyStaleSeqIsNeitherReappliedNorMisanswered(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "one", ClientID: "c1", Seq: 1}}})
	st.Apply([]raft.Entry{{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "two", ClientID: "c1", Seq: 2}}})

	stale := raft.Entry{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "one", ClientID: "c1", Seq: 1}}
	r := apply1(t, st, stale)

	mustRead(t, st, "k", "two")
	if got := st.sessions["c1"].Seq; got != 2 {
		t.Fatalf("session Seq = %d, want 2 -- a late duplicate must not move the session backwards", got)
	}
	if r.Value != "" || r.Found || r.Ok {
		t.Fatalf("stale duplicate returned %+v, want no value/Found/Ok -- the cache holds a different request's answer", r)
	}
}

// TestSnapshotRestoreRoundTrip is a regression test for gob silently dropping
// unexported fields: Snapshot must produce bytes Restore can decode back into
// a usable state machine, including one that accepts further writes (a nil map
// from a failed decode would panic on the first Put).
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1"), put("b", "2")})

	restored := NewStateMachine()
	restored.Restore(st.Snapshot())

	mustRead(t, restored, "a", "1")
	mustRead(t, restored, "b", "2")

	restored.Apply([]raft.Entry{put("c", "3")})
	mustRead(t, restored, "c", "3")
}

// TestSnapshotRestorePreservesCachedResults covers the other half of the
// payload. It is what makes the snapshot/waiter interaction work: installing a
// snapshot wipes any in-flight waiter, the client retries, and the retry is
// answerable only because the cached response travelled inside the snapshot.
func TestSnapshotRestorePreservesCachedResults(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("k", "first")})

	read := raft.Entry{Cmd: raft.Command{Op: raft.OpGet, Key: "k", ClientID: "c1", Seq: 1}}
	original := apply1(t, st, read)

	restored := NewStateMachine()
	restored.Restore(st.Snapshot())

	// Change the value so a re-executed read would answer differently.
	restored.Apply([]raft.Entry{put("k", "second")})

	if retried := apply1(t, restored, read); retried != original {
		t.Fatalf("retry after restore returned %+v, want the cached %+v -- cached responses must survive a snapshot", retried, original)
	}
}

// TestSnapshotRoundTripsManyKeys covers what Cluster.Restart depends on: a
// snapshot must restore the complete applied state -- every key and the whole
// client session table -- into a state machine that starts empty.
//
// Note what is deliberately NOT asserted here: byte equality between two
// snapshots of the same state. gob encodes maps in Go's randomized map
// iteration order, so a state machine's own snapshot differs from itself run
// to run once it holds more than one key. Snapshots are interchangeable in
// meaning, not in bytes, and anything comparing applied state across nodes has
// to compare behaviour rather than encodings.
func TestSnapshotRoundTripsManyKeys(t *testing.T) {
	src := NewStateMachine()
	var entries []raft.Entry
	for i, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		entries = append(entries, raft.Entry{Cmd: raft.Command{
			Op: raft.OpPut, Key: k, Value: k + "-v",
			ClientID: fmt.Sprintf("client-%d", i%3), Seq: uint64(i + 1),
		}})
	}
	src.Apply(entries)

	dst := NewStateMachine()
	dst.Restore(src.Snapshot())

	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		mustRead(t, dst, k, k+"-v")
	}

	// The session table has to survive too, or the restored node re-executes
	// requests it has already answered. Replaying an entry the source already
	// applied must be suppressed, not run again.
	replay := entries[len(entries)-1]
	if res := apply1(t, dst, replay); res != (Result{}) {
		t.Fatalf("replayed request returned %+v, want the cached empty Put result", res)
	}
	stale := raft.Entry{Cmd: raft.Command{
		Op: raft.OpPut, Key: "a", Value: "clobbered",
		ClientID: replay.Cmd.ClientID, Seq: replay.Cmd.Seq - 3,
	}}
	if res := apply1(t, dst, stale); res.Err == "" {
		t.Fatalf("a stale retry returned %+v, want a rejection -- the restored session table did not carry the client's sequence number", res)
	}
	mustRead(t, dst, "a", "a-v")
}
