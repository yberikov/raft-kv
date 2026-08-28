package kv

import (
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

func TestApplyPut(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1")})

	if v, ok := st.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q,%v want 1,true", v, ok)
	}
	if _, ok := st.Get("missing"); ok {
		t.Fatalf("Get(missing) = ok, want not found")
	}
}

func TestApplyDelete(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1")})
	st.Apply([]raft.Entry{del("a")})

	if _, ok := st.Get("a"); ok {
		t.Fatalf("Get(a) after delete = ok, want not found")
	}
}

// A delete of a key that was never set is a no-op, not an error -- and,
// critically, must not stop the rest of the batch from being applied.
func TestApplyDeleteOfMissingKeyIsNoOpAndDoesNotStopTheBatch(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{del("never-set"), put("a", "1"), put("b", "2")})

	if v, ok := st.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q,%v want 1,true -- entries after a no-op delete must still apply", v, ok)
	}
	if v, ok := st.Get("b"); !ok || v != "2" {
		t.Fatalf("Get(b) = %q,%v want 2,true -- entries after a no-op delete must still apply", v, ok)
	}
}

func TestApplyCasSucceedsWhenCurrentValueMatchesExpected(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1"), cas("a", "1", "2")})

	if v, ok := st.Get("a"); !ok || v != "2" {
		t.Fatalf("Get(a) = %q,%v want 2,true", v, ok)
	}
}

func TestApplyCasFailsWhenCurrentValueDoesNotMatchExpected(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1"), cas("a", "wrong", "2")})

	if v, ok := st.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q,%v want 1,true -- CAS with a stale Expected must not mutate the value", v, ok)
	}
}

func TestApplyCasFailsOnMissingKey(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{cas("never-set", "", "2")})

	if _, ok := st.Get("never-set"); ok {
		t.Fatalf("Get(never-set) after CAS on a missing key = ok, want not found")
	}
}

// TestApplyDedupSuppressesRetryOfClientsFirstCommand is a regression test: the
// dedup table's lastSeq entry must be recorded even the very first time a
// client is seen, or a retried first request slips past dedup and re-applies.
func TestApplyDedupSuppressesRetryOfClientsFirstCommand(t *testing.T) {
	st := NewStateMachine()
	first := raft.Entry{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "first", ClientID: "c1", Seq: 1}}
	retry := raft.Entry{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "second", ClientID: "c1", Seq: 1}}

	st.Apply([]raft.Entry{first})
	st.Apply([]raft.Entry{retry})

	if v, _ := st.Get("k"); v != "first" {
		t.Fatalf("Get(k) = %q, want %q -- retry of a client's first command must be suppressed", v, "first")
	}
}

func TestApplyDedupAllowsNewSeqFromSameClient(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{
		{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "v1", ClientID: "c1", Seq: 1}},
		{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "v2", ClientID: "c1", Seq: 2}},
	})

	if v, _ := st.Get("k"); v != "v2" {
		t.Fatalf("Get(k) = %q, want v2 -- a new Seq from the same client must apply", v)
	}
}

func TestApplyDedupIsPerClient(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{
		{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "from-c1", ClientID: "c1", Seq: 1}},
		{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "from-c2", ClientID: "c2", Seq: 1}},
	})

	if v, _ := st.Get("k"); v != "from-c2" {
		t.Fatalf("Get(k) = %q, want from-c2 -- Seq 1 from a different ClientID must not be treated as a duplicate", v)
	}
}

// TestSnapshotRestoreRoundTrip is a regression test for gob silently dropping
// unexported fields: Snapshot must produce bytes that Restore can actually
// decode back into a usable state machine, including one that accepts
// further writes afterward (a nil map from a failed decode would panic on
// the first Put).
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{put("a", "1"), put("b", "2")})

	snap := st.Snapshot()

	restored := NewStateMachine()
	restored.Restore(snap)

	if v, ok := restored.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) after restore = %q,%v want 1,true", v, ok)
	}
	if v, ok := restored.Get("b"); !ok || v != "2" {
		t.Fatalf("Get(b) after restore = %q,%v want 2,true", v, ok)
	}

	restored.Apply([]raft.Entry{put("c", "3")})
	if v, ok := restored.Get("c"); !ok || v != "3" {
		t.Fatalf("Get(c) after post-restore Put = %q,%v want 3,true", v, ok)
	}
}

// TestSnapshotRestorePreservesDedupState covers the other half of the
// snapshot payload: if lastSeq isn't carried across a snapshot/restore, a
// retried request that was already applied before the snapshot would
// re-apply after recovery.
func TestSnapshotRestorePreservesDedupState(t *testing.T) {
	st := NewStateMachine()
	st.Apply([]raft.Entry{{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "first", ClientID: "c1", Seq: 1}}})

	restored := NewStateMachine()
	restored.Restore(st.Snapshot())

	retry := raft.Entry{Cmd: raft.Command{Op: raft.OpPut, Key: "k", Value: "second", ClientID: "c1", Seq: 1}}
	restored.Apply([]raft.Entry{retry})

	if v, _ := restored.Get("k"); v != "first" {
		t.Fatalf("Get(k) = %q, want %q -- dedup state must survive a snapshot/restore", v, "first")
	}
}
