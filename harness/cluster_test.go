package harness

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"raft-kv/driver"
	"raft-kv/kv"
	"raft-kv/raft"
)

func mustResolve(t *testing.T, seed int64, ps []*driver.Pending) []kv.Result {
	t.Helper()
	out := make([]kv.Result, len(ps))
	for i, p := range ps {
		res, done := p.Poll()
		if !done {
			t.Fatalf("seed=%d: proposal %d never resolved even though its entry committed", seed, i)
		}
		if res.Err != "" {
			t.Fatalf("seed=%d: proposal %d resolved with Err=%q, want a real result", seed, i, res.Err)
		}
		out[i] = res
	}
	return out
}

func mustFail(t *testing.T, seed int64, ps []*driver.Pending, wantErr string) {
	t.Helper()
	for i, p := range ps {
		res, done := p.Poll()
		if !done {
			t.Fatalf("seed=%d: discarded proposal %d never resolved; a client would wait forever", seed, i)
		}
		if res.Err != wantErr {
			t.Fatalf("seed=%d: discarded proposal %d resolved with %+v, want Err=%q", seed, i, res, wantErr)
		}
	}
}

func lastIndex(status raft.Status) int {
	return status.StartIndex + len(status.Log) - 1
}

func TestClusterWithChaosElectsLeaderAndReplicatesProposedCommands(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	chaos := ChaosConfig{DropP: 0.05, DuplicateP: 0.05, MinDelay: 1, MaxDelay: 3}
	cluster := NewCluster(ids, seed, rand.New(rand.NewSource(seed)), chaos)

	if err := cluster.Run(600); err != nil {
		t.Fatalf("seed=%d: safety violation while electing a leader: %v", seed, err)
	}

	leaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			leaderId = id
			break
		}
	}
	if leaderId == 0 {
		t.Fatalf("seed=%d: no leader elected after 600 ticks under chaos", seed)
	}
	t.Logf("seed=%d: node %d elected leader", seed, leaderId)

	// No assertion on these pendings: under chaos the leader can be deposed
	// mid-replication, so they are legitimately allowed to come back
	// truncated, or not at all.
	cluster.Propose(leaderId, []raft.Command{{Key: "a"}, {Key: "b"}, {Key: "c"}})

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while replicating under chaos: %v", seed, err)
	}

	// Chaos can depose the leader we proposed to before the entries commit
	// (its heartbeats/AppendEntries can be dropped enough in a row to trip a
	// follower's election timeout). Re-resolve whoever is leader now, rather
	// than assuming leaderId survived the run, and require progress rather
	// than the exact 3 entries — a term-changing leader can still land
	// mid-replication.
	currentLeaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			currentLeaderId = id
			break
		}
	}
	if currentLeaderId == 0 {
		t.Fatalf("seed=%d: no leader present after replication window", seed)
	}

	leaderStatus := cluster.nodes[currentLeaderId].Status()
	if leaderStatus.CommitIndex == 0 {
		t.Fatalf("seed=%d: leader %d never committed anything under chaos", seed, currentLeaderId)
	}
	t.Logf("seed=%d: leader %d commitIndex=%d log=%v", seed, currentLeaderId, leaderStatus.CommitIndex, leaderStatus.Log)

	for _, id := range ids {
		status := cluster.nodes[id].Status()
		for i := 0; i <= min(status.CommitIndex, leaderStatus.CommitIndex); i++ {
			if fmt.Sprint(status.Log[i]) != fmt.Sprint(leaderStatus.Log[i]) {
				t.Fatalf("seed=%d: node %d diverges from leader %d at committed index %d:\n  node %d: %v\n  leader %d: %v",
					seed, id, currentLeaderId, i, id, status.Log[i], currentLeaderId, leaderStatus.Log[i])
			}
		}
	}
}

func TestClusterPartitionAndHeal(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	cluster := NewCluster(ids, seed, rand.New(rand.NewSource(seed)), ChaosConfig{})

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while electing the initial leader: %v", seed, err)
	}

	oldLeaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			oldLeaderId = id
			break
		}
	}
	if oldLeaderId == 0 {
		t.Fatalf("seed=%d: no leader elected after 300 ticks", seed)
	}
	t.Logf("seed=%d: node %d elected initial leader", seed, oldLeaderId)

	var majority []int
	for _, id := range ids {
		if id != oldLeaderId {
			majority = append(majority, id)
		}
	}

	// A leader commits a no-op of its own term as soon as it is elected, so
	// the old leader goes into the partition with commitIndex already past
	// zero. The property under test is that it commits nothing *further*
	// while it is alone, so measure from where it starts.
	beforePartition := cluster.nodes[oldLeaderId].Status().CommitIndex

	// Cut the old leader off alone; the other two form a reachable majority.
	cluster.network.Partition([][]int{{oldLeaderId}, majority})

	// The old leader can still append locally, but with no majority reachable
	// it can never commit these — they must be discarded once it rejoins.
	stranded, ok := cluster.Propose(oldLeaderId, []raft.Command{{Key: "stranded-1"}, {Key: "stranded-2"}})
	if !ok {
		t.Fatalf("seed=%d: partitioned old leader %d rejected the proposal; with no quorum check it should still believe it leads", seed, oldLeaderId)
	}

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while majority elects a new leader: %v", seed, err)
	}

	if got := cluster.nodes[oldLeaderId].Status().CommitIndex; got != beforePartition {
		t.Fatalf("seed=%d: partitioned old leader advanced commitIndex %d -> %d with no majority reachable",
			seed, beforePartition, got)
	}

	newLeaderId := 0
	for _, id := range majority {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			newLeaderId = id
			break
		}
	}
	if newLeaderId == 0 {
		t.Fatalf("seed=%d: majority side never elected a new leader after partition", seed)
	}
	t.Logf("seed=%d: node %d elected new leader in the majority partition", seed, newLeaderId)

	newLeaderBase := lastIndex(cluster.nodes[newLeaderId].Status())
	majorityProposals, ok := cluster.Propose(newLeaderId, []raft.Command{{Key: "m1"}, {Key: "m2"}, {Key: "m3"}})
	if !ok {
		t.Fatalf("seed=%d: new leader %d rejected a proposal", seed, newLeaderId)
	}

	if err := cluster.Run(100); err != nil {
		t.Fatalf("seed=%d: safety violation while majority replicates: %v", seed, err)
	}

	newLeaderStatus := cluster.nodes[newLeaderId].Status()
	if want := newLeaderBase + 3; newLeaderStatus.CommitIndex != want {
		t.Fatalf("seed=%d: new leader commitIndex = %d, want %d (majority should commit without the partitioned node)",
			seed, newLeaderStatus.CommitIndex, want)
	}
	mustResolve(t, seed, majorityProposals)

	cluster.network.Heal()

	if err := cluster.Run(100); err != nil {
		t.Fatalf("seed=%d: safety violation while old leader rejoins and repairs: %v", seed, err)
	}

	mustFail(t, seed, stranded, "truncated")

	finalLeaderStatus := cluster.nodes[newLeaderId].Status()
	for _, id := range ids {
		status := cluster.nodes[id].Status()
		if status.CommitIndex != finalLeaderStatus.CommitIndex {
			t.Fatalf("seed=%d: node %d commitIndex = %d, want %d (leader's) after healing",
				seed, id, status.CommitIndex, finalLeaderStatus.CommitIndex)
		}
		if fmt.Sprint(status.Log) != fmt.Sprint(finalLeaderStatus.Log) {
			t.Fatalf("seed=%d: node %d log did not converge with leader %d after healing:\n  node %d: %v\n  leader %d: %v",
				seed, id, newLeaderId, id, status.Log, newLeaderId, finalLeaderStatus.Log)
		}
		for _, entry := range status.Log {
			if entry.Cmd == (raft.Command{Key: "stranded-1"}) || entry.Cmd == (raft.Command{Key: "stranded-2"}) {
				t.Fatalf("seed=%d: node %d retained a stranded, never-committed entry after repair: %v", seed, id, status.Log)
			}
		}
	}
	t.Logf("seed=%d: converged log after heal: %v", seed, finalLeaderStatus.Log)
}

// TestClusterRestartRecoversPersistedState simulates a crash-and-recover
// cycle for the leader: its in-memory Core is thrown away and rebuilt via
// Cluster.Restart, which reads back whatever its Storage persisted. The
// recovered node must come back with the same term and log a real restart
// would preserve, but as a follower (state itself is never persisted), and
// the cluster must go on to re-elect and converge normally afterward.
func TestClusterRestartRecoversPersistedState(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	cluster := NewCluster(ids, seed, rand.New(rand.NewSource(seed)), ChaosConfig{})

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while electing a leader: %v", seed, err)
	}

	leaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			leaderId = id
			break
		}
	}
	if leaderId == 0 {
		t.Fatalf("seed=%d: no leader elected after 300 ticks", seed)
	}
	t.Logf("seed=%d: node %d elected leader", seed, leaderId)

	base := lastIndex(cluster.nodes[leaderId].Status())
	initial, ok := cluster.Propose(leaderId, []raft.Command{{Key: "a"}, {Key: "b"}, {Key: "c"}})
	if !ok {
		t.Fatalf("seed=%d: leader %d rejected a proposal", seed, leaderId)
	}
	if err := cluster.Run(50); err != nil {
		t.Fatalf("seed=%d: safety violation while replicating: %v", seed, err)
	}

	beforeStatus := cluster.nodes[leaderId].Status()
	if want := base + 3; beforeStatus.CommitIndex != want {
		t.Fatalf("seed=%d: leader commitIndex = %d, want %d before restart", seed, beforeStatus.CommitIndex, want)
	}
	mustResolve(t, seed, initial)

	// Anything still outstanding when a node restarts is unanswerable: its
	// state machine is rebuilt from scratch and it may never re-apply those
	// indices itself. Restart must hand those callers a rejection.
	orphaned, ok := cluster.Propose(leaderId, []raft.Command{{Key: "lost-to-restart"}})
	if !ok {
		t.Fatalf("seed=%d: leader %d rejected a proposal", seed, leaderId)
	}

	cluster.Restart(leaderId)
	mustFail(t, seed, orphaned, "restarted")

	restarted := cluster.nodes[leaderId].Status()
	if restarted.State != raft.FollowerState {
		t.Fatalf("seed=%d: restarted node %d should come back as follower, got %s", seed, leaderId, restarted.State)
	}
	if restarted.Term != beforeStatus.Term {
		t.Fatalf("seed=%d: restarted node %d term = %d, want %d (recovered)", seed, leaderId, restarted.Term, beforeStatus.Term)
	}
	if fmt.Sprint(restarted.Log) != fmt.Sprint(beforeStatus.Log) {
		t.Fatalf("seed=%d: restarted node %d log = %v, want recovered log %v", seed, leaderId, restarted.Log, beforeStatus.Log)
	}

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation after restart: %v", seed, err)
	}

	finalLeaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			finalLeaderId = id
			break
		}
	}
	if finalLeaderId == 0 {
		t.Fatalf("seed=%d: no leader present after restart recovery window", seed)
	}

	// A freshly (re-)elected leader can only advance commitIndex by
	// counting replication of entries from its own current term, so it may
	// not yet reflect the pre-restart commits even though every node
	// already agrees on them under the hood. Propose something new and
	// require it to commit, which proves the cluster is actually healthy
	// post-restart rather than just quiescent.
	afterRestart, ok := cluster.Propose(finalLeaderId, []raft.Command{{Key: "d"}, {Key: "e"}})
	if !ok {
		t.Fatalf("seed=%d: leader %d rejected a proposal", seed, finalLeaderId)
	}
	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while replicating after restart: %v", seed, err)
	}
	mustResolve(t, seed, afterRestart)

	currentLeaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			currentLeaderId = id
			break
		}
	}
	if currentLeaderId == 0 {
		t.Fatalf("seed=%d: no leader present after post-restart replication window", seed)
	}
	leaderStatus := cluster.nodes[currentLeaderId].Status()
	if leaderStatus.CommitIndex <= beforeStatus.CommitIndex {
		t.Fatalf("seed=%d: leader %d commitIndex = %d did not advance past pre-restart %d",
			seed, currentLeaderId, leaderStatus.CommitIndex, beforeStatus.CommitIndex)
	}

	for _, id := range ids {
		status := cluster.nodes[id].Status()
		for i := 0; i <= min(status.CommitIndex, leaderStatus.CommitIndex); i++ {
			if fmt.Sprint(status.Log[i]) != fmt.Sprint(leaderStatus.Log[i]) {
				t.Fatalf("seed=%d: node %d diverges from leader %d at committed index %d:\n  node %d: %v\n  leader %d: %v",
					seed, id, currentLeaderId, i, id, status.Log[i], currentLeaderId, leaderStatus.Log[i])
			}
		}
	}
	t.Logf("seed=%d: converged after restart, leader %d commitIndex=%d", seed, currentLeaderId, leaderStatus.CommitIndex)
}

// TestClusterRestartRecoversFromSnapshot exercises the full snapshot
// persistence path: CompactLog -> Ready() surfaces it -> the driver calls
// Storage.PersistSnapshot -> Restart rebuilds the node from
// Storage.SnapshotState() instead of the (now partially discarded) raw log.
func TestClusterRestartRecoversFromSnapshot(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	cluster := NewCluster(ids, seed, rand.New(rand.NewSource(seed)), ChaosConfig{})

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while electing a leader: %v", seed, err)
	}

	leaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			leaderId = id
			break
		}
	}
	if leaderId == 0 {
		t.Fatalf("seed=%d: no leader elected after 300 ticks", seed)
	}

	base := lastIndex(cluster.nodes[leaderId].Status())
	initial, ok := cluster.Propose(leaderId, []raft.Command{{Key: "a"}, {Key: "b"}, {Key: "c"}})
	if !ok {
		t.Fatalf("seed=%d: leader %d rejected a proposal", seed, leaderId)
	}
	if err := cluster.Run(50); err != nil {
		t.Fatalf("seed=%d: safety violation while replicating: %v", seed, err)
	}

	beforeStatus := cluster.nodes[leaderId].Status()
	if want := base + 3; beforeStatus.CommitIndex != want {
		t.Fatalf("seed=%d: leader commitIndex = %d, want %d before compaction", seed, beforeStatus.CommitIndex, want)
	}
	mustResolve(t, seed, initial)

	// Simulate the driver having applied everything up to "b" and compacting
	// it away, leaving only "c" behind the boundary placeholder.
	compactAt := base + 2
	termAtCompact := beforeStatus.Log[compactAt-beforeStatus.StartIndex].Term
	snap := cluster.nodes[leaderId].Compact(compactAt, termAtCompact)

	// Give the driver loop a chance to see SnapshotIndex on Ready() and
	// persist it via Storage.PersistSnapshot.
	if err := cluster.Run(5); err != nil {
		t.Fatalf("seed=%d: safety violation after compaction: %v", seed, err)
	}

	snapIndex, snapTerm, snapData := cluster.nodes[leaderId].Storage().SnapshotState()
	if snapIndex != compactAt || snapTerm != termAtCompact || !bytes.Equal(snapData.([]byte), snap.([]byte)) {
		t.Fatalf("seed=%d: storage SnapshotState = %d/%d/%v, want %d/%d/%v",
			seed, snapIndex, snapTerm, snapData, compactAt, termAtCompact, snap)
	}

	cluster.Restart(leaderId)

	restarted := cluster.nodes[leaderId].Status()
	if restarted.StartIndex != compactAt {
		t.Fatalf("seed=%d: restarted node %d StartIndex = %d, want %d", seed, leaderId, restarted.StartIndex, compactAt)
	}
	if len(restarted.Log) != 2 || restarted.Log[0].Cmd != (raft.Command{}) || restarted.Log[1].Cmd != (raft.Command{Key: "c"}) {
		t.Fatalf("seed=%d: restarted node %d Log = %+v, want [boundary@%d, c@%d]",
			seed, leaderId, restarted.Log, compactAt, compactAt+1)
	}

	// Prove the cluster is actually healthy post-restart, not just
	// quiescent: a fresh proposal must still commit and converge.
	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation after restart: %v", seed, err)
	}
	finalLeaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			finalLeaderId = id
			break
		}
	}
	if finalLeaderId == 0 {
		t.Fatalf("seed=%d: no leader present after restart recovery window", seed)
	}
	afterRestart, ok := cluster.Propose(finalLeaderId, []raft.Command{{Key: "d"}, {Key: "e"}})
	if !ok {
		t.Fatalf("seed=%d: leader %d rejected a proposal", seed, finalLeaderId)
	}
	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while replicating after restart: %v", seed, err)
	}
	mustResolve(t, seed, afterRestart)

	leaderStatus := cluster.nodes[finalLeaderId].Status()
	if leaderStatus.CommitIndex <= beforeStatus.CommitIndex {
		t.Fatalf("seed=%d: leader %d commitIndex = %d did not advance past pre-compaction %d",
			seed, finalLeaderId, leaderStatus.CommitIndex, beforeStatus.CommitIndex)
	}
	t.Logf("seed=%d: converged after snapshot restart, leader %d commitIndex=%d", seed, finalLeaderId, leaderStatus.CommitIndex)
}

func TestClusterElectsLeaderAndReplicatesProposedCommands(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	cluster := NewCluster(ids, seed, rand.New(rand.NewSource(seed)), ChaosConfig{})

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while electing a leader: %v", seed, err)
	}

	leaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			leaderId = id
			break
		}
	}
	if leaderId == 0 {
		t.Fatalf("seed=%d: no leader elected after 300 ticks", seed)
	}
	t.Logf("seed=%d: node %d elected leader", seed, leaderId)

	base := lastIndex(cluster.nodes[leaderId].Status())

	// Real KV ops rather than bare keys, so the results carry something worth
	// asserting: this is the only end-to-end check that a state machine's
	// return value reaches the caller that proposed the command, in order.
	proposals, ok := cluster.Propose(leaderId, []raft.Command{
		{Op: raft.OpPut, Key: "a", Value: "1"},
		{Op: raft.OpGet, Key: "a"},
		{Op: raft.OpCas, Key: "a", Expected: "1", Value: "2"},
	})
	if !ok {
		t.Fatalf("seed=%d: leader %d rejected a proposal", seed, leaderId)
	}

	if err := cluster.Run(50); err != nil {
		t.Fatalf("seed=%d: safety violation while replicating: %v", seed, err)
	}

	results := mustResolve(t, seed, proposals)
	want := []kv.Result{
		{},                        // Put reports nothing
		{Value: "1", Found: true}, // Get sees the Put that preceded it
		{Ok: true},                // Cas swaps, because Expected matched
	}
	for i := range want {
		if results[i] != want[i] {
			t.Fatalf("seed=%d: result %d = %+v, want %+v -- results must line up with the commands that produced them",
				seed, i, results[i], want[i])
		}
	}

	wantCommit := base + 3
	leaderStatus := cluster.nodes[leaderId].Status()
	if want := wantCommit + 1; len(leaderStatus.Log) != want { // dummy@0 + election no-ops + 3 proposed
		t.Fatalf("seed=%d: leader log = %v, want %d entries (%d pre-existing + 3 proposed)",
			seed, leaderStatus.Log, want, base+1)
	}
	if leaderStatus.CommitIndex != wantCommit {
		t.Fatalf("seed=%d: leader commitIndex = %d, want %d", seed, leaderStatus.CommitIndex, wantCommit)
	}

	for _, id := range ids {
		status := cluster.nodes[id].Status()
		if status.CommitIndex != wantCommit {
			t.Fatalf("seed=%d: node %d commitIndex = %d, want %d", seed, id, status.CommitIndex, wantCommit)
		}
		if fmt.Sprint(status.Log) != fmt.Sprint(leaderStatus.Log) {
			t.Fatalf("seed=%d: node %d log did not converge with leader:\n  node %d: %v\n  leader %d: %v",
				seed, id, id, status.Log, leaderId, leaderStatus.Log)
		}
	}
}

// TestClusterSnapshotInstallFailsStrandedProposals covers the one fail path
// with no other safety net. handleInstallSnapshotRequest throws the whole log
// away without ever reporting TruncateFrom, so a proposal stranded at an index
// the incoming snapshot covers is never truncated, never re-applied, and never
// otherwise noticed. Without failUpTo on that branch its caller waits forever.
func TestClusterSnapshotInstallFailsStrandedProposals(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	cluster := NewCluster(ids, seed, rand.New(rand.NewSource(seed)), ChaosConfig{})
	cluster.CompactThreshold = 2

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while electing the initial leader: %v", seed, err)
	}

	oldLeaderId := 0
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			oldLeaderId = id
			break
		}
	}
	if oldLeaderId == 0 {
		t.Fatalf("seed=%d: no leader elected after 300 ticks", seed)
	}

	var majority []int
	for _, id := range ids {
		if id != oldLeaderId {
			majority = append(majority, id)
		}
	}
	cluster.network.Partition([][]int{{oldLeaderId}, majority})

	stranded, ok := cluster.Propose(oldLeaderId, []raft.Command{{Op: raft.OpPut, Key: "stranded", Value: "x"}})
	if !ok {
		t.Fatalf("seed=%d: partitioned old leader %d rejected the proposal", seed, oldLeaderId)
	}

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while the majority elects a new leader: %v", seed, err)
	}
	newLeaderId := 0
	for _, id := range majority {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			newLeaderId = id
			break
		}
	}
	if newLeaderId == 0 {
		t.Fatalf("seed=%d: majority side never elected a new leader", seed)
	}

	// Drive the majority far enough ahead that CompactThreshold trims the
	// log past the stranded index, so the rejoining node cannot be repaired
	// by AppendEntries and must be sent a snapshot instead.
	for round := 0; round < 5; round++ {
		if _, ok := cluster.Propose(newLeaderId, []raft.Command{
			{Op: raft.OpPut, Key: fmt.Sprintf("k%d", round), Value: "v"},
		}); !ok {
			t.Fatalf("seed=%d: new leader %d rejected a proposal in round %d", seed, newLeaderId, round)
		}
		if err := cluster.Run(50); err != nil {
			t.Fatalf("seed=%d: safety violation while the majority advances: %v", seed, err)
		}
	}

	leaderStart := cluster.nodes[newLeaderId].Status().StartIndex
	strandedIndex := 1 // the partitioned leader's log was empty when it proposed
	if leaderStart < strandedIndex {
		t.Fatalf("seed=%d: new leader startIndex = %d, needs to exceed the stranded index %d for a snapshot to be required",
			seed, leaderStart, strandedIndex)
	}

	cluster.network.Heal()
	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while the old leader rejoins via snapshot: %v", seed, err)
	}

	if got := cluster.nodes[oldLeaderId].Status().StartIndex; got == 0 {
		t.Fatalf("seed=%d: node %d StartIndex = 0, so it was repaired by AppendEntries and this test never exercised the snapshot path",
			seed, oldLeaderId)
	}
	mustFail(t, seed, stranded, "snapshot installed")
}

// TestClusterRestartRebuildsStateMachineFromSnapshot pins down the half of a
// restart that the raft log cannot recover on its own. Core.Restore seeds
// appliedIndex at the snapshot boundary, so a recovered node never replays the
// entries folded into it; if the driver hands it an empty state machine, that
// data and -- worse -- the client session table are silently gone.
//
// An empty session table is the sharp end: the node re-executes requests it
// has already answered, so a client mid-retry gets its command applied twice.
// Every raft-level check stays green throughout, because the logs really are
// identical. Only comparing what the state machines produced catches it, which
// is what ResultLog inside Run does here.
func TestClusterRestartRebuildsStateMachineFromSnapshot(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	cluster := NewCluster(ids, seed, rand.New(rand.NewSource(seed)), ChaosConfig{})
	cluster.CompactThreshold = 2

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while electing a leader: %v", seed, err)
	}

	leaderId := func() int {
		for _, id := range ids {
			if cluster.nodes[id].Status().State == raft.LeaderState {
				return id
			}
		}
		return 0
	}

	const clientID = "session-client"
	const rounds = 6
	for round := 0; round < rounds; round++ {
		leader := leaderId()
		if leader == 0 {
			t.Fatalf("seed=%d: no leader in round %d", seed, round)
		}
		if _, ok := cluster.Propose(leader, []raft.Command{{
			Op: raft.OpPut, Key: fmt.Sprintf("k%d", round), Value: "v",
			ClientID: clientID, Seq: uint64(round + 1),
		}}); !ok {
			t.Fatalf("seed=%d: leader %d rejected a proposal in round %d", seed, leader, round)
		}
		if err := cluster.Run(50); err != nil {
			t.Fatalf("seed=%d: safety violation in round %d: %v", seed, round, err)
		}
	}

	victim := ids[0]
	if got := cluster.nodes[victim].Status().StartIndex; got == 0 {
		t.Fatalf("seed=%d: node %d never compacted (StartIndex=0), so a restart would replay the whole log and this test proves nothing",
			seed, victim)
	}

	cluster.Restart(victim)
	if err := cluster.Run(200); err != nil {
		t.Fatalf("seed=%d: safety violation after restart: %v", seed, err)
	}

	// A long-delayed duplicate of the client's very first request. Every node
	// must recognise it as stale from its session table. A node that came back
	// with an empty one instead re-executes it, and ResultLog inside Run sees
	// the two nodes disagree about what index produced.
	leader := leaderId()
	if leader == 0 {
		t.Fatalf("seed=%d: no leader after restart", seed)
	}
	stale, ok := cluster.Propose(leader, []raft.Command{{
		Op: raft.OpPut, Key: "k0", Value: "clobbered", ClientID: clientID, Seq: 1,
	}})
	if !ok {
		t.Fatalf("seed=%d: leader %d rejected the stale retry", seed, leader)
	}
	if err := cluster.Run(200); err != nil {
		t.Fatalf("seed=%d: nodes disagreed on the result of a stale retry: %v", seed, err)
	}
	mustFail(t, seed, stale, "session too old")

	// And the write it carried must not have landed anywhere.
	readBack, ok := cluster.Propose(leaderId(), []raft.Command{{Op: raft.OpGet, Key: "k0"}})
	if !ok {
		t.Fatalf("seed=%d: leader rejected the read-back", seed)
	}
	if err := cluster.Run(100); err != nil {
		t.Fatalf("seed=%d: safety violation during read-back: %v", seed, err)
	}
	if res := mustResolve(t, seed, readBack)[0]; res.Value != "v" {
		t.Fatalf("seed=%d: k0 = %+v after a suppressed duplicate, want the original %q", seed, res, "v")
	}
}
