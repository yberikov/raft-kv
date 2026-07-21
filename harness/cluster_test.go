package harness

import (
	"fmt"
	"math/rand"
	"testing"

	"raft-kv/raft"
)

func TestClusterWithChaosElectsLeaderAndReplicatesProposedCommands(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	chaos := ChaosConfig{DropP: 0.05, DuplicateP: 0.05, MinDelay: 1, MaxDelay: 3}
	cluster := NewCluster(ids, rand.New(rand.NewSource(seed)), chaos)

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

	cluster.nodes[leaderId].Step(raft.Message{
		Type:       raft.MsgProposeRequest,
		ProposeCmd: []any{"a", "b", "c"},
	})

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
	cluster := NewCluster(ids, rand.New(rand.NewSource(seed)), ChaosConfig{})

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

	// Cut the old leader off alone; the other two form a reachable majority.
	cluster.network.Partition([][]int{{oldLeaderId}, majority})

	// The old leader can still append locally, but with no majority reachable
	// it can never commit these — they must be discarded once it rejoins.
	cluster.nodes[oldLeaderId].Step(raft.Message{
		Type:       raft.MsgProposeRequest,
		ProposeCmd: []any{"stranded-1", "stranded-2"},
	})

	if err := cluster.Run(300); err != nil {
		t.Fatalf("seed=%d: safety violation while majority elects a new leader: %v", seed, err)
	}

	if got := cluster.nodes[oldLeaderId].Status().CommitIndex; got != 0 {
		t.Fatalf("seed=%d: partitioned old leader committed something (commitIndex=%d) with no majority reachable", seed, got)
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

	cluster.nodes[newLeaderId].Step(raft.Message{
		Type:       raft.MsgProposeRequest,
		ProposeCmd: []any{"m1", "m2", "m3"},
	})

	if err := cluster.Run(100); err != nil {
		t.Fatalf("seed=%d: safety violation while majority replicates: %v", seed, err)
	}

	newLeaderStatus := cluster.nodes[newLeaderId].Status()
	if newLeaderStatus.CommitIndex != 3 {
		t.Fatalf("seed=%d: new leader commitIndex = %d, want 3 (majority should commit without the partitioned node)",
			seed, newLeaderStatus.CommitIndex)
	}

	cluster.network.Heal()

	if err := cluster.Run(100); err != nil {
		t.Fatalf("seed=%d: safety violation while old leader rejoins and repairs: %v", seed, err)
	}

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
			if entry.Cmd == "stranded-1" || entry.Cmd == "stranded-2" {
				t.Fatalf("seed=%d: node %d retained a stranded, never-committed entry after repair: %v", seed, id, status.Log)
			}
		}
	}
	t.Logf("seed=%d: converged log after heal: %v", seed, finalLeaderStatus.Log)
}

func TestClusterElectsLeaderAndReplicatesProposedCommands(t *testing.T) {
	seed := testSeed(t)
	ids := []int{1, 2, 3}
	cluster := NewCluster(ids, rand.New(rand.NewSource(seed)), ChaosConfig{})

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

	cluster.nodes[leaderId].Step(raft.Message{
		Type:       raft.MsgProposeRequest,
		ProposeCmd: []any{"a", "b", "c"},
	})

	if err := cluster.Run(50); err != nil {
		t.Fatalf("seed=%d: safety violation while replicating: %v", seed, err)
	}

	leaderStatus := cluster.nodes[leaderId].Status()
	if len(leaderStatus.Log) != 4 { // dummy@0 + 3 proposed entries
		t.Fatalf("seed=%d: leader log = %v, want 4 entries (dummy + 3 proposed)", seed, leaderStatus.Log)
	}
	if leaderStatus.CommitIndex != 3 {
		t.Fatalf("seed=%d: leader commitIndex = %d, want 3", seed, leaderStatus.CommitIndex)
	}

	for _, id := range ids {
		status := cluster.nodes[id].Status()
		if status.CommitIndex != 3 {
			t.Fatalf("seed=%d: node %d commitIndex = %d, want 3", seed, id, status.CommitIndex)
		}
		if fmt.Sprint(status.Log) != fmt.Sprint(leaderStatus.Log) {
			t.Fatalf("seed=%d: node %d log did not converge with leader:\n  node %d: %v\n  leader %d: %v",
				seed, id, id, status.Log, leaderId, leaderStatus.Log)
		}
	}
}
