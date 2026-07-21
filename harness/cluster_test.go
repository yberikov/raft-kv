package harness

import (
	"fmt"
	"math/rand"
	"testing"

	"raft-kv/raft"
)

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
