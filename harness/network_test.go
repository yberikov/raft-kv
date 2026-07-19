package harness

import (
	"math/rand"
	"os"
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

func newTrio(seed int64) (map[uint64]*raft.Core, []uint64) {
	ids := []uint64{1, 2, 3}
	nodes := map[uint64]*raft.Core{}
	for _, id := range ids {
		nodes[id] = raft.NewCore(id, ids, 10, 20, rand.New(rand.NewSource(seed+int64(id))), 3)
	}
	return nodes, ids
}

func leaderOf(nodes map[uint64]*raft.Core, ids []uint64) uint64 {
	for _, id := range ids {
		status := nodes[id].Status()
		if status.State == raft.LeaderState {
			return id
		}
	}
	return 0
}

func runTicks(net *Network, nodes map[uint64]*raft.Core, ids []uint64, n int) {
	for i := 0; i < n; i++ {
		for _, id := range ids {
			nodes[id].Tick()
			net.Send(nodes[id].Ready()...)
		}
		for _, m := range net.Advance() {
			nodes[m.ToId].Step(m)
			net.Send(nodes[m.ToId].Ready()...)
		}
	}
}

func TestNetworkZeroChaosElectsLeader(t *testing.T) {
	seed := testSeed(t)
	nodes, ids := newTrio(seed)
	net := &Network{rng: rand.New(rand.NewSource(seed))}

	runTicks(net, nodes, ids, 300)

	leader := leaderOf(nodes, ids)
	if leader == 0 {
		t.Fatalf("seed=%d: no leader elected after 300 ticks", seed)
	}
	t.Logf("seed=%d: node %d elected leader", seed, leader)

	leaders := 0
	for _, id := range ids {
		status := nodes[id].Status()
		if status.State == raft.LeaderState {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("seed=%d: expected exactly 1 leader, found %d", seed, leaders)
	}
}
