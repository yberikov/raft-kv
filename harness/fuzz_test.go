package harness

import (
	"math/rand"
	"testing"

	"raft-kv/raft"
)

func fuzzLeaderId(cluster Cluster, ids []int) int {
	for _, id := range ids {
		if cluster.nodes[id].Status().State == raft.LeaderState {
			return id
		}
	}
	return 0
}

// TestFuzzCluster sweeps many seeds with randomized chaos parameters and
// occasional partition/heal cycles, looking for any checker violation.
// Each seed is fully reproducible on its own via RAFT_TEST_SEED=<base>+i.
func TestFuzzCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fuzz sweep in short mode")
	}

	base := testSeed(t)
	ids := []int{1, 2, 3}
	const iterations = 200

	for i := 0; i < iterations; i++ {
		seed := base + int64(i)
		rng := rand.New(rand.NewSource(seed))

		chaos := ChaosConfig{
			DropP:      rng.Float64() * 0.3,
			DuplicateP: rng.Float64() * 0.3,
			MinDelay:   1 + rng.Intn(3),
		}
		chaos.MaxDelay = chaos.MinDelay + 1 + rng.Intn(3)

		cluster := NewCluster(ids, seed, rng, chaos)

		doPartition := rng.Float64() < 0.4
		doPropose := rng.Float64() < 0.7

		run := func(ticks int) {
			if err := cluster.Run(ticks); err != nil {
				t.Fatalf("seed=%d: safety violation: %v", seed, err)
			}
		}

		propose := func(cmds []any) {
			if leaderId := fuzzLeaderId(cluster, ids); leaderId != 0 {
				cluster.nodes[leaderId].Step(raft.Message{
					Type:       raft.MsgProposeRequest,
					ProposeCmd: cmds,
				})
			}
		}

		run(100 + rng.Intn(200))

		if doPropose {
			propose([]any{"a", "b", "c"})
		}

		if doPartition {
			victim := ids[rng.Intn(len(ids))]
			var rest []int
			for _, id := range ids {
				if id != victim {
					rest = append(rest, id)
				}
			}
			cluster.network.Partition([][]int{{victim}, rest})

			run(100 + rng.Intn(200))

			if doPropose {
				propose([]any{"d", "e", "f"})
			}

			cluster.network.Heal()
		}

		run(100 + rng.Intn(200))
	}
}
