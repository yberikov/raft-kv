package harness

import (
	"fmt"
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
		cluster.CompactThreshold = 1 + rng.Intn(3)

		doPartition := rng.Float64() < 0.4
		doPropose := rng.Float64() < 0.7
		doRestart := rng.Float64() < 0.4
		doCrash := rng.Float64() < 0.4

		run := func(ticks int) {
			if err := cluster.Run(ticks); err != nil {
				t.Fatalf("seed=%d: safety violation: %v", seed, err)
			}
		}

		// clientID/seq model a single client session across the whole fuzz
		// iteration, so retries below exercise the state machine's dedup
		// table the way a real client's at-least-once resend would.
		clientID := fmt.Sprintf("fuzz-client-%d", seed)
		seq := uint64(0)
		nextCmd := func(key string) raft.Command {
			seq++
			return raft.Command{Op: raft.OpPut, Key: key, Value: key, ClientID: clientID, Seq: seq}
		}

		var lastProposed []raft.Command
		propose := func(cmds []raft.Command) {
			if leaderId := fuzzLeaderId(cluster, ids); leaderId != 0 {
				cluster.nodes[leaderId].Propose(cmds)
			}
			lastProposed = cmds
		}
		// retryLast simulates a client that didn't get a response in time
		// (leader change, partition, dropped reply) and resent the exact
		// same request -- same ClientID/Seq -- which the state machine's
		// dedup table must suppress rather than re-applying.
		retryLast := func() {
			if lastProposed != nil {
				propose(lastProposed)
			}
		}

		run(100 + rng.Intn(200))

		if doPropose {
			propose([]raft.Command{nextCmd("a"), nextCmd("b"), nextCmd("c")})
			if rng.Float64() < 0.4 {
				run(1 + rng.Intn(20))
				retryLast()
			}
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
				propose([]raft.Command{nextCmd("d"), nextCmd("e"), nextCmd("f")})
				if rng.Float64() < 0.4 {
					run(1 + rng.Intn(20))
					retryLast()
				}
			}

			cluster.network.Heal()
		}

		if doRestart {
			victim := ids[rng.Intn(len(ids))]
			cluster.Restart(victim)
		}

		if doCrash {
			victim := ids[rng.Intn(len(ids))]
			cluster.Crash(victim)
		}

		run(100 + rng.Intn(200))
	}
}
