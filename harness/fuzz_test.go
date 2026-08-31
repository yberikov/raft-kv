package harness

import (
	"fmt"
	"math/rand"
	"testing"

	"raft-kv/driver"
	"raft-kv/kv"
	"raft-kv/raft"
)

// requestKey identifies a client request across all its retries. Raft §8's
// dedup contract is stated in exactly these terms: a request is the pair
// (client, sequence number), and every delivery of it must be answered
// identically no matter how many copies reach the log.
type requestKey struct {
	clientID string
	seq      uint64
}

// trackedRequest pairs a proposal with the command that produced it, so a
// result can be attributed back to the request it answers.
type trackedRequest struct {
	cmd     raft.Command
	pending *driver.Pending
}

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

		// outstanding holds proposals whose results have not come back yet;
		// answered records, per request, the one answer that request is
		// allowed to have. Poll is a destructive read, so a request is
		// drained exactly once and its result kept.
		var outstanding []trackedRequest
		answered := make(map[requestKey]kv.Result)

		// drain is the result-stability checker: no matter how many times a
		// request reaches the log, every successful answer to it must be
		// identical. A retry that gets re-executed instead of deduped shows
		// up here as two different answers to one (client, seq).
		drain := func() {
			kept := outstanding[:0]
			for _, req := range outstanding {
				res, done := req.pending.Poll()
				if !done {
					kept = append(kept, req)
					continue
				}
				// A rejection is not an answer -- the entry was truncated,
				// snapshotted away, or lost to a restart. Only real results
				// are comparable, which also covers the case where the
				// original proposal never resolved and only the retry did.
				if res.Err != "" {
					continue
				}
				key := requestKey{req.cmd.ClientID, req.cmd.Seq}
				if prev, seen := answered[key]; seen && prev != res {
					t.Fatalf("seed=%d: request %+v was answered twice with different results: %+v then %+v -- a retry was re-executed instead of deduped",
						seed, key, prev, res)
				}
				answered[key] = res
			}
			outstanding = kept
		}

		run := func(ticks int) {
			if err := cluster.Run(ticks); err != nil {
				t.Fatalf("seed=%d: safety violation: %v", seed, err)
			}
			drain()
		}

		// clientID/seq model a single client session across the whole fuzz
		// iteration, so retries below exercise the state machine's dedup
		// table the way a real client's at-least-once resend would.
		clientID := fmt.Sprintf("fuzz-client-%d", seed)
		seq := uint64(0)
		next := func(op raft.OpType, key, value string) raft.Command {
			seq++
			return raft.Command{Op: op, Key: key, Value: value, ClientID: clientID, Seq: seq}
		}
		// A batch writes two keys with values unique to this request, then
		// reads the first one back. The read matters: a Put's result is the
		// empty Result, so a Put-only workload gives every request the same
		// indistinguishable answer and nothing downstream can tell two
		// results apart. The Get is what makes a result identifiable as
		// belonging to one specific request.
		batch := func(k1, k2 string) []raft.Command {
			return []raft.Command{
				next(raft.OpPut, k1, fmt.Sprintf("%s-%d", k1, seq+1)),
				next(raft.OpPut, k2, fmt.Sprintf("%s-%d", k2, seq+1)),
				next(raft.OpGet, k1, ""),
			}
		}

		var lastProposed []raft.Command
		propose := func(cmds []raft.Command) {
			if leaderId := fuzzLeaderId(cluster, ids); leaderId != 0 {
				if ps, ok := cluster.Propose(leaderId, cmds); ok {
					for i := range cmds {
						outstanding = append(outstanding, trackedRequest{cmd: cmds[i], pending: ps[i]})
					}
				}
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
			propose(batch("a", "b"))
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
				propose(batch("d", "e"))
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
