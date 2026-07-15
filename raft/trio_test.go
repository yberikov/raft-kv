package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// deliverer wires three (or more) Cores together in-process: Tick everyone,
// collect Ready() output, route by ToId, repeat. No drops, no delays — that's
// harness/'s job in Phase 2. This just proves the protocol closes the loop.
type deliverer struct {
	nodes map[uint64]*Core
	ids   []uint64
}

func newDeliverer(nodes map[uint64]*Core) *deliverer {
	d := &deliverer{nodes: nodes}
	for id := range nodes {
		d.ids = append(d.ids, id)
	}
	return d
}

func (d *deliverer) round(inbox []Message) []Message {
	for _, id := range d.ids {
		d.nodes[id].Tick()
	}
	for _, id := range d.ids {
		inbox = append(inbox, d.nodes[id].Ready()...)
	}
	var next []Message
	for _, m := range inbox {
		d.nodes[m.ToId].Step(m)
	}
	for _, id := range d.ids {
		next = append(next, d.nodes[id].Ready()...)
	}
	return next
}

func (d *deliverer) leader() uint64 {
	for id, n := range d.nodes {
		if n.state == LeaderState {
			return id
		}
	}
	return 0
}

func newTrio(t *testing.T, seed int64) *deliverer {
	t.Helper()
	ids := []uint64{1, 2, 3}
	nodes := map[uint64]*Core{}
	for _, id := range ids {
		nodes[id] = NewCore(id, ids, 10, 20, rand.New(rand.NewSource(seed+int64(id))), 3)
	}
	return newDeliverer(nodes)
}

func TestTrioElectsLeaderAndReplicates(t *testing.T) {
	seed := testSeed(t)
	d := newTrio(t, seed)

	var inbox []Message
	for round := 0; round < 200 && d.leader() == 0; round++ {
		inbox = d.round(inbox)
	}
	leader := d.leader()
	if leader == 0 {
		t.Fatalf("seed=%d: no leader elected after 200 rounds", seed)
	}
	t.Logf("seed=%d: node %d elected leader in term %d", seed, leader, d.nodes[leader].currentTerm)

	for i := 0; i < 3; i++ {
		d.nodes[leader].log = append(d.nodes[leader].log, Entry{cmd: fmt.Sprintf("op-%d", i), term: d.nodes[leader].currentTerm})
	}
	for extra := 0; extra < 30; extra++ {
		inbox = d.round(inbox)
	}

	if got := d.nodes[leader].commitIndex; got != 3 {
		t.Fatalf("seed=%d: leader commitIndex = %d, want 3", seed, got)
	}
	for _, id := range d.ids {
		if fmt.Sprint(d.nodes[id].log) != fmt.Sprint(d.nodes[leader].log) {
			t.Fatalf("seed=%d: node %d log did not converge with leader:\n  node %d: %v\n  leader %d: %v",
				seed, id, id, d.nodes[id].log, leader, d.nodes[leader].log)
		}
	}
}

func TestTrioRepairsDivergedFollower(t *testing.T) {
	seed := testSeed(t)
	d := newTrio(t, seed)

	// Force node 1 into an established leadership at term 5 without going
	// through an election — we're testing log repair here, not election.
	leader := d.nodes[1]
	leader.currentTerm = 5
	leader.state = LeaderState
	leader.log = append(leader.log, Entry{cmd: "a", term: 5}, Entry{cmd: "b", term: 5})
	for _, id := range d.ids {
		leader.nextIndex[id] = len(leader.log)
		leader.matchIndex[id] = 0
	}

	// node 2 diverges: uncommitted entries from stale terms that must be
	// walked back and overwritten by the leader's log.
	follower := d.nodes[2]
	follower.currentTerm = 5
	follower.log = append(follower.log, Entry{cmd: "x", term: 3}, Entry{cmd: "y", term: 3}, Entry{cmd: "z", term: 4})

	// node 3 is already caught up with the leader — kept in sync so it can't
	// be recruited into a competing election by node 2's (locally more
	// "advanced") diverged log while this test isolates repair on node 2.
	d.nodes[3].currentTerm = 5
	d.nodes[3].log = append(d.nodes[3].log, Entry{cmd: "a", term: 5}, Entry{cmd: "b", term: 5})

	var inbox []Message
	for round := 0; round < 50; round++ {
		inbox = d.round(inbox)
	}

	for _, id := range []uint64{2, 3} {
		if fmt.Sprint(d.nodes[id].log) != fmt.Sprint(leader.log) {
			t.Fatalf("seed=%d: node %d did not repair to match the leader:\n  node %d: %v\n  leader: %v",
				seed, id, id, d.nodes[id].log, leader.log)
		}
	}
}
