package raft

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// testSeed returns RAFT_TEST_SEED if set, otherwise a fresh random seed.
// Always logged so a failure can be replayed with RAFT_TEST_SEED=<n> go test ./raft/...
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

type coreOpt func(*Core)

func withTerm(term uint64) coreOpt {
	return func(c *Core) { c.currentTerm = term }
}

func withState(s stateType) coreOpt {
	return func(c *Core) { c.state = s }
}

func withVotedFor(id uint64) coreOpt {
	return func(c *Core) { c.votedFor = id }
}

func withVotesGranted(ids ...uint64) coreOpt {
	return func(c *Core) {
		c.votesGranted = map[uint64]bool{}
		for _, id := range ids {
			c.votesGranted[id] = true
		}
	}
}

// withLog appends entries after the implicit dummy entry NewCore seeds at
// index 0 (term 0). Entry i in the call ends up at array index i+1.
func withLog(entries ...Entry) coreOpt {
	return func(c *Core) { c.log = append(c.log, entries...) }
}

func withCommitIndex(idx int) coreOpt {
	return func(c *Core) { c.commitIndex = idx }
}

func withNextIndex(peer uint64, idx int) coreOpt {
	return func(c *Core) { c.nextIndex[peer] = idx }
}

func withMatchIndex(peer uint64, idx int) coreOpt {
	return func(c *Core) { c.matchIndex[peer] = idx }
}

// newTestCore builds a Core seeded deterministically from the test's seed
// (see testSeed), then applies opts to drop it directly into whatever state
// a test case needs without driving it there through a sequence of messages.
func newTestCore(t *testing.T, id uint64, peers []uint64, opts ...coreOpt) *Core {
	t.Helper()
	seed := testSeed(t)
	rng := rand.New(rand.NewSource(seed))
	c := NewCore(id, peers, 10, 20, rng, 3)
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// stepAndGetResponse feeds req into c and asserts it produced exactly one
// outgoing message, returning it.
func stepAndGetResponse(t *testing.T, c *Core, req Message) Message {
	t.Helper()
	c.Step(req)
	msgs := c.Ready()
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 response message, got %d: %+v", len(msgs), msgs)
	}
	return msgs[0]
}
