package harness

import (
	"fmt"
	"math/rand"

	"raft-kv/driver"
	"raft-kv/raft"
	"raft-kv/storage"
)

type Cluster struct {
	ids              []int
	nodes            map[int]*driver.Node
	network          *Network
	committedLog     CommittedLog
	resultLog        ResultLog
	debugBuff        RingBuffer
	CompactThreshold int
}

func NewCluster(ids []int, seed int64, rng *rand.Rand, chaosConfig ChaosConfig) Cluster {
	cluster := Cluster{
		ids:          ids,
		network:      NewNetwork(seed, rng, chaosConfig),
		nodes:        make(map[int]*driver.Node),
		committedLog: NewCommittedLog(),
		resultLog:    NewResultLog(),
		debugBuff:    NewRingBuffer(50),
	}

	for _, id := range ids {
		cluster.nodes[id] = driver.NewNode(driver.Config{
			Id:               id,
			Peers:            ids,
			MinElectionTicks: 10,
			MaxElectionTicks: 20,
			ReplicatePeriod:  3,
			Rng:              rng,
		}, storage.NewInMemoryStorage(3))
	}
	return cluster
}

func (c *Cluster) node(id int) *driver.Node { return c.nodes[id] }

func (c *Cluster) Restart(id int) { c.node(id).Restart() }
func (c *Cluster) Crash(id int)   { c.node(id).Crash() }

func (c *Cluster) Propose(id int, cmds []raft.Command) ([]*driver.Pending, bool) {
	return c.node(id).Propose(cmds)
}

func (c *Cluster) Run(tick int) error {
	for i := 0; i < tick; i++ {
		for _, id := range c.ids {
			c.nodes[id].CompactThreshold = c.CompactThreshold
			send, applied := c.node(id).Tick()

			for _, a := range applied {
				if err := c.resultLog.Merge(id, a.Index, a.Result); err != nil {
					return c.fail(i, err)
				}
			}
			c.network.Send(send...)
		}

		for _, msg := range c.network.Advance() {
			c.debugBuff.Push(i, msg)
			c.node(msg.ToId).Step(msg)
		}

		statuses := make([]raft.Status, 0, len(c.ids))
		for _, id := range c.ids {
			statuses = append(statuses, c.node(id).Status())
		}

		if err := c.committedLog.Merge(statuses); err != nil {
			return c.fail(i, err)
		}
		if err := c.committedLog.CheckLeaderCompleteness(statuses); err != nil {
			return c.fail(i, err)
		}
		if err := CheckLogMatching(statuses); err != nil {
			return c.fail(i, err)
		}
		if err := CheckElectionSafety(statuses); err != nil {
			return c.fail(i, err)
		}
	}
	return nil
}

func (c *Cluster) fail(tick int, err error) error {
	c.debugBuff.Dump(c.network.seed)
	return fmt.Errorf("error on seed %v and tick %v: %v", c.network.seed, tick, err)
}
