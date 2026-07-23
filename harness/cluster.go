package harness

import (
	"fmt"
	"math/rand"
	"raft-kv/raft"
)

type Cluster struct {
	ids          []int
	nodes        map[int]*raft.Core
	network      *Network
	committedLog CommittedLog
	debugBuff    RingBuffer
}

func NewCluster(ids []int, seed int64, rng *rand.Rand, chaosConfig ChaosConfig) Cluster {
	cluster := Cluster{
		ids:          ids,
		network:      NewNetwork(seed, rng, chaosConfig),
		committedLog: NewCommittedLog(),
		nodes:        make(map[int]*raft.Core),
		debugBuff:    NewRingBuffer(50),
	}
	for _, id := range ids {
		cluster.nodes[id] = raft.NewCore(id, ids, 10, 20, rng, 3)
	}
	return cluster
}

func (c *Cluster) Run(tick int) error {
	for i := 0; i < tick; i++ {
		for _, id := range c.ids {
			node := c.nodes[id]
			node.Tick()
			c.network.Send(node.Ready()...)
		}
		messages := c.network.Advance()
		for _, msg := range messages {
			c.debugBuff.Push(i, msg)
			c.nodes[msg.ToId].Step(msg)
		}

		statuses := make([]raft.Status, 0)
		for _, id := range c.ids {
			status := c.nodes[id].Status()
			statuses = append(statuses, status)
		}

		if err := c.committedLog.Merge(statuses); err != nil {
			c.debugBuff.Dump(c.network.seed)
			return fmt.Errorf("error on seed %v and tick %v: %v", c.network.seed, i, err)
		}
		if err := c.committedLog.CheckLeaderCompleteness(statuses); err != nil {
			c.debugBuff.Dump(c.network.seed)
			return fmt.Errorf("error on seed %v and tick %v: %v", c.network.seed, i, err)
		}
		if err := CheckLogMatching(statuses); err != nil {
			c.debugBuff.Dump(c.network.seed)
			return fmt.Errorf("error on seed %v and tick %v: %v", c.network.seed, i, err)
		}
		if err := CheckElectionSafety(statuses); err != nil {
			c.debugBuff.Dump(c.network.seed)
			return fmt.Errorf("error on seed %v and tick %v: %v", c.network.seed, i, err)
		}
	}
	return nil
}
