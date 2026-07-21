package harness

import (
	"math/rand"
	"raft-kv/raft"
)

type Cluster struct {
	ids          []int
	nodes        map[int]*raft.Core
	network      *Network
	committedLog CommittedLog
}

func NewCluster(ids []int, rng *rand.Rand, chaosConfig ChaosConfig) Cluster {
	cluster := Cluster{
		ids:          ids,
		network:      NewNetwork(rng, chaosConfig),
		committedLog: NewCommittedLog(),
		nodes:        make(map[int]*raft.Core),
	}
	for _, id := range ids {
		cluster.nodes[id] = raft.NewCore(id, ids, 10, 20, rng, 3)
	}
	return cluster
}

func (c Cluster) Run(tick int) error {
	for i := 0; i < tick; i++ {
		for _, id := range c.ids {
			node := c.nodes[id]
			node.Tick()
			c.network.Send(node.Ready()...)
		}
		messages := c.network.Advance()
		for _, msg := range messages {
			c.nodes[msg.ToId].Step(msg)
		}

		statuses := make([]raft.Status, 0)
		for _, id := range c.ids {
			status := c.nodes[id].Status()
			statuses = append(statuses, status)
		}

		if err := c.committedLog.Merge(statuses); err != nil {
			return err
		}
		if err := c.committedLog.CheckLeaderCompleteness(statuses); err != nil {
			return err
		}
		if err := CheckLogMatching(statuses); err != nil {
			return err
		}
		if err := CheckElectionSafety(statuses); err != nil {
			return err
		}
	}
	return nil
}
