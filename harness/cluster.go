package harness

import (
	"fmt"
	"math/rand"
	"raft-kv/raft"
	"raft-kv/storage"
)

type Cluster struct {
	ids          []int
	nodes        map[int]*raft.Core
	network      *Network
	storages     map[int]storage.Storage
	outbox       map[int][]pendingBatch
	committedLog CommittedLog
	debugBuff    RingBuffer

	CompactThreshold int
}

type pendingBatch struct {
	requiredIndex int
	messages      []raft.Message
}

func NewCluster(ids []int, seed int64, rng *rand.Rand, chaosConfig ChaosConfig) Cluster {
	cluster := Cluster{
		ids:          ids,
		network:      NewNetwork(seed, rng, chaosConfig),
		storages:     make(map[int]storage.Storage),
		outbox:       make(map[int][]pendingBatch),
		committedLog: NewCommittedLog(),
		nodes:        make(map[int]*raft.Core),
		debugBuff:    NewRingBuffer(50),
	}

	for _, id := range ids {
		cluster.nodes[id] = raft.NewCore(id, ids, 10, 20, rng, 3)
		cluster.storages[id] = storage.NewInMemoryStorage(3)
	}
	return cluster
}

func (c *Cluster) Restart(id int) {
	st := c.storages[id]
	term, votedFor := st.State()
	snapIndex, snapTerm, snapData := st.SnapshotState()
	log := st.Entries(snapIndex, st.LastIndex()+1)

	node := raft.NewCore(id, c.ids, 10, 20, c.network.rng, 3)
	node.Restore(term, votedFor, log, snapIndex, snapTerm, snapData)
	c.nodes[id] = node
	c.outbox[id] = nil
}

func (c *Cluster) Crash(id int) {
	st := c.storages[id]
	durable := st.LastFsyncedIndex()
	st.TruncateFrom(durable + 1)
	c.Restart(id)
}

func (c *Cluster) Run(tick int) error {
	for i := 0; i < tick; i++ {
		for _, id := range c.ids {
			node := c.nodes[id]
			node.Tick()
			c.storages[id].Tick()
			ready := node.Ready()
			if ready.StateChanged {
				c.storages[id].PersistState(ready.Term, ready.VotedFor)
			}
			if ready.TruncateFrom != 0 {
				c.storages[id].TruncateFrom(ready.TruncateFrom)
			}
			if ready.SnapshotIndex != 0 {
				c.storages[id].PersistSnapshot(ready.SnapshotIndex, ready.SnapshotTerm, ready.SnapshotData)
				c.storages[id].ReclaimSegments()
			}
			if len(ready.EntriesToPersist) > 0 {
				c.storages[id].AppendEntries(ready.EntriesToPersist)
			}
			if len(ready.Messages) > 0 {
				requiredIndex := 0
				if len(ready.EntriesToPersist) > 0 {
					requiredIndex = c.storages[id].LastIndex()
				}
				c.outbox[id] = append(c.outbox[id], pendingBatch{requiredIndex: requiredIndex, messages: ready.Messages})
			}

			durable := c.storages[id].LastFsyncedIndex()
			for len(c.outbox[id]) > 0 && c.outbox[id][0].requiredIndex <= durable {
				c.network.Send(c.outbox[id][0].messages...)
				c.outbox[id] = c.outbox[id][1:]
			}

			if c.CompactThreshold > 0 {
				status := node.Status()
				if status.CommitIndex-status.StartIndex >= c.CompactThreshold {
					idx := status.CommitIndex
					term := status.Log[idx-status.StartIndex].Term
					node.CompactLog(idx, term, fmt.Sprintf("snapshot@%d", idx))
				}
			}
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
