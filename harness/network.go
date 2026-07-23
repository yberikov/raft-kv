package harness

import (
	"container/heap"
	"math/rand"
	"raft-kv/raft"
)

type Network struct {
	seed      int64
	rng       *rand.Rand
	now       int
	queue     deliveryHeap
	chaos     ChaosConfig
	seqN      uint64
	partition map[int]int
}

func NewNetwork(seed int64, rng *rand.Rand, chaos ChaosConfig) *Network {
	network := &Network{seed: seed, rng: rng, chaos: chaos}
	network.queue = make(deliveryHeap, 0)
	network.partition = make(map[int]int)
	heap.Init(&network.queue)

	return network
}

func (n *Network) Send(ms ...raft.Message) {

	for _, message := range ms {
		if n.GetGroupID(message.FromId) != n.GetGroupID(message.ToId) {
			continue
		}
		delay := n.chaos.MinDelay + n.rng.Intn(n.chaos.MaxDelay-n.chaos.MinDelay+1)
		if n.rng.Float64() > n.chaos.DropP {
			heap.Push(&n.queue, deliveryEntry{deliverAt: n.now + delay, messages: []raft.Message{message}, seqN: n.seqN})
			if n.rng.Float64() < n.chaos.DuplicateP {
				heap.Push(&n.queue, deliveryEntry{deliverAt: n.now + delay, messages: []raft.Message{message}, seqN: n.seqN})
			}
		}
		n.seqN++
	}
}

func (n *Network) Advance() []raft.Message {
	n.now++
	messages := make([]raft.Message, 0)
	for len(n.queue) > 0 && n.queue[0].deliverAt <= n.now {
		entry := heap.Pop(&n.queue).(deliveryEntry)
		for _, message := range entry.messages {
			if n.GetGroupID(message.FromId) != n.GetGroupID(message.ToId) {
				continue
			}
			messages = append(messages, message)
		}
	}
	return messages
}

func (n *Network) Partition(groups [][]int) {
	for groupIdx, group := range groups {
		for _, node := range group {
			n.partition[node] = groupIdx
		}
	}
}

func (n *Network) Heal() {
	for nodeId := range n.partition {
		n.partition[nodeId] = -1
	}
}

func (n *Network) GetGroupID(id int) int {
	if _, ok := n.partition[id]; !ok {
		return -1
	}
	return n.partition[id]
}
