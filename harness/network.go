package harness

import (
	"container/heap"
	"math/rand"
	"raft-kv/raft"
)

type Network struct {
	rng   *rand.Rand
	now   int
	queue deliveryHeap
	chaos ChaosConfig
	seqN  uint64
}

func NewNetwork(rng *rand.Rand, chaos ChaosConfig) *Network {
	network := &Network{rng: rng, chaos: chaos}
	network.queue = make(deliveryHeap, 0)
	heap.Init(&network.queue)

	return network
}

func (n *Network) Send(ms ...raft.Message) {

	for _, message := range ms {
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
		messages = append(messages, entry.messages...)
	}
	return messages
}
