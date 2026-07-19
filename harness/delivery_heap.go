package harness

import (
	"raft-kv/raft"
)

type deliveryEntry struct {
	deliverAt int
	seqN      uint64
	messages  []raft.Message
}

type deliveryHeap []deliveryEntry

func (h deliveryHeap) Len() int {
	return len(h)
}

func (h deliveryHeap) Less(i, j int) bool {
	if h[i].deliverAt == h[j].deliverAt {
		return h[i].seqN < h[j].seqN
	}
	return h[i].deliverAt < h[j].deliverAt
}

func (h deliveryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *deliveryHeap) Push(x any) {
	*h = append(*h, x.(deliveryEntry))
}

func (h *deliveryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
