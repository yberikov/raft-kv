package harness

import (
	"fmt"
	"os"
	"raft-kv/raft"
)

type RingBuffer struct {
	head *Node
	tail *Node
	size int
	cap  int
}

type Node struct {
	tick    int
	message raft.Message
	next    *Node
}

func (n Node) ToString() string {
	return fmt.Sprintf("%+v", n)
}

func NewRingBuffer(cap int) RingBuffer {
	return RingBuffer{cap: cap}
}

func (r *RingBuffer) Push(tick int, message raft.Message) {
	if r.head == nil {
		r.head = &Node{tick: tick, message: message}
		r.tail = r.head
		r.size++
		return
	}
	r.tail.next = &Node{tick: tick, message: message}
	r.tail = r.tail.next
	r.size++
	if r.size > r.cap {
		r.head = r.head.next
		r.size--
	}
}

func (r *RingBuffer) Dump(seed int64) error {
	name := fmt.Sprintf("log-%d.txt", seed)
	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for node := r.head; node != nil; node = node.next {
		file.Write([]byte(node.ToString()))
	}

	return nil
}
