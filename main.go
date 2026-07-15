package main

import (
	"math/rand"
	"raft-kv/raft"
	"time"
)

func main() {
	rng := rand.New(rand.NewSource(1))
	core := raft.NewCore(1, []uint64{1, 2, 3}, 10, 40, rng, 5)
	ticker := time.NewTicker(time.Millisecond * 10)
	for {
		select {
		case <-ticker.C:
			core.Tick()
		}
	}
}
