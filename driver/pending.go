package driver

import (
	"context"
	"raft-kv/kv"
)

type Pending struct {
	term uint64
	ch   chan kv.Result
}

type pendingSet struct {
	waiters map[int]*Pending
}

func (p *Pending) Poll() (kv.Result, bool) {
	select {
	case result := <-p.ch:
		return result, true
	default:
		return kv.Result{}, false
	}
}

func (p *Pending) Wait(ctx context.Context) (kv.Result, error) {
	select {
	case res := <-p.ch:
		return res, nil
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
}

func (s *pendingSet) add(index int, term uint64) *Pending {
	pending := Pending{
		term: term,
		ch:   make(chan kv.Result, 1),
	}
	s.waiters[index] = &pending
	return &pending
}

func (s *pendingSet) resolve(index int, term uint64, result kv.Result) {
	pending, ok := s.waiters[index]
	if !ok {
		return
	}
	if pending.term != term {
		result = kv.Result{Err: "term mismatch"}
	}

	pending.ch <- result
	delete(s.waiters, index)
}

func (s *pendingSet) failUpTo(index int, err string) {
	s.failIf(err, func(i int) bool { return i <= index })
}

func (s *pendingSet) failFrom(index int, err string) {
	s.failIf(err, func(i int) bool { return i >= index })
}

func (s *pendingSet) failAll(err string) {
	s.failIf(err, func(index int) bool { return true })
}

func (s *pendingSet) failIf(err string, match func(index int) bool) {
	for index, p := range s.waiters {
		if match(index) {
			result := kv.Result{
				Err: err,
			}
			p.ch <- result
			delete(s.waiters, index)
		}

	}
}
