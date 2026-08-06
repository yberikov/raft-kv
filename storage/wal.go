package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"raft-kv/raft"
)

const defaultSegmentMaxBytes = 64 << 20 // 64MB

const (
	recordKindState byte = iota + 1
	recordKindEntries
	recordKindTruncate
)

type stateRecord struct {
	Term     uint64
	VotedFor int
}

type entriesRecord struct {
	Entries []raft.Entry
}

type truncateRecord struct {
	Index int
}

func init() {
	gob.Register("")
}

func encodeStateRecord(term uint64, votedFor int) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(recordKindState)
	err := gob.NewEncoder(&buf).Encode(stateRecord{Term: term, VotedFor: votedFor})
	return buf.Bytes(), err
}

func encodeEntriesRecord(entries []raft.Entry) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(recordKindEntries)
	err := gob.NewEncoder(&buf).Encode(entriesRecord{Entries: entries})
	return buf.Bytes(), err
}

func encodeTruncateRecord(index int) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(recordKindTruncate)
	err := gob.NewEncoder(&buf).Encode(truncateRecord{Index: index})
	return buf.Bytes(), err
}

func decodeWALRecord(payload []byte) (kind byte, value any, err error) {
	if len(payload) == 0 {
		return 0, nil, fmt.Errorf("storage: empty WAL record")
	}
	kind = payload[0]
	dec := gob.NewDecoder(bytes.NewReader(payload[1:]))
	switch kind {
	case recordKindState:
		var v stateRecord
		if err := dec.Decode(&v); err != nil {
			return 0, nil, err
		}
		return kind, v, nil
	case recordKindEntries:
		var v entriesRecord
		if err := dec.Decode(&v); err != nil {
			return 0, nil, err
		}
		return kind, v, nil
	case recordKindTruncate:
		var v truncateRecord
		if err := dec.Decode(&v); err != nil {
			return 0, nil, err
		}
		return kind, v, nil
	default:
		return 0, nil, fmt.Errorf("storage: unknown WAL record kind %d", kind)
	}
}

type pendingFsyncBatch struct {
	index int
	since time.Time
}

// WAL is a disk-backed Storage implementation: a segmentSet on disk plus an
// in-memory mirror kept in sync with every mutation. Entries fsync on an
// N-count-or-M-duration group-commit policy; state changes fsync
// immediately, since they're rare and a lost vote is a correctness issue,
// not just a latency one.
type WAL struct {
	mu       sync.Mutex
	segments *segmentSet

	log      []raft.Entry
	term     uint64
	votedFor int
	durable  int
	pending  []pendingFsyncBatch

	n int
	m time.Duration

	ticker *time.Ticker
	stop   chan struct{}
	done   chan struct{}

	syncErr error
}

func NewWAL(dir string, n int, m time.Duration) (*WAL, error) {
	segs, err := newSegmentSet(dir, defaultSegmentMaxBytes)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		segments: segs,
		log:      []raft.Entry{{Cmd: nil, Term: 0}},
		n:        n,
		m:        m,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	if err := segs.replay(func(payload []byte) error {
		kind, value, err := decodeWALRecord(payload)
		if err != nil {
			return err
		}
		w.applyRecord(kind, value)
		return nil
	}); err != nil {
		segs.close()
		return nil, err
	}
	w.durable = len(w.log) - 1

	w.ticker = time.NewTicker(m / 2)
	go w.run()

	return w, nil
}

func (w *WAL) applyRecord(kind byte, value any) {
	switch kind {
	case recordKindState:
		v := value.(stateRecord)
		w.term = v.Term
		w.votedFor = v.VotedFor
	case recordKindEntries:
		v := value.(entriesRecord)
		w.log = append(w.log, v.Entries...)
	case recordKindTruncate:
		v := value.(truncateRecord)
		w.log = w.log[:v.Index]
	}
}

func (w *WAL) run() {
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			return
		case <-w.ticker.C:
			w.mu.Lock()
			if w.syncErr == nil && len(w.pending) > 0 && time.Since(w.pending[0].since) >= w.m {
				w.syncLocked()
			}
			w.mu.Unlock()
		}
	}
}

func (w *WAL) syncLocked() {
	if err := w.segments.sync(); err != nil {
		w.syncErr = err
		return
	}
	w.durable = w.pending[len(w.pending)-1].index
	w.pending = w.pending[:0]
}

func (w *WAL) PersistState(term uint64, votedFor int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return w.syncErr
	}

	payload, err := encodeStateRecord(term, votedFor)
	if err != nil {
		return err
	}
	if err := w.segments.append(payload); err != nil {
		w.syncErr = err
		return err
	}
	if err := w.segments.sync(); err != nil {
		w.syncErr = err
		return err
	}

	w.term = term
	w.votedFor = votedFor
	return nil
}

func (w *WAL) AppendEntries(entries []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return w.syncErr
	}

	payload, err := encodeEntriesRecord(entries)
	if err != nil {
		return err
	}
	if err := w.segments.append(payload); err != nil {
		w.syncErr = err
		return err
	}

	w.log = append(w.log, entries...)
	w.pending = append(w.pending, pendingFsyncBatch{index: len(w.log) - 1, since: time.Now()})

	if len(w.pending) >= w.n {
		w.syncLocked()
	}
	return w.syncErr
}

func (w *WAL) LastFsyncedIndex() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.durable
}

func (w *WAL) Entries(lo, hi int) []raft.Entry {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]raft.Entry(nil), w.log[lo:hi]...)
}

func (w *WAL) LastIndex() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.log) - 1
}

func (w *WAL) TermAt(index int) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.log[index].Term
}

func (w *WAL) TruncateFrom(index int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return w.syncErr
	}

	payload, err := encodeTruncateRecord(index)
	if err != nil {
		return err
	}
	if err := w.segments.append(payload); err != nil {
		w.syncErr = err
		return err
	}

	w.log = w.log[:index]
	if w.durable > index-1 {
		w.durable = index - 1
	}
	kept := w.pending[:0]
	for _, p := range w.pending {
		if p.index < index {
			kept = append(kept, p)
		}
	}
	w.pending = kept
	return nil
}

func (w *WAL) Tick() {}

func (w *WAL) State() (term uint64, votedFor int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.term, w.votedFor
}

func (w *WAL) Close() error {
	close(w.stop)
	<-w.done
	w.ticker.Stop()

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segments.close()
}
