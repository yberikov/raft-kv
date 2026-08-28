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
	recordKindSnapshot
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

type snapshotRecord struct {
	Index int
	Term  uint64
	Data  any
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

func encodeSnapshotRecord(index int, term uint64, data any) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(recordKindSnapshot)
	err := gob.NewEncoder(&buf).Encode(snapshotRecord{Index: index, Term: term, Data: data})
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
	case recordKindSnapshot:
		var v snapshotRecord
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

type WAL struct {
	mu       sync.Mutex
	segments *segmentSet

	log      []raft.Entry
	term     uint64
	votedFor int
	durable  int
	pending  []pendingFsyncBatch

	startIndex        int
	startTerm         uint64
	snapshotData      any
	snapshotSegmentID int

	n int
	m time.Duration

	ticker *time.Ticker
	stop   chan struct{}
	done   chan struct{}

	syncErr error
}

func NewWAL(dir string, n int, m time.Duration) (*WAL, error) {
	return newWAL(dir, n, m, defaultSegmentMaxBytes)
}

// newWAL is NewWAL with an explicit segment size, so tests can force
// multiple segments without writing 64MB of data.
func newWAL(dir string, n int, m time.Duration, segmentMaxBytes int64) (*WAL, error) {
	segs, err := newSegmentSet(dir, segmentMaxBytes)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		segments: segs,
		log:      []raft.Entry{{Term: 0}},
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
	w.durable = w.lastIndex()

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
		w.log = w.log[:v.Index-w.startIndex]
	case recordKindSnapshot:

		v := value.(snapshotRecord)
		w.log = []raft.Entry{{Term: v.Term}}
		w.startIndex = v.Index
		w.startTerm = v.Term
		w.snapshotData = v.Data
	}
}

// lastIndex returns the logical index of the last entry in w.log. Callers
// must hold w.mu.
func (w *WAL) lastIndex() int {
	return w.startIndex + len(w.log) - 1
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

func (w *WAL) persistStateLocked(term uint64, votedFor int) error {
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
func (w *WAL) PersistState(term uint64, votedFor int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.persistStateLocked(term, votedFor)
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
	w.pending = append(w.pending, pendingFsyncBatch{index: w.lastIndex(), since: time.Now()})

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
	return append([]raft.Entry(nil), w.log[lo-w.startIndex:hi-w.startIndex]...)
}

func (w *WAL) LastIndex() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex()
}

func (w *WAL) TermAt(index int) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.log[index-w.startIndex].Term
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

	w.log = w.log[:index-w.startIndex]
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

func (w *WAL) PersistSnapshot(index int, term uint64, data any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return w.syncErr
	}

	payload, err := encodeSnapshotRecord(index, term, data)
	if err != nil {
		return err
	}
	if err := w.segments.append(payload); err != nil {
		w.syncErr = err
		return err
	}
	snapshotSegmentID := w.segments.activeID

	var keep []raft.Entry
	if pos := index - w.startIndex; pos >= 0 && pos < len(w.log) {
		// The snapshot boundary falls within our current log — keep the
		// consistent suffix beyond it, same as CompactLog.
		keep = append([]raft.Entry(nil), w.log[pos:]...)
		keep[0] = raft.Entry{Term: term}
	} else {
		// We don't have entries out to index at all (e.g. this node just
		// crash-recovered and fell far behind) — the whole log is obsolete.
		keep = []raft.Entry{{Term: term}}
	}

	if len(keep) > 1 {
		survivors := append([]raft.Entry(nil), keep[1:]...)
		entriesPayload, err := encodeEntriesRecord(survivors)
		if err != nil {
			return err
		}
		if err := w.segments.append(entriesPayload); err != nil {
			w.syncErr = err
			return err
		}
	}

	if err := w.segments.sync(); err != nil {
		w.syncErr = err
		return err
	}

	w.log = keep
	w.startIndex = index
	w.startTerm = term
	w.snapshotData = data
	w.snapshotSegmentID = snapshotSegmentID

	if w.durable < index {
		w.durable = index
	}
	kept := w.pending[:0]
	for _, p := range w.pending {
		if p.index > index {
			kept = append(kept, p)
		}
	}
	w.pending = kept

	return nil
}

func (w *WAL) SnapshotState() (index int, term uint64, data any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.startIndex, w.startTerm, w.snapshotData
}

func (w *WAL) ReclaimSegments() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.snapshotSegmentID == 0 {
		return nil
	}

	if err := w.persistStateLocked(w.term, w.votedFor); err != nil {
		return err
	}

	return w.segments.deleteBefore(w.snapshotSegmentID)
}
func (w *WAL) Close() error {
	close(w.stop)
	<-w.done
	w.ticker.Stop()

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segments.close()
}
