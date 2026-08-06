package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const segmentSuffix = ".wal"

type segmentSet struct {
	dir      string
	maxBytes int64

	active     *os.File
	activeID   int
	activeSize int64
}

func newSegmentSet(dir string, maxBytes int64) (*segmentSet, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ids, err := listSegmentIDs(dir)
	if err != nil {
		return nil, err
	}

	s := &segmentSet{dir: dir, maxBytes: maxBytes}

	nextID := 1
	if len(ids) > 0 {
		nextID = ids[len(ids)-1]
	}
	if err := s.openActive(nextID); err != nil {
		return nil, err
	}
	return s, nil
}

func segmentName(id int) string {
	return fmt.Sprintf("%08d%s", id, segmentSuffix)
}

func listSegmentIDs(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []int
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentSuffix) {
			continue
		}
		idStr := strings.TrimSuffix(e.Name(), segmentSuffix)
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids, nil
}

func (s *segmentSet) openActive(id int) error {
	path := filepath.Join(s.dir, segmentName(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	s.active = f
	s.activeID = id
	s.activeSize = info.Size()
	return nil
}

// append writes payload as a framed record to the active segment, rolling
// over to a new segment first if it would push the active one past
// maxBytes. A record is never split across segments.
func (s *segmentSet) append(payload []byte) error {
	frame := encodeRecord(payload)
	if s.activeSize > 0 && s.activeSize+int64(len(frame)) > s.maxBytes {
		if err := s.roll(); err != nil {
			return err
		}
	}
	n, err := s.active.Write(frame)
	s.activeSize += int64(n)
	return err
}

func (s *segmentSet) roll() error {
	if err := s.active.Close(); err != nil {
		return err
	}
	return s.openActive(s.activeID + 1)
}

func (s *segmentSet) sync() error {
	return s.active.Sync()
}

func (s *segmentSet) close() error {
	return s.active.Close()
}

// list returns segment filenames in replay order.
func (s *segmentSet) list() ([]string, error) {
	ids, err := listSegmentIDs(s.dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(ids))
	for i, id := range ids {
		names[i] = segmentName(id)
	}
	return names, nil
}

// replay walks every segment in order, decoding each record and passing its
// payload to fn. It stops at the first clean EOF or torn record — either one
// just means "this is where the log ends," not a fatal error.
func (s *segmentSet) replay(fn func(payload []byte) error) error {
	names, err := s.list()
	if err != nil {
		return err
	}
	for _, name := range names {
		stop, err := replaySegmentFile(filepath.Join(s.dir, name), fn)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
	return nil
}

func replaySegmentFile(path string, fn func(payload []byte) error) (stop bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	for {
		payload, err := decodeRecord(f)
		if err == io.EOF {
			return false, nil
		}
		if err == ErrTornRecord {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if err := fn(payload); err != nil {
			return false, err
		}
	}
}
