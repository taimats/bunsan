package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	logv1 "github.com/taimats/bunsan/gen/log/v1"
)

var defaultMaxBytes uint64 = 1024

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSeg *segment
	segments  []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = defaultMaxBytes
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = defaultMaxBytes
	}
	return &Log{Dir: dir, Config: c}, nil
}

func (l *Log) setup() error {
	fs, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	baseOffsets := make([]uint64, len(fs))
	for _, f := range fs {
		str := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
		off, err := strconv.ParseUint(str, 10, 0)
		if err != nil {
			return err
		}
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i += 2 {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSeg = s
	return nil
}

func (l *Log) Append(r *logv1.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	highestOffset, err := l.highestOffset()
	if err != nil {
		return 0, err
	}

	if l.activeSeg.isMaxed() {
		if err = l.newSegment(highestOffset + 1); err != nil {
			return 0, err
		}
	}

	off, err := l.activeSeg.Append(r)
	if err != nil {
		return 0, err
	}
	return off, nil
}

func (l *Log) Read(off uint64) (*logv1.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var seg *segment
	for _, s := range l.segments {
		if s.baseOffset <= off && off < s.nextOffset {
			seg = s
			break
		}
	}
	return seg.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) Truncate(lowestOffset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segs []*segment
	for _, s := range l.segments {
		if s.nextOffset < lowestOffset {
			if err := s.Remove(); err != nil {
				return err
			}
		}
		segs = append(segs, s)
	}
	l.segments = segs
	return nil
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.highestOffset()
}

func (l *Log) highestOffset() (uint64, error) {
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for _, s := range l.segments {
		readers = append(readers, newOriginReader(s.store, 0))
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func newOriginReader(s *store, off int64) *originReader {
	return &originReader{
		store: s,
		off:   off,
	}
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
