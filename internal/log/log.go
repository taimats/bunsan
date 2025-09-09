package log

import (
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
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
