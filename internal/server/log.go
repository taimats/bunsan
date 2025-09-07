package server

import (
	"errors"
	"sync"
)

var ErrOffsetNotFound = errors.New("offset not found")

type Log struct {
	mux     sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(r Record) (uint64, error) {
	l.mux.Lock()
	defer l.mux.Unlock()
	r.Offset = uint64(len(l.records))
	l.records = append(l.records, r)
	return r.Offset, nil
}

func (l *Log) Read(offset uint64) (Record, error) {
	l.mux.Lock()
	defer l.mux.Unlock()
	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return l.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}
