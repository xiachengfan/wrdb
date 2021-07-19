package wrdbserver

import (
	"encoding/binary"
	"github.com/hashicorp/raft"
	"sync"
	"wrdb/pkg/fileutil"
	"wrdb/wal"
)

type Options struct {
	// Path is the file path to the BoltDB to use
	Path string

	// BoltOptions contains any specific BoltDB options you might
	// want to specify [e.g. open timeout]
	WalOptions *wal.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool
}

type Wal struct {
	mu    sync.Mutex
	log   *wal.Log
	buf   []byte
	batch wal.Batch
}

var _ raft.LogStore = &Wal{}

func NewBoltWal(path string) (*Wal, error) {

	return NewWal(Options{Path: path})
}

func NewWal(options Options) (*Wal, error) {
	s := new(Wal)
	var err error
	options.WalOptions.LogFormat = wal.JSON
	if fileutil.Exist(options.Path) {
		s.log, err = wal.Open(options.Path, options.WalOptions)
		if err != nil {
			return nil, err
		}
	} else {
		s.log, err = wal.Create(options.Path, options.WalOptions)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Wal) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}

func (s *Wal) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.FirstIndex()
}

func (s *Wal) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.LastIndex()
}

func (s *Wal) GetLog(index uint64, log *raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.log.Read(index)
	if err != nil {
		if err == wal.ErrNotFound {
			return raft.ErrLogNotFound
		}
		return err
	}
	log.Index = index
	if len(data) == 0 {
		return wal.ErrCorrupt
	}
	log.Type = raft.LogType(data[0])
	data = data[1:]
	var n int
	log.Term, n = binary.Uvarint(data)
	if n <= 0 {
		return wal.ErrCorrupt
	}
	data = data[n:]
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return wal.ErrCorrupt
	}
	data = data[n:]
	if uint64(len(data)) < size {
		return wal.ErrCorrupt
	}
	log.Data = data[:size]
	data = data[size:]
	size, n = binary.Uvarint(data)
	if n <= 0 {
		return wal.ErrCorrupt
	}
	data = data[n:]
	if uint64(len(data)) < size {
		return wal.ErrCorrupt
	}
	log.Extensions = data[:size]
	data = data[size:]
	if len(data) > 0 {
		return wal.ErrCorrupt
	}
	return nil
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

func appendLog(dst []byte, log *raft.Log) []byte {
	dst = append(dst, byte(log.Type))
	dst = appendUvarint(dst, log.Term)
	dst = appendUvarint(dst, uint64(len(log.Data)))
	dst = append(dst, log.Data...)
	dst = appendUvarint(dst, uint64(len(log.Extensions)))
	dst = append(dst, log.Extensions...)
	return dst
}

// StoreLog is used to store a single raft log
func (s *Wal) StoreLog(log *raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = s.buf[:0]
	s.buf = appendLog(s.buf, log)
	return s.log.Write(log.Index, s.buf)
}

// StoreLogs is used to store a set of raft logs
func (s *Wal) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batch.Clear()
	for _, log := range logs {
		s.buf = s.buf[:0]
		s.buf = appendLog(s.buf, log)
		s.batch.Write(log.Index, s.buf)
	}
	return s.log.WriteBatch(&s.batch)
}

// DeleteRange is used to delete logs within a given range inclusively.
func (s *Wal) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	first, err := s.log.FirstIndex()
	if err != nil {
		return err
	}
	last, err := s.log.LastIndex()
	if err != nil {
		return err
	}
	if min == first {
		if err := s.log.TruncateFront(max + 1); err != nil {
			return err
		}
	} else if max == last {
		if err := s.log.TruncateBack(min - 1); err != nil {
			return err
		}
	} else {
		return wal.ErrOutOfRange
	}
	return nil
}

// Sync performs an fsync on the log. This is not necessary when the
// durability is set to High.
func (s *Wal) Sync() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.Sync()
}
