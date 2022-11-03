package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// the encoding to persist record sizes and index entries in
var (
	enc = binary.BigEndian
)

// the number of bytes used to store the record's length
const (
	lenWidth = 8
)

// store is a wrapper around a file
type store struct {
	File *os.File
	mu   sync.Mutex
	size uint64
	buf  *bufio.Writer
}

// newStore creates a new store with the provided file
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	// error with the file provided, cannot create a store
	if err != nil {
		return nil, err
	}

	// create a store with the file provided and return it
	size := uint64(fi.Size()) // in case recreating a store from a file with existing data
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists the given bytes to the store
func (s *store) Append(toPersist []byte) (bytesWritten uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// get current position where the bytes will be persisted
	pos = s.size

	// write the length of the record so we know how many bytes to read later
	if err := binary.Write(s.buf, enc, uint64(len(toPersist))); err != nil {
		return 0, 0, err
	}

	// write to the buffered writer to reduce system calls and improve performance
	numBytes, err := s.buf.Write(toPersist)
	if err != nil {
		return 0, 0, err
	}

	numBytes += lenWidth
	s.size += uint64(numBytes)
	// return the number of bytes written
	// and the position where the store holds the record (to be used for indexing)
	// At pos --> record_length (8 bytes) followed by the record data itself
	return uint64(numBytes), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// make sure to flush the writer buffer so all records are on disk
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// find out how many bytes we need to read to get the whole record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// fetch the record of size 'size'
	bytes := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(bytes, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return bytes, nil
}

// ReadAt reads len(p) bytes into p beginning at the off offset in the store's file
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close persists any buffered data before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}

// Name returns the store's file path
func (s *store) Name() string {
	return s.File.Name()
}
