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

func (s *store) Read() {

}

func (s *store) Append() {

}
