package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("Hello, world!")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// create a store with a temp file
	s, err := newStore(f)
	require.NoError(t, err)

	// test append and read from store
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// create the store again using the same temp file
	s, err = newStore(f)
	require.NoError(t, err)
	// verify the service will recover its state after a restart (e.g. still contains/reads the data)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	// write the test string 3 times
	for i := uint64(1); i < 4; i++ {
		// append to store
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		// pos where record was appended + number of bytes written
		//	should reflect the total length of the store so far
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		// read at certain positoin
		read, err := s.Read(pos)
		require.NoError(t, err)
		// check if bytes read are the same as the test bytes
		require.Equal(t, write, read)
		// move position by one record length + the lenWidth used to write the length
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// read the record size
		bytes := make([]byte, lenWidth)
		n, err := s.ReadAt(bytes, off)
		require.NoError(t, err)
		// ensure bytes read are lenWidth
		require.Equal(t, lenWidth, n)
		// shift position as needed to actual record
		off += int64(n)

		// read in the actual record starting at off offset
		size := enc.Uint64(bytes)
		bytes = make([]byte, size)
		n, err = s.ReadAt(bytes, off)
		require.NoError(t, err)
		require.Equal(t, write, bytes)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
