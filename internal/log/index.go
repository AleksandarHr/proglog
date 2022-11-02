package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// define the number of bytes that make up each index entry
// each entry ocntains two fields:
//   - the record's offset
//   - the record's position in the store file
//
// Given an entry's offset, we can jump straight
//
//	to its location in in the file by accessing (offset * entWidth)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// index comprises of a persisted file and a memory-mapped file
// size tells us the size of the index and where to write the next entry appended to the index
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// When starting the serivce, it needs to know the offset to set on the
//	next record appended to the log. It learns the next record's offset
//	by looking at the last entry of the index, e.g. simply reading the
//	last 12 bytes of the file. However, this process gets messed up when
//	the files grow in order to memory-map them. The files are grown by
//	appending empty space at the end of them, so the last entry is
//	no longer at the end of the file. Rather, there is some unknown amount
//	of space between this entry and the file's end. This space prevents the
//	the service from restarting properly. For that reason, shut down the
//	the service by truncating the index files to remove this empty space
//	and put the last entry at the end of the file once again. This graceful
//	shutdown returns the service to a state where it can restart properly
//	and efficiently.

// newIndex creates an index for the given file
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// save the current size of the file to track the amount of data
	//	in the index as index entries are added
	idx.size = uint64(fi.Size())

	// grow the file to the max index size
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// memory-map the file and return the created index
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Close() error {

	// sure the memory-mapped file has synced its data to the persisted file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// ensure that the persisted file has flushed its contents to stable storage
	if err := i.file.Sync(); err != nil {
		return err
	}

	// truncate the persisted file to the amount of data that's actually in it
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	// close the file
	return i.file.Close()
}

// Read takes in an offset and returns the associated record's position in the store
// The provided offset is relative to the segment's base offset.
// 0 is always the offset of the index's first entry, 1 is the second, etc.
// Using relative indices reduces the size of the indexes (offsets are uint32s)
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// if the index is empty
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index
func (i *index) Write(off uint32, pos uint64) error {
	// validate that there is space to write the entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// encode the offset and write it to the memory-mapped file
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	// encode the position and write it to the memory-mapped file
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// increment the position where the next write will go
	i.size += uint64(entWidth)
	return nil
}

// Name returns the index's file path
func (i *index) Name() string {
	return i.file.Name()
}
