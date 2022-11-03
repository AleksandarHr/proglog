package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/aleksandarhr/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// segment wraps the index and store types to coordinate operations
//
//	across the two. For example, when the log appends a record to the
//	active segment, the segment needs to write the data to its store and
//	add a new entry in the index. Similarly for reads, the segment needs
//	to look up the entry from the index and then fetch the data from the store.
type segment struct {
	// keep pointers to segment's store and index
	store *store
	index *index

	// needed to know what offset to append new records under
	//	and to calculate the relative offsets for the index entries
	baseOffset, nextOffset uint64

	// allows to copmare the store file and index sizes to
	//	the configured limits to know when the segment is maxed out
	config Config
}

// newSegment creates a new segment (e.g. when the current active segment hits its max size)
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	// open the store file
	// O_CREATE --> create it if it does not exist)
	// O_APPEND --> make the os append to the file when writing
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// create the store with the store file
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// open the index file (or create it if it does not exist)
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// create the index with the index file
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// set the segment's next offset to prepare for the next appended recrod
	if off, _, err := s.index.Read(-1); err != nil {
		// if the index is empty, the first record and its offset are the segment's base offset
		s.nextOffset = baseOffset
	} else {
		// if the index has at least one entry, next offset would be the sume of
		// the segment's base offset and the relative offset within the segment, + 1
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append writes the record to the segment and returns the newly appended record's offset
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// update the new record's offset information
	currOffset := s.nextOffset
	record.Offset = currOffset

	// use protobuffer to marshal the record to bytes
	bytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// write the record to the store
	_, pos, err := s.store.Append(bytes)
	if err != nil {
		return 0, nil
	}

	// compute the offset relative to the segment's base offset
	// NOTE: both nextOffset and baseOffset are absolute offsets
	relativeOffset := uint32(s.nextOffset - uint64(s.baseOffset))
	// write information about the record to the index
	err = s.index.Write(relativeOffset, pos)
	if err != nil {
		return 0, err
	}

	// increment the segment's next offset
	s.nextOffset++
	return currOffset, nil
}

// Read returns the record for the given offset
func (s *segment) Read(off uint64) (*api.Record, error) {
	// compute relative offset from absolute index
	relativeOffset := int64(off - s.baseOffset)

	// get associated index entry
	_, pos, err := s.index.Read(relativeOffset)
	if err != nil {
		return nil, err
	}

	// read the proper amount of data from the record's position in the store
	bytes, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// use protobuf to unmarshal the bytes into a Record struct
	record := &api.Record{}
	err = proto.Unmarshal(bytes, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size
func (s *segment) IsMaxed() bool {
	// check if segment has written too much to the store
	isStoreMaxed := s.store.size >= s.config.Segment.MaxStoreBytes

	// check if segment has written too much to the store
	isIndexMaxed := s.index.size >= s.config.Segment.MaxIndexBytes
	return isStoreMaxed || isIndexMaxed
}

// Remove closes the segment and removes the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
