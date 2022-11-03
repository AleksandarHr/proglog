package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/aleksandarhr/proglog/api/v1"
)

// Log manages the list of segments
type Log struct {
	mu sync.RWMutex

	// Dir is where segments are stored
	Dir    string
	Config Config

	// pointer to the currently active segments, to append writes to
	activeSegment *segment
	// a list of segments
	segments []*segment
}

// NewLog sets the default for the configs, if not specified, and sets up a log instance
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	lg := &Log{
		Dir:    dir,
		Config: c,
	}

	return lg, lg.setup()
}

// setup a log instance
func (lg *Log) setup() error {
	// on startup, the log is responsible for setting itself up for the
	//	segments that already exist on disk or, if the log is new and
	//	has no existing segments, for bootstrapping the initial segment.

	// read the segment files
	files, err := ioutil.ReadDir(lg.Dir)
	if err != nil {
		return err
	}

	// parse the base offsets info from the name of each segment
	var baseOffsets []uint64
	for _, file := range files {
		offsetString := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)

		off, _ := strconv.ParseUint(offsetString, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// sort the base offsets in ascending order (e.g. segments are
	//	in order from oldest to newest)
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// create the segments with newSegment() helper function
	// baseOffsets contains an information for index and store,
	//	so increment i by two to skip over the duplicate information
	for i := 0; i < len(baseOffsets); i += 2 {
		if err = lg.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}

	if lg.segments == nil {
		if err = lg.newSegment(lg.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

// Append appends a record to the log
func (lg *Log) Append(record *api.Record) (uint64, error) {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	// append the record to the active segment
	offset, err := lg.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	// if the segment is at its max size, create a new active segment
	if lg.activeSegment.IsMaxed() {
		err = lg.newSegment(offset + 1)
	}
	return offset, err
}

// Read reads the record stored at the given offset
func (lg *Log) Read(offset uint64) (*api.Record, error) {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	var s *segment
	// given the absolute offset of the record to be read, find the relevant segment
	for _, segment := range lg.segments {
		if segment.baseOffset <= offset && offset < segment.nextOffset {
			s = segment
		}
	}

	if s == nil || s.nextOffset <= offset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}

	// get the index entry form the segment's index and read the
	//	data out of the segment's store file and return it
	return s.Read(offset)
}

// Close iterates over the segments and closes them
func (lg *Log) Close() error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	for _, segment := range lg.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Remove closes the log and removes its data
func (lg *Log) Remove() error {
	if err := lg.Close(); err != nil {
		return err
	}
	return os.RemoveAll(lg.Dir)
}

// Reset remvoes the log and creates a new log in its places
func (lg *Log) Reset() error {
	if err := lg.Remove(); err != nil {
		return err
	}
	return lg.setup()
}

// LowestOffset returns the lower bound of the offset range of the log
func (lg *Log) LowestOffset() (uint64, error) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	return lg.segments[0].baseOffset, nil
}

// LowestOffset returns the upper bound of the offset range of the log
func (lg *Log) HighestOffset() (uint64, error) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	offset := lg.segments[len(lg.segments)-1].nextOffset
	if offset == 0 {
		return 0, nil
	}

	return offset - 1, nil
}

// Truncate removes all segments whose highest offset is lower than lowest
// We will periodically call Truncate to remove old segments
// whose data have (hopefully) been processed by then a is not needed anymore
func (lg *Log) Truncate(lowest uint64) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	var segments []*segment
	for _, s := range lg.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	lg.segments = segments
	return nil
}

// Reader returns an io.Reader to read the whole log
// This is needed when implementing coordinate consensus and is also needed
// to support snapshots and restoring a log
func (lg *Log) Reader() io.Reader {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	readers := make([]io.Reader, len(lg.segments))
	for i, segment := range lg.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	// use io.MultiReader to concatenate the segment's stores
	return io.MultiReader(readers...)
}

// originReader wraps the segment store
// It satisfies the io.Reader interface, so it can be passed into the io.MultiReader() call
// It ensures that reading starts from the origin of the store and the entire file is read
type originReader struct {
	store  *store
	offset int64
}

func (o *originReader) Read(bytes []byte) (int, error) {
	nBytes, err := o.store.ReadAt(bytes, o.offset)
	o.offset += int64(nBytes)
	return nBytes, err
}

// newSegment creates a new segment, appends it to the log's slice of
// segments, and makes the new segment the active one so that subsequent
// append calls write to it
func (lg *Log) newSegment(offset uint64) error {
	seg, err := newSegment(lg.Dir, offset, lg.Config)
	if err != nil {
		return err
	}
	lg.segments = append(lg.segments, seg)
	lg.activeSegment = seg
	return nil
}
