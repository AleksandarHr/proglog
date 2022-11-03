package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "github.com/aleksandarhr/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

// testAppendRead tests that a record has been successfully appended to
// and read from the log
func testAppendRead(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	offset, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	read, err := log.Read(offset)
	require.NoError(t, err)
	require.Equal(t, read.Value, rec.Value)
}

// testOufOfRangeErr tests that the log returns an error when
// tried to read an offset which is outside of the range of offsets
// the log has stored so far
func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
}

// testInitExisting tests that when a log is create, it bootstraps
// itself from the data stored by prior log instances
func testInitExisting(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}

	// append three records to the log
	for i := 0; i < 3; i++ {
		_, err := log.Append(rec)
		require.NoError(t, err)
	}
	// close the log
	require.NoError(t, log.Close())

	lowest, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowest)

	highest, err := log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highest)

	// create a new log configured with the same directory
	nLog, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	// confirm that the new log sets itself up with data from the original log
	nLowest, err := nLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, lowest, nLowest)

	nHighest, err := nLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, highest, nHighest)
}

// testReader tests that we can read the full, raw log as it is stored
// ot disk so that we can snapshot and restore the logs
func testReader(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}

	offset, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	reader := log.Reader()
	bytes, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(bytes[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, rec.Value, read.Value)
}

// testTruncate tests that we can truncate the log and
// remove old segments that we don't need anymore
func testTruncate(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(rec)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}
