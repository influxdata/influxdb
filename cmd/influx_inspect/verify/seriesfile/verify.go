package seriesfile

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

// verifyResult contains the result of a Verify... call
type verifyResult struct {
	valid bool
	err   error
}

// Verify contains configuration for running verification of series files.
type Verify struct {
	Concurrent int
	Logger     *zap.Logger

	done chan struct{}
}

// NewVerify constructs a Verify with good defaults.
func NewVerify() Verify {
	return Verify{
		Concurrent: runtime.GOMAXPROCS(0),
		Logger:     zap.NewNop(),
	}
}

// VerifySeriesFile performs verifications on a series file. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the series file.
func (v Verify) VerifySeriesFile(filePath string) (valid bool, err error) {
	v.Logger = v.Logger.With(zap.String("path", filePath))
	v.Logger.Info("Verifying series file")

	defer func() {
		if rec := recover(); rec != nil {
			v.Logger.Error("Panic verifying file", zap.String("recovered", fmt.Sprint(rec)))
			valid = false
		}
	}()

	partitionInfos, err := ioutil.ReadDir(filePath)
	if os.IsNotExist(err) {
		v.Logger.Error("Series file does not exist")
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Check every partition in concurrently.
	concurrent := v.Concurrent
	if concurrent <= 0 {
		concurrent = 1
	}
	in := make(chan string, len(partitionInfos))
	out := make(chan verifyResult, len(partitionInfos))

	// Make sure all the workers are cleaned up when we return.
	var wg sync.WaitGroup
	defer wg.Wait()

	// Set up cancellation. Any return will cause the workers to be cancelled.
	v.done = make(chan struct{})
	defer close(v.done)

	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for partitionPath := range in {
				valid, err := v.VerifyPartition(partitionPath)
				select {
				case out <- verifyResult{valid: valid, err: err}:
				case <-v.done:
					return
				}
			}
		}()
	}

	// send off the work and read the results.
	for _, partitionInfo := range partitionInfos {
		in <- filepath.Join(filePath, partitionInfo.Name())
	}
	close(in)

	for range partitionInfos {
		result := <-out
		if result.err != nil {
			return false, err
		} else if !result.valid {
			return false, nil
		}
	}

	return true, nil
}

// VerifyPartition performs verifications on a partition of a series file. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the partition.
func (v Verify) VerifyPartition(partitionPath string) (valid bool, err error) {
	v.Logger = v.Logger.With(zap.String("partition", filepath.Base(partitionPath)))
	v.Logger.Info("Verifying partition")

	defer func() {
		if rec := recover(); rec != nil {
			v.Logger.Error("Panic verifying partition", zap.String("recovered", fmt.Sprint(rec)))
			valid = false
		}
	}()

	segmentInfos, err := ioutil.ReadDir(partitionPath)
	if err != nil {
		return false, err
	}

	segments := make([]*tsdb.SeriesSegment, 0, len(segmentInfos))
	ids := make(map[uint64]IDData)

	// check every segment
	for _, segmentInfo := range segmentInfos {
		select {
		default:
		case <-v.done:
			return false, nil
		}

		segmentPath := filepath.Join(partitionPath, segmentInfo.Name())
		segmentID, err := tsdb.ParseSeriesSegmentFilename(segmentInfo.Name())
		if err != nil {
			continue
		}

		if valid, err := v.VerifySegment(segmentPath, ids); err != nil {
			return false, err
		} else if !valid {
			return false, nil
		}

		// open the segment for verifying the index. we want it to be open outside
		// the for loop as well, so the defer is ok.
		segment := tsdb.NewSeriesSegment(segmentID, segmentPath)
		if err := segment.Open(); err != nil {
			return false, err
		}
		defer segment.Close()

		segments = append(segments, segment)
	}

	// check the index
	indexPath := filepath.Join(partitionPath, "index")
	if valid, err := v.VerifyIndex(indexPath, segments, ids); err != nil {
		return false, err
	} else if !valid {
		return false, nil
	}

	return true, nil
}

// IDData keeps track of data about a series ID.
type IDData struct {
	Offset  int64
	Key     []byte
	Deleted bool
}

// VerifySegment performs verifications on a segment of a series file. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the partition.
// The ids map is populated with information about the ids stored in the segment.
func (v Verify) VerifySegment(segmentPath string, ids map[uint64]IDData) (valid bool, err error) {
	segmentName := filepath.Base(segmentPath)
	v.Logger = v.Logger.With(zap.String("segment", segmentName))
	v.Logger.Info("Verifying segment")

	// Open up the segment and grab it's data.
	segmentID, err := tsdb.ParseSeriesSegmentFilename(segmentName)
	if err != nil {
		return false, err
	}
	segment := tsdb.NewSeriesSegment(segmentID, segmentPath)
	if err := segment.Open(); err != nil {
		v.Logger.Error("Error opening segment", zap.Error(err))
		return false, nil
	}
	defer segment.Close()
	buf := newBuffer(segment.Data())

	defer func() {
		if rec := recover(); rec != nil {
			v.Logger.Error("Panic verifying segment", zap.String("recovered", fmt.Sprint(rec)),
				zap.Int64("offset", buf.offset))
			valid = false
		}
	}()

	// Skip the header: it has already been verified by the Open call.
	if err := buf.advance(tsdb.SeriesSegmentHeaderSize); err != nil {
		v.Logger.Error("Unable to advance buffer",
			zap.Int64("offset", buf.offset),
			zap.Error(err))
		return false, nil
	}

	prevID, firstID := uint64(0), true

entries:
	for len(buf.data) > 0 {
		select {
		default:
		case <-v.done:
			return false, nil
		}

		flag, id, key, sz := tsdb.ReadSeriesEntry(buf.data)

		// Check the flag is valid and for id monotonicity.
		hasKey := true
		switch flag {
		case tsdb.SeriesEntryInsertFlag:
			if !firstID && prevID > id {
				v.Logger.Error("ID is not monotonically increasing",
					zap.Uint64("prev_id", prevID),
					zap.Uint64("id", id),
					zap.Int64("offset", buf.offset))
				return false, nil
			}

			firstID = false
			prevID = id

			if ids != nil {
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)

				ids[id] = IDData{
					Offset: tsdb.JoinSeriesOffset(segment.ID(), uint32(buf.offset)),
					Key:    keyCopy,
				}
			}

		case tsdb.SeriesEntryTombstoneFlag:
			hasKey = false
			if ids != nil {
				data := ids[id]
				data.Deleted = true
				ids[id] = data
			}

		case 0: // if zero, there are no more entries
			if err := buf.advance(sz); err != nil {
				v.Logger.Error("Unable to advance buffer",
					zap.Int64("offset", buf.offset),
					zap.Error(err))
				return false, nil
			}
			break entries

		default:
			v.Logger.Error("Invalid flag",
				zap.Uint8("flag", flag),
				zap.Int64("offset", buf.offset))
			return false, nil
		}

		// Ensure the key parses. This may panic, but our defer handler should
		// make the error message more usable by providing the key.
		if hasKey {
			parsed := false
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						v.Logger.Error("Panic parsing key",
							zap.String("key", fmt.Sprintf("%x", key)),
							zap.Int64("offset", buf.offset),
							zap.String("recovered", fmt.Sprint(rec)))
					}
				}()
				tsdb.ParseSeriesKey(key)
				parsed = true
			}()
			if !parsed {
				return false, nil
			}
		}

		// Advance past the entry.
		if err := buf.advance(sz); err != nil {
			v.Logger.Error("Unable to advance buffer",
				zap.Int64("offset", buf.offset),
				zap.Error(err))
			return false, nil
		}
	}

	return true, nil
}

// VerifyIndex performs verification on an index in a series file. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the partition.
// The ids map must be built from verifying the passed in segments.
func (v Verify) VerifyIndex(indexPath string, segments []*tsdb.SeriesSegment,
	ids map[uint64]IDData) (valid bool, err error) {
	v.Logger.Info("Verifying index")

	defer func() {
		if rec := recover(); rec != nil {
			v.Logger.Error("Panic verifying index", zap.String("recovered", fmt.Sprint(rec)))
			valid = false
		}
	}()

	index := tsdb.NewSeriesIndex(indexPath)
	if err := index.Open(); err != nil {
		v.Logger.Error("Error opening index", zap.Error(err))
		return false, nil
	}
	defer index.Close()

	if err := index.Recover(segments); err != nil {
		v.Logger.Error("Error recovering index", zap.Error(err))
		return false, nil
	}

	// we check all the ids in a consistent order to get the same errors if
	// there is a problem
	idsList := make([]uint64, 0, len(ids))
	for id := range ids {
		idsList = append(idsList, id)
	}
	sort.Slice(idsList, func(i, j int) bool {
		return idsList[i] < idsList[j]
	})

	for _, id := range idsList {
		select {
		default:
		case <-v.done:
			return false, nil
		}

		IDData := ids[id]

		expectedOffset, expectedID := IDData.Offset, id
		if IDData.Deleted {
			expectedOffset, expectedID = 0, 0
		}

		// check both that the offset is right and that we get the right
		// id for the key
		if gotOffset := index.FindOffsetByID(id); gotOffset != expectedOffset {
			v.Logger.Error("Index inconsistency",
				zap.Uint64("id", id),
				zap.Int64("got_offset", gotOffset),
				zap.Int64("expected_offset", expectedOffset))
			return false, nil
		}

		if gotID := index.FindIDBySeriesKey(segments, IDData.Key); gotID != expectedID {
			v.Logger.Error("Index inconsistency",
				zap.Uint64("id", id),
				zap.Uint64("got_id", gotID),
				zap.Uint64("expected_id", expectedID))
			return false, nil
		}
	}

	return true, nil
}

// buffer allows one to safely advance a byte slice and keep track of how many bytes were advanced.
type buffer struct {
	offset int64
	data   []byte
}

// newBuffer constructs a buffer with the provided data.
func newBuffer(data []byte) *buffer {
	return &buffer{
		offset: 0,
		data:   data,
	}
}

// advance will consume n bytes from the data slice and return an error if there is not enough
// data to do so.
func (b *buffer) advance(n int64) error {
	if int64(len(b.data)) < n {
		return fmt.Errorf("unable to advance %d bytes: %d remaining", n, len(b.data))
	}
	b.data = b.data[n:]
	b.offset += n
	return nil
}
