package tsdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync/atomic"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"go.uber.org/zap"
)

// verifyResult contains the result of a Verify... call
type verifyResult struct {
	valid bool
	err   error
}

// Verify contains configuration for running verification of series files.
type Verify struct {
	Recover           bool   // If set to false then Verify will not recover panics
	Concurrent        int    // the level of concurrency to process partitions
	ContinueOnError   bool   // if true then Verify will continue to verify after hitting an error
	MeasurementPrefix []byte // a prefix to filter series by. If set then only matching series will be processed
	Logger            *zap.Logger

	SeriesFilePath string
	sfile          *SeriesFile
	done           chan struct{}
}

// NewVerify constructs a Verify with good defaults.
func NewVerify() Verify {
	return Verify{
		Recover:    true,
		Concurrent: runtime.GOMAXPROCS(0),
		Logger:     zap.NewNop(),
	}
}

// VerifySeriesFile performs verifications on a series file. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the series file.
func (v Verify) VerifySeriesFile() (valid bool, err error) {
	log, logEnd := logger.NewOperation(context.Background(), v.Logger, "Beginning Series File verification", "sfile_verification",
		zap.String("path", v.SeriesFilePath),
	)
	v.Logger = log
	defer logEnd()

	// series file is used only for emitting more information on failure.
	v.sfile = NewSeriesFile(v.SeriesFilePath)
	if err := v.sfile.Open(context.Background()); err != nil {
		return false, err
	}
	defer v.sfile.Close()

	defer func() {
		if !v.Recover {
			return
		} else if rec := recover(); rec != nil {
			v.Logger.Error("Panic verifying file", zap.String("recovered", fmt.Sprint(rec)))
			valid = false
		}
	}()

	partitionDirs, err := ioutil.ReadDir(v.SeriesFilePath)
	if os.IsNotExist(err) {
		v.Logger.Error("Series file does not exist")
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Check every partition in concurrently.
	n := v.Concurrent
	if n <= 0 {
		n = 1
	}

	m := len(partitionDirs)
	out := make(chan verifyResult, m)
	var pidx uint32          // Index tracking progress of work towards m
	for k := 0; k < n; k++ { // Create a worker pool of n workers
		go func() {
			for { // Each worker continues doing work until m work has been done.
				idx := int(atomic.AddUint32(&pidx, 1) - 1)
				if idx >= m {
					return // No more work.
				}

				// Work
				path := filepath.Join(v.SeriesFilePath, partitionDirs[idx].Name())
				valid, err := v.VerifyPartition(path)
				out <- verifyResult{valid: valid, err: err}
			}
		}()
	}

	for i := 0; i < m; i++ {
		result := <-out
		if valid {
			valid = result.valid
		}

		if result.err != nil {
			return false, err
		} else if !result.valid && !v.ContinueOnError {
			return false, nil
		}
	}
	return valid, nil
}

// VerifyPartition performs verifications on a partition of a series file. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the partition.
func (v Verify) VerifyPartition(partitionPath string) (valid bool, err error) {
	log, logEnd := logger.NewOperation(context.Background(), v.Logger, "Beginning partition verification", "partition_verification", zap.String("partition", filepath.Base(partitionPath)))
	v.Logger = log
	defer logEnd()

	defer func() {
		if !v.Recover {
			return
		} else if rec := recover(); rec != nil {
			v.Logger.Error("Panic verifying partition", zap.String("recovered", fmt.Sprint(rec)))
			valid = false
		}
	}()

	segmentInfos, err := ioutil.ReadDir(partitionPath)
	if err != nil {
		return false, err
	}

	segments := make([]*SeriesSegment, 0, len(segmentInfos))
	ids := make(map[SeriesIDTyped]IDData)

	// check every segment
	for _, segmentInfo := range segmentInfos {
		select {
		default:
		case <-v.done:
			return false, nil
		}

		segmentPath := filepath.Join(partitionPath, segmentInfo.Name())
		segmentID, err := ParseSeriesSegmentFilename(segmentInfo.Name())
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
		segment := NewSeriesSegment(segmentID, segmentPath)
		if err := segment.Open(); err != nil {
			return false, err
		}
		defer segment.Close()

		segments = append(segments, segment)
	}

	// check the index
	return v.VerifyIndex(filepath.Join(partitionPath, "index"), segments, ids)
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
func (v Verify) VerifySegment(segmentPath string, ids map[SeriesIDTyped]IDData) (valid bool, err error) {
	var insertEntries, deleteEntries int64
	log, logEnd := logger.NewOperation(context.Background(), v.Logger, "Beginning segment verification", "segment_verification",
		zap.String("segment_path", segmentPath),
	)
	v.Logger = log
	defer func() {
		v.Logger.Info("Processed entries", zap.Int64("insertions", insertEntries), zap.Int64("deletions", deleteEntries))
	}()
	defer logEnd()

	// Open up the segment and grab it's data.
	segmentID, err := ParseSeriesSegmentFilename(filepath.Base(segmentPath))
	if err != nil {
		return false, err
	}
	segment := NewSeriesSegment(segmentID, segmentPath)
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
	if err := buf.advance(SeriesSegmentHeaderSize); err != nil {
		v.Logger.Error("Unable to advance buffer",
			zap.Int64("offset", buf.offset),
			zap.Error(err))
		return false, nil
	}

	currentID, firstID := NewSeriesIDTyped(0), true

entries:
	for len(buf.data) > 0 {
		select {
		default:
		case <-v.done:
			return false, nil
		}

		flag, id, key, sz := ReadSeriesEntry(buf.data)

		// Check the flag is valid and for id monotonicity.
		hasKey := true
		parsed := false
		switch flag {
		case SeriesEntryInsertFlag:
			var skip bool
			if v.MeasurementPrefix != nil {
				skip = func() bool {
					defer func() {
						if rec := recover(); rec != nil {
							v.Logger.Error("Panic parsing key",
								zap.String("key", fmt.Sprintf("%x", key)),
								zap.Int64("offset", buf.offset),
								zap.String("recovered", fmt.Sprint(rec)))
						}
					}()

					parsed = true
					// This is more efficient than ParseSeriesKey if you just need the measurement.
					_, data := ReadSeriesKeyLen(key)
					name, _ := ReadSeriesKeyMeasurement(data)
					return !bytes.HasPrefix(name, v.MeasurementPrefix)
				}()

				if !parsed && !v.ContinueOnError {
					return false, nil
				}
			}

			if skip {
				break // measurement prefix did not match series key
			}

			insertEntries++
			if !firstID && currentID.SeriesID().Greater(id.SeriesID()) {
				v.Logger.Error("ID is not monotonically increasing",
					zap.Uint64("prev_id", currentID.RawID()),
					zap.Uint64("id", id.RawID()),
					zap.Int64("offset", buf.offset))
				if v.ContinueOnError {
					break
				}
				return false, nil
			}

			firstID = false
			currentID = id

			if ids != nil {
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)

				if _, ok := ids[currentID]; ok {
					v.Logger.Error("Insert before Delete", zap.Uint64("id", currentID.SeriesID().RawID()),
						zap.Int64("offset", buf.offset),
					)
					if !v.ContinueOnError {
						return false, nil
					}
				}
				ids[currentID] = IDData{
					Offset: JoinSeriesOffset(segment.ID(), uint32(buf.offset)),
					Key:    keyCopy,
				}
			}

		case SeriesEntryTombstoneFlag:
			deleteEntries++
			hasKey = false
			if ids != nil {
				data, ok := ids[currentID]
				if ok {
					data.Deleted = true
					ids[currentID] = data
				} else if v.MeasurementPrefix == nil {
					// A tombstone entry was found before the insert...
					v.Logger.Error("Delete before Insert", zap.Uint64("id", currentID.SeriesID().RawID()),
						zap.Int64("offset", buf.offset),
					)
					if !v.ContinueOnError {
						return false, nil
					}
				}
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
		// If measurement prefix checking enabled then we don't need to do this again.
		if hasKey && !parsed {
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						v.Logger.Error("Panic parsing key",
							zap.String("key", fmt.Sprintf("%x", key)),
							zap.Int64("offset", buf.offset),
							zap.String("recovered", fmt.Sprint(rec)))
					}
				}()
				ParseSeriesKey(key)
				parsed = true
			}()
			if !parsed && !v.ContinueOnError {
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
func (v Verify) VerifyIndex(indexPath string, segments []*SeriesSegment, ids map[SeriesIDTyped]IDData) (valid bool, err error) {
	log, logEnd := logger.NewOperation(context.Background(), v.Logger, "Beginning index verification", "segment_index",
		zap.String("index_path", indexPath),
	)
	v.Logger = log
	defer logEnd()

	defer func() {
		if !v.Recover {
			return
		} else if rec := recover(); rec != nil {
			v.Logger.Error("Panic verifying index", zap.String("recovered", fmt.Sprint(rec)))
			valid = false
		}
	}()

	index := NewSeriesIndex(indexPath)
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
	idsList := make([]SeriesIDTyped, 0, len(ids))
	for id := range ids {
		idsList = append(idsList, id)
	}
	sort.Slice(idsList, func(i, j int) bool {
		return idsList[i].RawID() < idsList[j].RawID()
	})

	var success = true
	for _, id := range idsList {
		select {
		default:
		case <-v.done:
			return false, nil
		}

		IDData := ids[id]

		if gotDeleted := index.IsDeleted(id.SeriesID()); gotDeleted != IDData.Deleted {
			// Get measurement name and tags from original segment entry
			var name []byte
			var tags models.Tags
			if IDData.Key != nil {
				name, tags, err = v.processSeriesKey(IDData.Key)
				if err != nil {
					return false, err // This should never happen
				}
			}

			// Get the key again from the ID that's in the index..
			name2, tags2, err := v.seriesKeyFromID(id.SeriesID())
			if err != nil {
				v.Logger.Error("Index inconsistent", zap.Error(err))
				if !v.ContinueOnError {
					return false, err
				}
				success = false
				continue
			}

			v.Logger.Error("Index inconsistency",
				zap.Uint64("id_typed", id.RawID()),
				zap.Uint64("id", id.SeriesID().RawID()),
				zap.Bool("got_deleted", gotDeleted),
				zap.Bool("expected_deleted", IDData.Deleted),
				zap.String("measurement_from_entry", string(name)),
				zap.String("tags_from_entry", tags.String()),
				zap.String("measurement_from_index_id", string(name2)),
				zap.String("tags_from_index_id", tags2.String()),
			)
			if !v.ContinueOnError {
				return false, nil
			}
			success = false
			continue
		}

		// do not perform any other checks if the id is deleted.
		if IDData.Deleted {
			continue
		}

		// otherwise, check both that the offset is right and that we get the right id for the key
		if gotOffset := index.FindOffsetByID(id.SeriesID()); gotOffset != IDData.Offset {
			v.Logger.Error("Index inconsistency",
				zap.Uint64("id_typed", id.RawID()),
				zap.Uint64("id", id.SeriesID().RawID()),
				zap.Int64("got_offset", gotOffset),
				zap.Int64("expected_offset", IDData.Offset))
			if !v.ContinueOnError {
				return false, nil
			}
			success = false
			continue
		}

		if gotID := index.FindIDBySeriesKey(segments, IDData.Key); gotID != id {
			v.Logger.Error("Index inconsistency",
				zap.Uint64("id_typed", id.RawID()),
				zap.Uint64("id", id.SeriesID().RawID()),
				zap.Uint64("got_id", gotID.RawID()),
				zap.Uint64("expected_id_typed", id.RawID()))
			if !v.ContinueOnError {
				return false, nil
			}
			success = false
			continue
		}
	}

	return success, nil
}

// seriesKeyFromID retrieves the series key for the provided id, converting the
// measurement name into base-16 and replacing the special-case tag keys _measurement
// and _field.
func (v Verify) seriesKeyFromID(id SeriesID) ([]byte, models.Tags, error) {
	// Attempt to get the series key for debugging.
	key, _ := ReadSeriesKey(v.sfile.SeriesKey(id))
	if key == nil {
		return nil, nil, nil
	}
	return v.processSeriesKey(key)
}

func (v Verify) processSeriesKey(key []byte) ([]byte, models.Tags, error) {
	name, tags := ParseSeriesKey(key)

	if len(tags) < 2 || !bytes.Equal(tags[0].Key, models.MeasurementTagKeyBytes) || !bytes.Equal(tags[len(tags)-1].Key, models.FieldKeyTagKeyBytes) {
		v.Logger.Error("Invalid series key",
			zap.String("key", string(key)),
			zap.String("key_base_16", fmt.Sprintf("%x", key)),
		)
		return nil, nil, errors.New("unable to parse series key")
	}

	for i, tag := range tags {
		if i == 0 {
			tag.Key = []byte("_measurement")
			tags[i] = tag
		} else if i == len(tags)-1 {
			tag.Key = []byte("_field")
			tags[i] = tag
		}
	}

	nameHex := []byte(fmt.Sprintf("%x", name))
	return nameHex, tags, nil
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
