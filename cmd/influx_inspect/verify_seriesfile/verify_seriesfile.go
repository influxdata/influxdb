// Package verify_seriesfile verifies integrity of series files.
package verify_seriesfile

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

// Command represents the program execution for "influx_inspect verify-seriesfile".
type Command struct {
	Stderr io.Writer
	Stdout io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("verify-seriesfile", flag.ExitOnError)
	fs.SetOutput(cmd.Stdout)

	dataDir := fs.String("dir", os.Getenv("HOME")+"/.influxdb", "data directory.")
	dbPath := fs.String("db", "", "database path. overrides data directory.")
	filePath := fs.String("file", "", "series file path. overrides db and data directory.")

	if err := fs.Parse(args); err != nil {
		return err
	}

	return cmd.run(*dataDir, *dbPath, *filePath)
}

// run performs all of the validations, preferring a filePath, then a dbPath,
// and then a dataDir.
func (cmd *Command) run(dataDir, dbPath, filePath string) error {
	if filePath != "" {
		_, err := cmd.runFile(filePath)
		return err
	}
	if dbPath != "" {
		_, err := cmd.runFile(filepath.Join(dbPath, "_series"))
		return err
	}

	dbs, err := ioutil.ReadDir(filepath.Join(dataDir, "data"))
	if err != nil {
		return err
	}

	for _, db := range dbs {
		if !db.IsDir() {
			continue
		}
		_, err := cmd.runFile(filepath.Join(dataDir, "data", db.Name(), "_series"))
		if err != nil {
			return err
		}
	}

	return nil
}

// runFile performs validations on a series file. The error is only returned if
// there was some fatal problem with operating, not if there was a problem with
// the series file.
func (cmd *Command) runFile(filePath string) (valid bool, err error) {
	logger := logger.New(cmd.Stderr).With(zap.String("path", filePath))
	logger.Info("starting validation")

	defer func() {
		if rec := recover(); rec != nil {
			logger.Error("panic validating file",
				zap.String("recovered", fmt.Sprint(rec)))
			valid = false
		}
	}()

	partitionInfos, err := ioutil.ReadDir(filePath)
	if os.IsNotExist(err) {
		logger.Error("series file does not exist")
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// check every partition
	for _, partitionInfo := range partitionInfos {
		pLogger := logger.With(zap.String("partition", partitionInfo.Name()))
		pLogger.Info("validating partition")
		partitionPath := filepath.Join(filePath, partitionInfo.Name())

		segmentInfos, err := ioutil.ReadDir(partitionPath)
		if err != nil {
			return false, err
		}
		segments := make([]*tsdb.SeriesSegment, 0, len(segmentInfos))
		ids := make(map[uint64]idData)

		// check every segment
		for _, segmentInfo := range segmentInfos {
			segmentPath := filepath.Join(partitionPath, segmentInfo.Name())

			segmentID, err := tsdb.ParseSeriesSegmentFilename(segmentInfo.Name())
			if err != nil {
				continue
			}

			sLogger := pLogger.With(zap.String("segment", segmentInfo.Name()))
			sLogger.Info("validating segment")

			segment := tsdb.NewSeriesSegment(segmentID, segmentPath)
			if err := segment.Open(); err != nil {
				sLogger.Error("opening segment", zap.Error(err))
				return false, nil
			}
			defer segment.Close()

			if offset, err := cmd.validateSegment(segment, ids); err != nil {
				sLogger.Error("iterating over segment",
					zap.Int64("offset", offset),
					zap.Error(err))
				return false, nil
			}

			segments = append(segments, segment)
		}

		// validate the index if it exists
		indexPath := filepath.Join(partitionPath, "index")
		_, err = os.Stat(indexPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return false, err
		}

		pLogger.Info("validating index")

		index := tsdb.NewSeriesIndex(indexPath)
		if err := index.Open(); err != nil {
			pLogger.Error("opening index", zap.Error(err))
			return false, nil
		}
		defer index.Close()

		if err := index.Recover(segments); err != nil {
			pLogger.Error("recovering index", zap.Error(err))
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
			idData := ids[id]
			idLogger := pLogger.With(zap.Uint64("id", id))

			expectedOffset, expectedID := idData.offset, id
			if idData.deleted {
				expectedOffset, expectedID = 0, 0
			}

			// check both that the offset is right and that we get the right
			// id for the key

			if gotOffset := index.FindOffsetByID(id); gotOffset != expectedOffset {
				idLogger.Error("index inconsistency",
					zap.Int64("got_offset", gotOffset),
					zap.Int64("expected_offset", expectedOffset))
				return false, nil
			}

			if gotID := index.FindIDBySeriesKey(segments, idData.key); gotID != expectedID {
				idLogger.Error("index inconsistency",
					zap.Uint64("got_id", gotID),
					zap.Uint64("expected_id", expectedID))
				return false, nil
			}
		}
	}

	logger.Info("validation passed")
	return true, nil
}

// idData keeps track of data about a series ID.
type idData struct {
	offset  int64
	key     []byte
	deleted bool
}

// validateSegment checks that all of the entries in the segment are well formed. If there
// is any error, the offset at which it happened is returned.
func (cmd *Command) validateSegment(segment *tsdb.SeriesSegment, ids map[uint64]idData) (offset int64, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic validating segment: %v", rec)
		}
	}()

	data := segment.Data()

	advance := func(n int64) error {
		if int64(len(data)) < n {
			return fmt.Errorf("unable to advance %d bytes: %d remaining", n, len(data))
		}
		offset += n
		data = data[n:]
		return nil
	}

	// Skip the header: it has already been validated by the Open call.
	if err := advance(tsdb.SeriesSegmentHeaderSize); err != nil {
		return offset, err
	}

	prevID, firstID := uint64(0), true

entries:
	for len(data) > 0 {
		flag, id, key, sz := tsdb.ReadSeriesEntry(data)

		// check the flag is valid and for id monotonicity
		switch flag {
		case tsdb.SeriesEntryInsertFlag:
			if !firstID && prevID > id {
				return offset, fmt.Errorf("id is not monotonic: %d and then %d", prevID, id)
			}

			firstID = false
			prevID = id
			ids[id] = idData{
				offset: tsdb.JoinSeriesOffset(segment.ID(), uint32(offset)),
				key:    key,
			}

		case tsdb.SeriesEntryTombstoneFlag:
			data := ids[id]
			data.deleted = true
			ids[id] = data

		case 0: // if zero, there are no more entries
			if err := advance(sz); err != nil {
				return offset, err
			}
			break entries

		default:
			return offset, fmt.Errorf("invalid flag: %d", flag)
		}

		// ensure the key parses. this may panic, but our defer handler should
		// make the error message more usable.
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					err = fmt.Errorf("panic parsing key: %x", key)
				}
			}()
			tsdb.ParseSeriesKey(key)
		}()
		if err != nil {
			return offset, err
		}

		// consume the entry
		if err := advance(sz); err != nil {
			return offset, err
		}
	}

	return 0, nil
}
