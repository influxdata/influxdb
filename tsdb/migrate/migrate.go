// Package migrate provides tooling to migrate data from InfluxDB 1.x to 2.x
package migrate

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/cmd/influx_inspect/buildtsi"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/bufio"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/influxdata/influxdb/v2/tsdb/tsi1"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
	"go.uber.org/zap"
)

const (
	dataDirName1x       = "data"
	internalDBName1x    = "_internal"
	importTempExtension = ".migrate"

	// 	// InfluxDB 1.x TSM index entry size.
	tsmIndexEntrySize1x = 0 +
		8 + // Block min time
		8 + // Block max time
		8 + // Offset of block
		4 // Size in bytes of block

	tsmKeyFieldSeparator1x = "#!~#" // 		tsm1 key field separator.
)

type Config struct {
	SourcePath string
	DestPath   string
	DestOrg    influxdb.ID

	From            int64
	To              int64
	MigrateHotShard bool

	DryRun bool

	// Optional if you want to emit logs
	Stdout         io.Writer
	VerboseLogging bool
}

// A Migrator migrates TSM data from a InfluxDB 1.x to InfluxDB 2.x.
type Migrator struct {
	Config
	store         *bolt.KVStore // ref needed to we can cleanup
	metaSvc       *kv.Service
	verboseStdout io.Writer

	current2xTSMGen int
}

func NewMigrator(c Config) *Migrator {
	if c.Stdout == nil {
		c.Stdout = ioutil.Discard
	}

	verboseStdout := ioutil.Discard
	if c.VerboseLogging {
		verboseStdout = c.Stdout
	}

	log := logger.New(c.Stdout)
	boltClient := bolt.NewClient(log.With(zap.String("service", "bolt")))
	boltClient.Path = filepath.Join(c.DestPath, "influxd.bolt")

	store := bolt.NewKVStore(
		log.With(zap.String("service", "kvstore-bolt")),
		filepath.Join(c.DestPath, "influxd.bolt"),
	)
	store.WithDB(boltClient.DB())
	if err := store.Open(context.Background()); err != nil {
		panic(err)
	}

	metaSvc := kv.NewService(log.With(zap.String("store", "kv")), store)

	// Update the destination path - we only care about the tsm store now.
	c.DestPath = filepath.Join(c.DestPath, "engine", "data")

	return &Migrator{Config: c, store: store, metaSvc: metaSvc, verboseStdout: verboseStdout}
}

// shardMapping provides a mapping between a 1.x shard and a bucket in 2.x
type shardMapping struct {
	path     string
	bucketID influxdb.ID
}

// Process1xShards migrates the contents of any matching 1.x shards.
//
// The caller can filter shards only belonging to a retention policy and database.
// Providing the zero value for the filters will result in all shards being
// migrated, with the exception of the `_internal` database, which is never
// migrated unless explicitly filtered on.
func (m *Migrator) Process1xShards(dbFilter, rpFilter string) error {
	defer m.store.Close()

	// determine current gen
	fs := tsm1.NewFileStore(m.DestPath)
	if err := fs.Open(context.Background()); err != nil {
		return err
	}
	m.current2xTSMGen = fs.NextGeneration()
	fs.Close()

	var (
		toProcessShards []shardMapping
		curDB, curRP    string      // track current db and rp
		bucketID        influxdb.ID // track current bucket ID
	)

	err := walkShardDirs(filepath.Join(m.SourcePath, dataDirName1x), func(db string, rp string, path string) error {
		if dbFilter == "" && db == internalDBName1x {
			return nil // Don't import TSM data from _internal unless explicitly instructed to
		}

		// A database or retention policy filter has been specified and this
		// shard path does not match it.
		if (dbFilter != "" && db != dbFilter) || (rpFilter != "" && rp != rpFilter) {
			return nil
		}

		var err error
		if db != curDB || rp != curRP {
			if bucketID, err = m.createBucket(db, rp); err != nil {
				return err
			}
			curDB, curRP = db, rp
		}

		toProcessShards = append(toProcessShards, shardMapping{path: path, bucketID: bucketID})
		return nil
	})
	if err != nil {
		return err
	}

	// Sort shards so that for each database and retention policy, we deal handle
	// them in the order they were created.
	sortShardDirs(toProcessShards)

	for _, shard := range toProcessShards {
		now := time.Now()
		if err := m.Process1xShard(shard.path, shard.bucketID); err != nil {
			return err
		}
		fmt.Fprintf(m.Stdout, "Migrated shard %s to bucket %s in %v\n", shard.path, shard.bucketID.String(), time.Since(now))
	}

	fmt.Fprintln(m.Stdout, "Building TSI index")

	sfilePath := filepath.Join(filepath.Dir(m.DestPath), storage.DefaultSeriesFileDirectoryName)
	sfile := seriesfile.NewSeriesFile(sfilePath)
	sfile.Logger = logger.New(m.verboseStdout)
	if err := sfile.Open(context.Background()); err != nil {
		return err
	}
	defer sfile.Close()

	indexPath := filepath.Join(filepath.Dir(m.DestPath), storage.DefaultIndexDirectoryName)
	// Check if TSI index exists.
	if _, err = os.Stat(indexPath); err == nil {
		if m.DryRun {
			fmt.Fprintf(m.Stdout, "Would remove index located at %q\n", indexPath)
		} else if err := os.RemoveAll(indexPath); err != nil { // Remove the index
			return err
		} else {
			fmt.Fprintf(m.Stdout, "Removed existing TSI index at %q\n", indexPath)
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	if m.DryRun {
		fmt.Fprintf(m.Stdout, "Would rebuild index at %q\n", indexPath)
		return nil
	}

	walPath := filepath.Join(filepath.Dir(m.DestPath), storage.DefaultWALDirectoryName)
	err = buildtsi.IndexShard(sfile, indexPath, m.DestPath, walPath,
		tsi1.DefaultMaxIndexLogFileSize, uint64(tsm1.DefaultCacheMaxMemorySize),
		10000, logger.New(m.verboseStdout), false)

	if err != nil {
		msg := fmt.Sprintf(`
**ERROR** - TSI index rebuild failed.

The index has potentially been left in an unrecoverable state. Indexes can be rebuilt
using the 'influxd inspect build-tsi' command.

Step 1: remove TSI index with '$ rm -rf %s'
Step 2: run '$ influxd inspect build-tsi'

Original error: %v
`, indexPath, err)
		return errors.New(msg)
	}

	return nil
}

// sortShardDirs sorts shard directories in lexicographical order according to
// database and retention policy. Shards within the same database and
// retention policy are sorted numerically by shard id.
func sortShardDirs(shards []shardMapping) error {
	var err2 error
	sort.Slice(shards, func(i, j int) bool {
		iDir := filepath.Dir(shards[i].path)
		jDir := filepath.Dir(shards[j].path)
		if iDir != jDir {
			return iDir < jDir // db or rp differ
		}

		// Same db and rp. Sort on shard id.
		iID, err := strconv.Atoi(filepath.Base(shards[i].path))
		if err != nil {
			err2 = err
			return false
		}

		jID, err := strconv.Atoi(filepath.Base(shards[j].path))
		if err != nil {
			err2 = err
			return false
		}
		return iID < jID
	})
	return err2
}

func (m *Migrator) createBucket(db, rp string) (influxdb.ID, error) {
	name := filepath.Join(db, rp)

	bucket, err := m.metaSvc.FindBucketByName(context.Background(), m.DestOrg, name)
	if err != nil {
		innerErr, ok := err.(*influxdb.Error)
		if !ok || innerErr.Code != influxdb.ENotFound {
			return 0, err
		}
	} else if bucket != nil {
		// Ignore an error returned from being unable to find a bucket.
		fmt.Fprintf(m.verboseStdout, "Bucket %q already exists with ID %s\n", name, bucket.ID.String())
		return bucket.ID, nil
	}

	if !m.DryRun {
		bucket = &influxdb.Bucket{
			Name:  name,
			OrgID: m.DestOrg,
		}
		if err := m.metaSvc.CreateBucket(context.Background(), bucket); err != nil {
			return 0, err
		}
		fmt.Fprintf(m.verboseStdout, "Created bucket %q with ID %s\n", name, bucket.ID.String())
	} else {
		fmt.Fprintf(m.Stdout, "Would create bucket %q\n", name)
	}

	return bucket.ID, nil
}

// Process1xShard migrates the TSM data in a single 1.x shard to the 2.x data directory.
//
// First, the shard is checked to determine it's fully compacted. Hot shards are
// not migrated by default as the WAL is not processed, which could lead to data
// loss. Next, each TSM file contents is checked to ensure it overlaps the
// desired time-range, and all matching data is migrated.
//
func (m *Migrator) Process1xShard(pth string, bucketID influxdb.ID) error {
	// * Check full compaction
	// * Stream TSM file into new TSM file
	//	- full blocks can be copied over if the time range overlaps.
	//  - partial blocks need to be decoded and written out up to the timestamp.
	//  - Index needs to include any entries that have at least one block overlapping
	//    the time range.

	//
	// TODO(edd): strategy for detecting hot shard - need to check for any
	// existence of WAL files.
	//

	// Check for `tmp` files and identify TSM file(s) path.
	var tsmPaths []string // Possible a fully compacted shard has multiple TSM files.
	filepath.Walk(pth, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(p, fmt.Sprintf(".%s.%s", tsm1.TSMFileExtension, tsm1.CompactionTempExtension)) {
			return fmt.Errorf("tmp TSM file detected at %q — aborting shard import", p)
		} else if ext := filepath.Ext(p); ext == "."+tsm1.TSMFileExtension {
			tsmPaths = append(tsmPaths, p)
		}

		// All other non-tsm shard contents are skipped.
		return nil
	})

	if len(tsmPaths) == 0 {
		return fmt.Errorf("no tsm data found at %q", pth)
	}

	var processed bool
	for _, tsmPath := range tsmPaths {
		fd, err := os.Open(tsmPath)
		if err != nil {
			return err
		}

		r, err := tsm1.NewTSMReader(fd)
		if err != nil {
			fd.Close()
			return err
		}

		tsmMin, tsmMax := r.TimeRange()
		if !r.OverlapsTimeRange(m.From, m.To) {
			fmt.Fprintf(m.verboseStdout, "Skipping out-of-range (min-time: %v, max-time: %v) TSM file at path %q\n",
				time.Unix(0, tsmMin), time.Unix(0, tsmMax), tsmPath)
			if err := r.Close(); err != nil {
				return err
			}
			fd.Close()
			continue
		}

		processed = true // the generation needs to be incremented

		now := time.Now()
		// Entire TSM file is within the imported time range; copy all block data
		// and rewrite TSM index.
		if tsmMin >= m.From && tsmMax <= m.To {
			if err := m.processTSMFileFast(r, fd, bucketID); err != nil {
				r.Close()
				fd.Close() // flushes buffer before close
				return fmt.Errorf("error processing TSM file %q: %v", tsmPath, err)
			}
			if err := r.Close(); err != nil {
				return err
			}
			continue
		}

		if err := m.processTSMFile(r); err != nil {
			r.Close()
			return fmt.Errorf("error processing TSM file %q: %v", tsmPath, err)
		}
		fmt.Fprintf(m.verboseStdout, "Processed TSM file: %s in %v\n", tsmPath, time.Since(now))
		if err := r.Close(); err != nil {
			return err
		}
	}

	// Before returning we need to increase the generation to map the next shard
	// and ensure the TSM files don't clash with this one.
	if processed {
		// Determine how much to move increase the generation by looking at the
		// number of generations in the shard.
		minGen, _, err := tsm1.DefaultParseFileName(tsmPaths[0])
		if err != nil {
			return err
		}

		maxGen, _, err := tsm1.DefaultParseFileName(tsmPaths[len(tsmPaths)-1])
		if err != nil {
			return err
		}

		m.current2xTSMGen += maxGen - minGen + 1
	}

	return nil
}

func (m *Migrator) processTSMFile(r *tsm1.TSMReader) error {
	// TODO - support processing a partial TSM file.
	//
	// 0) Figure out destination TSM filename - see processTSMFileFast for how to do that.
	// 1) For each block in the file - check the min/max time on the block (using the TSM index) overlap;
	// 2) If they overlap completely then you can write the entire block (easy);
	// 3) Otherwise, decompress the block and scan the timestamps - reject the portion(s) of the block that don't overlap;
	// 4) Compress the new block back up and write it out
	// 5) Re-sort the TSM index, removing any entries where you rejected the entire block. (sort1xTSMKeys will sort the keys properly for you).

	panic("not yet implemented")
}

// processTSMFileFast processes all blocks in the provided TSM file, because all
// TSM data in the file is within the time range being imported.
func (m *Migrator) processTSMFileFast(r *tsm1.TSMReader, fi *os.File, bucketID influxdb.ID) (err error) {
	gen, seq, err := tsm1.DefaultParseFileName(r.Path())
	if err != nil {
		return err
	}

	name := tsm1.DefaultFormatFileName(m.current2xTSMGen+gen-1, seq)
	newPath := filepath.Join(m.DestPath, name+"."+tsm1.TSMFileExtension+importTempExtension)

	if m.DryRun {
		fmt.Fprintf(m.Stdout, "Migrating %s --> %s\n", r.Path(), newPath)
		return nil
	}

	fo, err := writeCloser(r.Path(), newPath)
	if err != nil {
		return err
	}

	// If there is no error writing the file then remove the .tmp extension.
	defer func() {
		fo.Close()
		if err == nil {
			// Rename import file.
			finalPath := strings.TrimSuffix(newPath, importTempExtension)
			if err2 := fs.RenameFile(newPath, finalPath); err2 != nil {
				err = err2
				return
			}
			fmt.Fprintf(m.Stdout, "Migrated %s --> %s\n", r.Path(), finalPath)
		}
	}()

	// Determine end of block by reading index offset.
	indexOffset, err := indexOffset(fi)
	if err != nil {
		return err
	}

	// Return to beginning of file and copy the header and all block data to
	// new file.
	if _, err = fi.Seek(0, io.SeekStart); err != nil {
		return err
	}

	n, err := io.CopyN(fo, fi, int64(indexOffset))
	if err != nil {
		return err
	} else if n != int64(indexOffset) {
		return fmt.Errorf("short read of block data. Read %d/%d bytes", n, indexOffset)
	}

	// Gather keys - need to materialise them all because they have to be re-sorted
	keys := make([][]byte, 0, 1000)
	itr := r.Iterator(nil)
	for itr.Next() {
		keys = append(keys, itr.Key())
	}
	if itr.Err() != nil {
		return itr.Err()
	}

	// Sort 1.x TSM keys according to their new 2.x values.
	// Don't allocate the new keys though, otherwise you're doubling the heap
	// requirements for this file's index, which could be ~2GB * 2.
	sort1xTSMKeys(keys)

	// Rewrite TSM index into new file.
	var tagsBuf models.Tags // Buffer to use for each series.
	var oldM []byte
	var seriesKeyBuf []byte // Buffer to use for new series key.
	var entriesBuf []tsm1.IndexEntry
	newM := tsdb.EncodeName(m.DestOrg, bucketID)

	for _, tsmKey := range keys {
		sKey1x, fKey := tsm1.SeriesAndFieldFromCompositeKey(tsmKey)
		oldM, tagsBuf = models.ParseKeyBytesWithTags(sKey1x, tagsBuf)

		// Rewrite the measurement and tags.
		sKey2x := rewriteSeriesKey(oldM, newM[:], fKey, tagsBuf, seriesKeyBuf[:0])

		// The key is not in a TSM format. Convert it to TSM format.
		sKey2x = append(sKey2x, tsmKeyFieldSeparator1xBytes...)
		sKey2x = append(sKey2x, fKey...)

		// Write the entries for the key back into new file.
		if entriesBuf, err = r.ReadEntries(tsmKey, entriesBuf[:0]); err != nil {
			return fmt.Errorf("unable to get entries for key %q. Error: %v", tsmKey, err)
		}

		typ, err := r.Type(tsmKey) // TODO(edd): could capture type during previous iterator out of this loop
		if err != nil {
			return fmt.Errorf("unable to get type for key %q. Error: %v", tsmKey, err)
		}

		if err := writeIndexEntries(fo, sKey2x, typ, entriesBuf); err != nil {
			return err
		}
	}

	// Write Footer.
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], indexOffset)
	_, err = fo.Write(buf[:])
	return err
}

var (
	sortTSMKeysBufFirst  []byte
	sortTSMKeysBufSecond []byte
)

// sort1xTSMKeys sorts 1.x TSM keys lexicographically as if they were 2.x TSM keys.
//
// It is not safe to call sort1xTSMKeys concurrently because it uses shared
// buffers to reduce allocations.
func sort1xTSMKeys(keys [][]byte) {
	sort.SliceStable(keys, func(i, j int) bool {
		firstCutIdx := bytes.Index(keys[i], tsmKeyFieldSeparator1xBytes)
		secondCutIdx := bytes.Index(keys[j], tsmKeyFieldSeparator1xBytes)

		if cap(sortTSMKeysBufFirst) < firstCutIdx+1 {
			sortTSMKeysBufFirst = append(sortTSMKeysBufFirst, make([]byte, firstCutIdx-len(sortTSMKeysBufFirst)+1)...)
		}
		sortTSMKeysBufFirst = sortTSMKeysBufFirst[:firstCutIdx+1]
		copy(sortTSMKeysBufFirst, keys[i][:firstCutIdx])
		sortTSMKeysBufFirst[len(sortTSMKeysBufFirst)-1] = ','

		if cap(sortTSMKeysBufSecond) < secondCutIdx+1 {
			sortTSMKeysBufSecond = append(sortTSMKeysBufSecond, make([]byte, secondCutIdx-len(sortTSMKeysBufSecond)+1)...)
		}
		sortTSMKeysBufSecond = sortTSMKeysBufSecond[:secondCutIdx+1]
		copy(sortTSMKeysBufSecond, keys[j][:secondCutIdx])
		sortTSMKeysBufSecond[len(sortTSMKeysBufSecond)-1] = ','

		return bytes.Compare(
			append(append(sortTSMKeysBufFirst, models.FieldKeyTagKeyBytes...), keys[i][firstCutIdx+len(tsmKeyFieldSeparator1x):]...),
			append(append(sortTSMKeysBufSecond, models.FieldKeyTagKeyBytes...), keys[j][secondCutIdx+len(tsmKeyFieldSeparator1x):]...),
		) < 0
	})
}

var tsmKeyFieldSeparator1xBytes = []byte(tsmKeyFieldSeparator1x)

func writeIndexEntries(w io.Writer, key []byte, typ byte, entries []tsm1.IndexEntry) error {
	var buf [5 + tsmIndexEntrySize1x]byte
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(key)))
	buf[2] = typ
	binary.BigEndian.PutUint16(buf[3:5], uint16(len(entries)))

	// Write the key length.
	if _, err := w.Write(buf[0:2]); err != nil {
		return fmt.Errorf("write: writer key length error: %v", err)
	}

	// Write the key.
	if _, err := w.Write(key); err != nil {
		return fmt.Errorf("write: writer key error: %v", err)
	}

	// Write the block type and count
	if _, err := w.Write(buf[2:5]); err != nil {
		return fmt.Errorf("write: writer block type and count error: %v", err)
	}

	// Write each index entry for all blocks for this key
	for _, entry := range entries {
		entry.AppendTo(buf[5:])
		n, err := w.Write(buf[5:])
		if err != nil {
			return err
		} else if n != tsmIndexEntrySize1x {
			return fmt.Errorf("incorrect number of bytes written for entry: %d", n)
		}
	}
	return nil
}

// rewriteSeriesKey takes a 1.x tsm series key and rewrites it to
// a 2.x format by including the `_m`, `_f` tag pairs and a new measurement
// comprising the org/bucket id.
func rewriteSeriesKey(oldM, newM []byte, fkey []byte, tags models.Tags, buf []byte) []byte {
	// Add the `_f` and `_m` tags.
	tags = append(tags, models.Tag{}, models.Tag{}) // Make room for two new tags.
	copy(tags[1:], tags)                            // Copy existing tags down.
	tags[0] = models.NewTag(models.MeasurementTagKeyBytes, oldM)
	tags[len(tags)-1] = models.NewTag(models.FieldKeyTagKeyBytes, fkey)
	// Create a new series key using the new measurement name and tags.
	return models.AppendMakeKey(buf, newM, tags)
}

func walkShardDirs(root string, fn func(db, rp, path string) error) error {
	type location struct {
		db, rp, path string
		id           int
	}

	dirs := map[string]location{}
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}
		if filepath.Ext(info.Name()) == "."+tsm1.TSMFileExtension {
			shardDir := filepath.Dir(path)

			id, err := strconv.Atoi(filepath.Base(shardDir))
			if err != nil || id < 1 {
				return fmt.Errorf("not a valid shard dir: %v", shardDir)
			}

			absPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			parts := strings.Split(absPath, string(filepath.Separator))
			db, rp := parts[len(parts)-4], parts[len(parts)-3]
			dirs[shardDir] = location{db: db, rp: rp, id: id, path: shardDir}
			return nil
		}
		return nil
	}); err != nil {
		return err
	}

	dirsSlice := make([]location, 0, len(dirs))
	for _, v := range dirs {
		dirsSlice = append(dirsSlice, v)
	}

	sort.Slice(dirsSlice, func(i, j int) bool {
		return dirsSlice[i].id < dirsSlice[j].id
	})

	for _, shard := range dirs {
		if err := fn(shard.db, shard.rp, shard.path); err != nil {
			return err
		}
	}
	return nil
}

// writeCloser initialises an io.WriteCloser for writing a new TSM file.
func writeCloser(src, dst string) (io.WriteCloser, error) {
	fd, err := os.Create(dst)
	if err != nil {
		return nil, err
	}

	w := bufio.NewWriterSize(fd, 1<<20)
	return w, nil
}

// indexOffset returns the offset to the TSM index of the provided file, which
// must be a valid TSM file.
func indexOffset(fd *os.File) (uint64, error) {
	_, err := fd.Seek(-8, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 8)
	n, err := fd.Read(buf)
	if err != nil {
		return 0, err
	} else if n != 8 {
		return 0, fmt.Errorf("short read of index offset on file %q", fd.Name())
	}

	return binary.BigEndian.Uint64(buf), nil
}
