package seriesfile

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/binaryutil"
	"github.com/influxdata/influxdb/v2/pkg/lifecycle"
	"github.com/influxdata/influxdb/v2/pkg/mincore"
	"github.com/influxdata/influxdb/v2/pkg/rhh"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	ErrSeriesFileClosed         = errors.New("tsdb: series file closed")
	ErrInvalidSeriesPartitionID = errors.New("tsdb: invalid series partition id")
)

const (
	// SeriesFilePartitionN is the number of partitions a series file is split into.
	SeriesFilePartitionN = 8
)

// SeriesFile represents the section of the index that holds series data.
type SeriesFile struct {
	mu  sync.Mutex // protects concurrent open and close
	res lifecycle.Resource

	path       string
	partitions []*SeriesPartition

	// N.B we have many partitions, but they must share the same metrics, so the
	// metrics are managed in a single shared package variable and
	// each partition decorates the same metric measurements with different
	// partition id label values.
	defaultMetricLabels prometheus.Labels
	metricsEnabled      bool

	pageFaultLimiter *rate.Limiter // Limits page faults by the series file

	LargeWriteThreshold int

	Logger *zap.Logger
}

// NewSeriesFile returns a new instance of SeriesFile.
func NewSeriesFile(path string) *SeriesFile {
	return &SeriesFile{
		path:           path,
		metricsEnabled: true,
		Logger:         zap.NewNop(),

		LargeWriteThreshold: DefaultLargeSeriesWriteThreshold,
	}
}

// WithLogger sets the logger on the SeriesFile and all underlying partitions. It must be called before Open.
func (f *SeriesFile) WithLogger(log *zap.Logger) {
	f.Logger = log.With(zap.String("service", "series-file"))
}

// SetDefaultMetricLabels sets the default labels for metrics on the Series File.
// It must be called before the SeriesFile is opened.
func (f *SeriesFile) SetDefaultMetricLabels(labels prometheus.Labels) {
	f.defaultMetricLabels = make(prometheus.Labels, len(labels))
	for k, v := range labels {
		f.defaultMetricLabels[k] = v
	}
}

// DisableMetrics ensures that activity is not collected via the prometheus metrics.
// DisableMetrics must be called before Open.
func (f *SeriesFile) DisableMetrics() {
	f.metricsEnabled = false
}

// WithPageFaultLimiter sets a limiter to restrict the number of page faults.
func (f *SeriesFile) WithPageFaultLimiter(limiter *rate.Limiter) {
	f.pageFaultLimiter = limiter
}

// Open memory maps the data file at the file's path.
func (f *SeriesFile) Open(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.res.Opened() {
		return errors.New("series file already opened")
	}

	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	_, logEnd := logger.NewOperation(ctx, f.Logger, "Opening Series File", "series_file_open", zap.String("path", f.path))
	defer logEnd()

	// Create path if it doesn't exist.
	if err := os.MkdirAll(filepath.Join(f.path), 0777); err != nil {
		return err
	}

	// Initialise metrics for trackers.
	mmu.Lock()
	if sms == nil && f.metricsEnabled {
		sms = newSeriesFileMetrics(f.defaultMetricLabels)
	}
	if ims == nil && f.metricsEnabled {
		// Make a copy of the default labels so that another label can be provided.
		labels := make(prometheus.Labels, len(f.defaultMetricLabels))
		for k, v := range f.defaultMetricLabels {
			labels[k] = v
		}
		labels["series_file_partition"] = "" // All partitions have this label.
		ims = rhh.NewMetrics(namespace, seriesFileSubsystem+"_index", labels)
	}
	mmu.Unlock()

	// Open partitions.
	f.partitions = make([]*SeriesPartition, 0, SeriesFilePartitionN)
	for i := 0; i < SeriesFilePartitionN; i++ {
		// TODO(edd): These partition initialisation should be moved up to NewSeriesFile.
		p := NewSeriesPartition(i, f.SeriesPartitionPath(i))
		p.LargeWriteThreshold = f.LargeWriteThreshold
		p.Logger = f.Logger.With(zap.Int("partition", p.ID()))
		p.pageFaultLimiter = f.pageFaultLimiter

		// For each series file index, rhh trackers are used to track the RHH Hashmap.
		// Each of the trackers needs to be given slightly different default
		// labels to ensure the correct partition_ids are set as labels.
		labels := make(prometheus.Labels, len(f.defaultMetricLabels))
		for k, v := range f.defaultMetricLabels {
			labels[k] = v
		}
		labels["series_file_partition"] = fmt.Sprint(p.ID())

		p.index.rhhMetrics = ims
		p.index.rhhLabels = labels
		p.index.rhhMetricsEnabled = f.metricsEnabled

		// Set the metric trackers on the partition with any injected default labels.
		p.tracker = newSeriesPartitionTracker(sms, labels)
		p.tracker.enabled = f.metricsEnabled

		if err := p.Open(); err != nil {
			f.Logger.Error("Unable to open series file",
				zap.String("path", f.path),
				zap.Int("partition", p.ID()),
				zap.Error(err))
			f.closeNoLock()
			return err
		}
		f.partitions = append(f.partitions, p)
	}

	// The resource is now open.
	f.res.Open()

	return nil
}

func (f *SeriesFile) closeNoLock() (err error) {
	// Close the resource and wait for any outstanding references.
	f.res.Close()

	var errs []error
	for _, p := range f.partitions {
		errs = append(errs, p.Close())
	}
	return multierr.Combine(errs...)
}

// Close unmaps the data file.
func (f *SeriesFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeNoLock()
}

// Path returns the path to the file.
func (f *SeriesFile) Path() string { return f.path }

// SeriesPartitionPath returns the path to a given partition.
func (f *SeriesFile) SeriesPartitionPath(i int) string {
	return filepath.Join(f.path, fmt.Sprintf("%02x", i))
}

// Partitions returns all partitions.
func (f *SeriesFile) Partitions() []*SeriesPartition { return f.partitions }

// Acquire ensures that the series file won't be closed until after the reference
// has been released.
func (f *SeriesFile) Acquire() (*lifecycle.Reference, error) {
	return f.res.Acquire()
}

// EnableCompactions allows compactions to run.
func (f *SeriesFile) EnableCompactions() {
	for _, p := range f.partitions {
		p.EnableCompactions()
	}
}

// DisableCompactions prevents new compactions from running.
func (f *SeriesFile) DisableCompactions() {
	for _, p := range f.partitions {
		p.DisableCompactions()
	}
}

// FileSize returns the size of all partitions, in bytes.
func (f *SeriesFile) FileSize() (n int64, err error) {
	for _, p := range f.partitions {
		v, err := p.FileSize()
		n += v
		if err != nil {
			return n, err
		}
	}
	return n, err
}

// CreateSeriesListIfNotExists creates a list of series in bulk if they don't exist. It overwrites
// the collection's Keys and SeriesIDs fields. The collection's SeriesIDs slice will have IDs for
// every name+tags, creating new series IDs as needed. If any SeriesID is zero, then a type
// conflict has occurred for that series.
func (f *SeriesFile) CreateSeriesListIfNotExists(collection *tsdb.SeriesCollection) error {
	collection.SeriesKeys = GenerateSeriesKeys(collection.Names, collection.Tags)
	collection.SeriesIDs = make([]tsdb.SeriesID, len(collection.SeriesKeys))
	keyPartitionIDs := f.SeriesKeysPartitionIDs(collection.SeriesKeys)

	var g errgroup.Group
	for i := range f.partitions {
		p := f.partitions[i]
		g.Go(func() error {
			return p.CreateSeriesListIfNotExists(collection, keyPartitionIDs)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	collection.ApplyConcurrentDrops()
	return nil
}

// DeleteSeriesID flags a list of series as permanently deleted.
// If a series is reintroduced later then it must create a new id.
func (f *SeriesFile) DeleteSeriesIDs(ids []tsdb.SeriesID) error {
	m := make(map[int][]tsdb.SeriesID)
	for _, id := range ids {
		partitionID := f.SeriesIDPartitionID(id)
		m[partitionID] = append(m[partitionID], id)
	}

	var g errgroup.Group
	for partitionID, partitionIDs := range m {
		partitionID, partitionIDs := partitionID, partitionIDs
		g.Go(func() error { return f.partitions[partitionID].DeleteSeriesIDs(partitionIDs) })
	}
	return g.Wait()
}

// IsDeleted returns true if the ID has been deleted before.
func (f *SeriesFile) IsDeleted(id tsdb.SeriesID) bool {
	p := f.SeriesIDPartition(id)
	if p == nil {
		return false
	}
	return p.IsDeleted(id)
}

// SeriesKey returns the series key for a given id.
func (f *SeriesFile) SeriesKey(id tsdb.SeriesID) []byte {
	if id.IsZero() {
		return nil
	}
	p := f.SeriesIDPartition(id)
	if p == nil {
		return nil
	}
	return p.SeriesKey(id)
}

// SeriesKeyName returns the measurement name for a series id.
func (f *SeriesFile) SeriesKeyName(id tsdb.SeriesID) []byte {
	if id.IsZero() {
		return nil
	}
	data := f.SeriesIDPartition(id).SeriesKey(id)
	if data == nil {
		return nil
	}
	_, data = ReadSeriesKeyLen(data)
	name, _ := ReadSeriesKeyMeasurement(data)
	return name
}

// SeriesKeys returns a list of series keys from a list of ids.
func (f *SeriesFile) SeriesKeys(ids []tsdb.SeriesID) [][]byte {
	keys := make([][]byte, len(ids))
	for i := range ids {
		keys[i] = f.SeriesKey(ids[i])
	}
	return keys
}

// Series returns the parsed series name and tags for an offset.
func (f *SeriesFile) Series(id tsdb.SeriesID) ([]byte, models.Tags) {
	key := f.SeriesKey(id)
	if key == nil {
		return nil, nil
	}
	return ParseSeriesKey(key)
}

// SeriesID returns the series id for the series.
func (f *SeriesFile) SeriesID(name []byte, tags models.Tags, buf []byte) tsdb.SeriesID {
	return f.SeriesIDTyped(name, tags, buf).SeriesID()
}

// SeriesIDTyped returns the typed series id for the series.
func (f *SeriesFile) SeriesIDTyped(name []byte, tags models.Tags, buf []byte) tsdb.SeriesIDTyped {
	key := AppendSeriesKey(buf[:0], name, tags)
	return f.SeriesIDTypedBySeriesKey(key)
}

// SeriesIDTypedBySeriesKey returns the typed series id for the series.
func (f *SeriesFile) SeriesIDTypedBySeriesKey(key []byte) tsdb.SeriesIDTyped {
	keyPartition := f.SeriesKeyPartition(key)
	if keyPartition == nil {
		return tsdb.SeriesIDTyped{}
	}
	return keyPartition.FindIDTypedBySeriesKey(key)
}

// HasSeries return true if the series exists.
func (f *SeriesFile) HasSeries(name []byte, tags models.Tags, buf []byte) bool {
	return !f.SeriesID(name, tags, buf).IsZero()
}

// SeriesCount returns the number of series.
func (f *SeriesFile) SeriesCount() uint64 {
	var n uint64
	for _, p := range f.partitions {
		n += p.SeriesCount()
	}
	return n
}

// SeriesIDs returns a slice of series IDs in all partitions, sorted.
// This may return a lot of data at once, so use sparingly.
func (f *SeriesFile) SeriesIDs() []tsdb.SeriesID {
	var ids []tsdb.SeriesID
	for _, p := range f.partitions {
		ids = p.AppendSeriesIDs(ids)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Less(ids[j]) })
	return ids
}

func (f *SeriesFile) SeriesIDPartitionID(id tsdb.SeriesID) int {
	return int((id.RawID() - 1) % SeriesFilePartitionN)
}

func (f *SeriesFile) SeriesIDPartition(id tsdb.SeriesID) *SeriesPartition {
	partitionID := f.SeriesIDPartitionID(id)
	if partitionID >= len(f.partitions) {
		return nil
	}
	return f.partitions[partitionID]
}

func (f *SeriesFile) SeriesKeysPartitionIDs(keys [][]byte) []int {
	partitionIDs := make([]int, len(keys))
	for i := range keys {
		partitionIDs[i] = f.SeriesKeyPartitionID(keys[i])
	}
	return partitionIDs
}

func (f *SeriesFile) SeriesKeyPartitionID(key []byte) int {
	return int(xxhash.Sum64(key) % SeriesFilePartitionN)
}

func (f *SeriesFile) SeriesKeyPartition(key []byte) *SeriesPartition {
	partitionID := f.SeriesKeyPartitionID(key)
	if partitionID >= len(f.partitions) {
		return nil
	}
	return f.partitions[partitionID]
}

// AppendSeriesKey serializes name and tags to a byte slice.
// The total length is prepended as a uvarint.
func AppendSeriesKey(dst []byte, name []byte, tags models.Tags) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	origLen := len(dst)

	// The tag count is variable encoded, so we need to know ahead of time what
	// the size of the tag count value will be.
	tcBuf := make([]byte, binary.MaxVarintLen64)
	tcSz := binary.PutUvarint(tcBuf, uint64(len(tags)))

	// Size of name/tags. Does not include total length.
	size := 0 + //
		2 + // size of measurement
		len(name) + // measurement
		tcSz + // size of number of tags
		(4 * len(tags)) + // length of each tag key and value
		tags.Size() // size of tag keys/values

	// Variable encode length.
	totalSz := binary.PutUvarint(buf, uint64(size))

	// If caller doesn't provide a buffer then pre-allocate an exact one.
	if dst == nil {
		dst = make([]byte, 0, size+totalSz)
	}

	// Append total length.
	dst = append(dst, buf[:totalSz]...)

	// Append name.
	binary.BigEndian.PutUint16(buf, uint16(len(name)))
	dst = append(dst, buf[:2]...)
	dst = append(dst, name...)

	// Append tag count.
	dst = append(dst, tcBuf[:tcSz]...)

	// Append tags.
	for _, tag := range tags {
		binary.BigEndian.PutUint16(buf, uint16(len(tag.Key)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Key...)

		binary.BigEndian.PutUint16(buf, uint16(len(tag.Value)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Value...)
	}

	// Verify that the total length equals the encoded byte count.
	if got, exp := len(dst)-origLen, size+totalSz; got != exp {
		panic(fmt.Sprintf("series key encoding does not match calculated total length: actual=%d, exp=%d, key=%x", got, exp, dst))
	}

	return dst
}

// ReadSeriesKey returns the series key from the beginning of the buffer.
func ReadSeriesKey(data []byte) (key, remainder []byte) {
	sz, n := binary.Uvarint(data)
	return data[:int(sz)+n], data[int(sz)+n:]
}

func ReadSeriesKeyLen(data []byte) (sz int, remainder []byte) {
	sz64, i := binary.Uvarint(data)
	return int(sz64), data[i:]
}

func ReadSeriesKeyMeasurement(data []byte) (name, remainder []byte) {
	n, data := binary.BigEndian.Uint16(data), data[2:]
	return data[:n], data[n:]
}

func ReadSeriesKeyTagN(data []byte) (n int, remainder []byte) {
	n64, i := binary.Uvarint(data)
	return int(n64), data[i:]
}

func ReadSeriesKeyTag(data []byte) (key, value, remainder []byte) {
	n, data := binary.BigEndian.Uint16(data), data[2:]
	key, data = data[:n], data[n:]

	n, data = binary.BigEndian.Uint16(data), data[2:]
	value, data = data[:n], data[n:]
	return key, value, data
}

// ParseSeriesKey extracts the name & tags from a series key.
func ParseSeriesKey(data []byte) (name []byte, tags models.Tags) {
	return parseSeriesKey(data, nil)
}

// ParseSeriesKeyInto extracts the name and tags for data, parsing the tags into
// dstTags, which is then returened.
//
// The returned dstTags may have a different length and capacity.
func ParseSeriesKeyInto(data []byte, dstTags models.Tags) ([]byte, models.Tags) {
	return parseSeriesKey(data, dstTags)
}

// parseSeriesKey extracts the name and tags from data, attempting to re-use the
// provided tags value rather than allocating. The returned tags may have a
// different length and capacity to those provided.
func parseSeriesKey(data []byte, dst models.Tags) ([]byte, models.Tags) {
	var name []byte
	_, data = ReadSeriesKeyLen(data)
	name, data = ReadSeriesKeyMeasurement(data)
	tagN, data := ReadSeriesKeyTagN(data)

	dst = dst[:cap(dst)] // Grow dst to use full capacity
	if got, want := len(dst), tagN; got < want {
		dst = append(dst, make(models.Tags, want-got)...)
	} else if got > want {
		dst = dst[:want]
	}
	dst = dst[:tagN]

	for i := 0; i < tagN; i++ {
		var key, value []byte
		key, value, data = ReadSeriesKeyTag(data)
		dst[i].Key, dst[i].Value = key, value
	}

	return name, dst
}

func CompareSeriesKeys(a, b []byte) int {
	// Handle 'nil' keys.
	if len(a) == 0 && len(b) == 0 {
		return 0
	} else if len(a) == 0 {
		return -1
	} else if len(b) == 0 {
		return 1
	}

	// Read total size.
	_, a = ReadSeriesKeyLen(a)
	_, b = ReadSeriesKeyLen(b)

	// Read names.
	name0, a := ReadSeriesKeyMeasurement(a)
	name1, b := ReadSeriesKeyMeasurement(b)

	// Compare names, return if not equal.
	if cmp := bytes.Compare(name0, name1); cmp != 0 {
		return cmp
	}

	// Read tag counts.
	tagN0, a := ReadSeriesKeyTagN(a)
	tagN1, b := ReadSeriesKeyTagN(b)

	// Compare each tag in order.
	for i := 0; ; i++ {
		// Check for EOF.
		if i == tagN0 && i == tagN1 {
			return 0
		} else if i == tagN0 {
			return -1
		} else if i == tagN1 {
			return 1
		}

		// Read keys.
		var key0, key1, value0, value1 []byte
		key0, value0, a = ReadSeriesKeyTag(a)
		key1, value1, b = ReadSeriesKeyTag(b)

		// Compare keys & values.
		if cmp := bytes.Compare(key0, key1); cmp != 0 {
			return cmp
		} else if cmp := bytes.Compare(value0, value1); cmp != 0 {
			return cmp
		}
	}
}

// GenerateSeriesKeys generates series keys for a list of names & tags using
// a single large memory block.
func GenerateSeriesKeys(names [][]byte, tagsSlice []models.Tags) [][]byte {
	buf := make([]byte, 0, SeriesKeysSize(names, tagsSlice))
	keys := make([][]byte, len(names))
	for i := range names {
		offset := len(buf)
		buf = AppendSeriesKey(buf, names[i], tagsSlice[i])
		keys[i] = buf[offset:]
	}
	return keys
}

// SeriesKeysSize returns the number of bytes required to encode a list of name/tags.
func SeriesKeysSize(names [][]byte, tagsSlice []models.Tags) int {
	var n int
	for i := range names {
		n += SeriesKeySize(names[i], tagsSlice[i])
	}
	return n
}

// SeriesKeySize returns the number of bytes required to encode a series key.
func SeriesKeySize(name []byte, tags models.Tags) int {
	var n int
	n += 2 + len(name)
	n += binaryutil.UvarintSize(uint64(len(tags)))
	for _, tag := range tags {
		n += 2 + len(tag.Key)
		n += 2 + len(tag.Value)
	}
	n += binaryutil.UvarintSize(uint64(n))
	return n
}

// wait rate limits page faults to the underlying data. Skipped if limiter is not set.
func wait(limiter *mincore.Limiter, b []byte) error {
	if limiter == nil {
		return nil
	}
	return limiter.WaitRange(context.Background(), b)
}
