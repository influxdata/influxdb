package seriesfile

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/pkg/rhh"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	ErrSeriesPartitionClosed              = errors.New("tsdb: series partition closed")
	ErrSeriesPartitionCompactionCancelled = errors.New("tsdb: series partition compaction cancelled")
)

// DefaultSeriesPartitionCompactThreshold is the number of series IDs to hold in the in-memory
// series map before compacting and rebuilding the on-disk representation.
const DefaultSeriesPartitionCompactThreshold = 1 << 17 // 128K

// SeriesPartition represents a subset of series file data.
type SeriesPartition struct {
	mu   sync.RWMutex
	wg   sync.WaitGroup
	id   int
	path string

	closed  bool
	closing chan struct{}
	once    sync.Once

	segments []*SeriesSegment
	index    *SeriesIndex
	seq      uint64 // series id sequence

	compacting          bool
	compactionsDisabled int

	pageFaultLimiter *rate.Limiter // Limits page faults by the partition

	CompactThreshold    int
	LargeWriteThreshold int

	tracker *seriesPartitionTracker
	Logger  *zap.Logger
}

// NewSeriesPartition returns a new instance of SeriesPartition.
func NewSeriesPartition(id int, path string) *SeriesPartition {
	p := &SeriesPartition{
		id:                  id,
		path:                path,
		closing:             make(chan struct{}),
		CompactThreshold:    DefaultSeriesPartitionCompactThreshold,
		LargeWriteThreshold: DefaultLargeSeriesWriteThreshold,
		tracker:             newSeriesPartitionTracker(newSeriesFileMetrics(nil), prometheus.Labels{"series_file_partition": fmt.Sprint(id)}),
		Logger:              zap.NewNop(),
		seq:                 uint64(id) + 1,
	}
	p.index = NewSeriesIndex(p.IndexPath())
	return p
}

// Open memory maps the data file at the partition's path.
func (p *SeriesPartition) Open() error {
	if p.closed {
		return errors.New("tsdb: cannot reopen series partition")
	}

	// Create path if it doesn't exist.
	if err := os.MkdirAll(filepath.Join(p.path), 0777); err != nil {
		return err
	}

	// Open components.
	if err := func() (err error) {
		if err := p.openSegments(); err != nil {
			return err
		}
		// Init last segment for writes.
		if err := p.activeSegment().InitForWrite(); err != nil {
			return err
		}

		if err := p.index.Open(); err != nil {
			return err
		}
		p.index.SetPageFaultLimiter(p.pageFaultLimiter, p.tracker.AddPageFaults)

		if err = p.index.Recover(p.segments); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		p.Close()
		return err
	}

	p.tracker.SetSeries(p.index.Count()) // Set series count metric.
	p.tracker.SetDiskSize(p.DiskSize())  // Set on-disk size metric.
	return nil
}

func (p *SeriesPartition) openSegments() error {
	fis, err := ioutil.ReadDir(p.path)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		segmentID, err := ParseSeriesSegmentFilename(fi.Name())
		if err != nil {
			continue
		}

		segment := NewSeriesSegment(segmentID, filepath.Join(p.path, fi.Name()))
		if err := segment.Open(); err != nil {
			return err
		}
		segment.SetPageFaultLimiter(p.pageFaultLimiter, p.tracker.AddPageFaults)
		p.segments = append(p.segments, segment)
	}

	// Find max series id by searching segments in reverse order.
	for i := len(p.segments) - 1; i >= 0; i-- {
		if seq := p.segments[i].MaxSeriesID(); seq.RawID() >= p.seq {
			// Reset our sequence num to the next one to assign
			p.seq = seq.RawID() + SeriesFilePartitionN
			break
		}
	}

	// Create initial segment if none exist.
	if len(p.segments) == 0 {
		segment, err := CreateSeriesSegment(0, filepath.Join(p.path, "0000"))
		if err != nil {
			return err
		}
		segment.SetPageFaultLimiter(p.pageFaultLimiter, p.tracker.AddPageFaults)
		p.segments = append(p.segments, segment)
	}

	p.tracker.SetSegments(uint64(len(p.segments)))
	return nil
}

// Close unmaps the data files.
func (p *SeriesPartition) Close() (err error) {
	p.once.Do(func() { close(p.closing) })
	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true

	for _, s := range p.segments {
		if e := s.Close(); e != nil && err == nil {
			err = e
		}
	}
	p.segments = nil

	if p.index != nil {
		if e := p.index.Close(); e != nil && err == nil {
			err = e
		}
	}
	p.index = nil

	return err
}

// ID returns the partition id.
func (p *SeriesPartition) ID() int { return p.id }

// Path returns the path to the partition.
func (p *SeriesPartition) Path() string { return p.path }

// IndexPath returns the path to the series index.
func (p *SeriesPartition) IndexPath() string { return filepath.Join(p.path, "index") }

// Index returns the partition's index.
func (p *SeriesPartition) Index() *SeriesIndex { return p.index }

// Segments returns the segments in the partition.
func (p *SeriesPartition) Segments() []*SeriesSegment { return p.segments }

// FileSize returns the size of all partitions, in bytes.
func (p *SeriesPartition) FileSize() (n int64, err error) {
	for _, ss := range p.segments {
		fi, err := os.Stat(ss.Path())
		if err != nil {
			return 0, err
		}
		n += fi.Size()
	}
	return n, err
}

// CreateSeriesListIfNotExists creates a list of series in bulk if they don't exist.
// The ids parameter is modified to contain series IDs for all keys belonging to this partition.
// If the type does not match the existing type for the key, a zero id is stored.
func (p *SeriesPartition) CreateSeriesListIfNotExists(collection *tsdb.SeriesCollection, keyPartitionIDs []int) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrSeriesPartitionClosed
	}

	span, ctx := tracing.StartSpanFromContext(context.TODO())
	defer span.Finish()

	writeRequired := 0
	for iter := collection.Iterator(); iter.Next(); {
		index := iter.Index()
		if keyPartitionIDs[index] != p.id {
			continue
		}
		id := p.index.FindIDBySeriesKey(p.segments, iter.SeriesKey())
		if id.IsZero() {
			writeRequired++
			continue
		}
		if id.HasType() && id.Type() != iter.Type() {
			iter.Invalid(fmt.Sprintf(
				"series type mismatch: already %s but got %s",
				id.Type(), iter.Type()))
			continue
		}
		collection.SeriesIDs[index] = id.SeriesID()
	}
	p.mu.RUnlock()

	// Exit if all series for this partition already exist.
	if writeRequired == 0 {
		return nil
	}

	type keyRange struct {
		key    []byte
		id     tsdb.SeriesIDTyped
		offset int64
	}

	// Preallocate the space we'll need before grabbing the lock.
	newKeyRanges := make([]keyRange, 0, writeRequired)
	newIDs := make(map[string]tsdb.SeriesIDTyped, writeRequired)

	// Pre-grow index for large writes.
	if writeRequired >= p.LargeWriteThreshold {
		p.mu.Lock()
		p.index.GrowBy(writeRequired)
		p.mu.Unlock()
	}

	// Obtain write lock to create new series.
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrSeriesPartitionClosed
	}

	for iter := collection.Iterator(); iter.Next(); {
		index := iter.Index()

		// Skip series that don't belong to the partition or have already been created.
		if keyPartitionIDs[index] != p.id || !iter.SeriesID().IsZero() {
			continue
		}

		// Re-attempt lookup under write lock. Be sure to double check the type. If the type
		// doesn't match what we found, we should not set the ids field for it, but we should
		// stop processing the key.
		key, typ := iter.SeriesKey(), iter.Type()

		// First check the map, then the index.
		id := newIDs[string(key)]
		if id.IsZero() {
			id = p.index.FindIDBySeriesKey(p.segments, key)
		}

		// If the id is found, we are done processing this key. We should only set the ids slice
		// if the type matches.
		if !id.IsZero() {
			if id.HasType() && id.Type() != typ {
				iter.Invalid(fmt.Sprintf(
					"series type mismatch: already %s but got %s",
					id.Type(), iter.Type()))
				continue
			}
			collection.SeriesIDs[index] = id.SeriesID()
			continue
		}

		// Write to series log and save offset.
		id, offset, err := p.insert(key, typ)
		if err != nil {
			return err
		}

		// Append new key to be added to hash map after flush.
		collection.SeriesIDs[index] = id.SeriesID()
		newIDs[string(key)] = id
		newKeyRanges = append(newKeyRanges, keyRange{key, id, offset})
	}

	// Flush active segment writes so we can access data in mmap.
	if segment := p.activeSegment(); segment != nil {
		if err := segment.Flush(); err != nil {
			return err
		}
	}

	// Add keys to hash map(s).
	for _, keyRange := range newKeyRanges {
		p.index.Insert(keyRange.key, keyRange.id, keyRange.offset)
	}
	p.tracker.AddSeriesCreated(uint64(len(newKeyRanges))) // Track new series in metric.
	p.tracker.AddSeries(uint64(len(newKeyRanges)))

	// Check if we've crossed the compaction threshold.
	if p.compactionsEnabled() && !p.compacting && p.CompactThreshold != 0 && p.index.InMemCount() >= uint64(p.CompactThreshold) {
		p.compacting = true
		log, logEnd := logger.NewOperation(ctx, p.Logger, "Series partition compaction", "series_partition_compaction", zap.String("path", p.path))

		p.wg.Add(1)
		p.tracker.IncCompactionsActive()
		go func() {
			defer p.wg.Done()

			compactor := NewSeriesPartitionCompactor()
			compactor.cancel = p.closing
			duration, err := compactor.Compact(p)
			if err != nil {
				p.tracker.IncCompactionErr()
				log.Error("Series partition compaction failed", zap.Error(err))
			} else {
				p.tracker.IncCompactionOK(duration)
			}

			logEnd()

			// Clear compaction flag.
			p.mu.Lock()
			p.compacting = false
			p.mu.Unlock()
			p.tracker.DecCompactionsActive()

			// Disk size may have changed due to compaction.
			p.tracker.SetDiskSize(p.DiskSize())
		}()
	}

	return nil
}

// Compacting returns if the SeriesPartition is currently compacting.
func (p *SeriesPartition) Compacting() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.compacting
}

// DeleteSeriesID flags a list of series as permanently deleted.
// If a series is reintroduced later then it must create a new id.
func (p *SeriesPartition) DeleteSeriesIDs(ids []tsdb.SeriesID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrSeriesPartitionClosed
	}

	var n uint64
	for _, id := range ids {
		// Already tombstoned, ignore.
		if p.index.IsDeleted(id) {
			continue
		}

		// Write tombstone entries. The type is ignored in tombstones.
		_, err := p.writeLogEntry(AppendSeriesEntry(nil, SeriesEntryTombstoneFlag, id.WithType(models.Empty), nil))
		if err != nil {
			return err
		}
		n++
	}

	// Flush active segment write.
	if segment := p.activeSegment(); segment != nil {
		if err := segment.Flush(); err != nil {
			return err
		}
	}

	// Mark tombstone in memory.
	for _, id := range ids {
		p.index.Delete(id)
	}
	p.tracker.SubSeries(n)

	return nil
}

// IsDeleted returns true if the ID has been deleted before.
func (p *SeriesPartition) IsDeleted(id tsdb.SeriesID) bool {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return false
	}
	v := p.index.IsDeleted(id)
	p.mu.RUnlock()
	return v
}

// SeriesKey returns the series key for a given id.
func (p *SeriesPartition) SeriesKey(id tsdb.SeriesID) []byte {
	if id.IsZero() {
		return nil
	}
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil
	}
	key := p.seriesKeyByOffset(p.index.FindOffsetByID(id))
	p.mu.RUnlock()
	return key
}

// Series returns the parsed series name and tags for an offset.
func (p *SeriesPartition) Series(id tsdb.SeriesID) ([]byte, models.Tags) {
	key := p.SeriesKey(id)
	if key == nil {
		return nil, nil
	}
	return ParseSeriesKey(key)
}

// FindIDBySeriesKey return the series id for the series key.
func (p *SeriesPartition) FindIDBySeriesKey(key []byte) tsdb.SeriesID {
	return p.FindIDTypedBySeriesKey(key).SeriesID()
}

// FindIDTypedBySeriesKey return the typed series id for the series key.
func (p *SeriesPartition) FindIDTypedBySeriesKey(key []byte) tsdb.SeriesIDTyped {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return tsdb.SeriesIDTyped{}
	}
	id := p.index.FindIDBySeriesKey(p.segments, key)
	p.mu.RUnlock()
	return id
}

// SeriesCount returns the number of series.
func (p *SeriesPartition) SeriesCount() uint64 {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return 0
	}
	n := p.index.Count()
	p.mu.RUnlock()
	return n
}

// DiskSize returns the number of bytes taken up on disk by the partition.
func (p *SeriesPartition) DiskSize() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.diskSize()
}

func (p *SeriesPartition) diskSize() uint64 {
	totalSize := p.index.OnDiskSize()
	for _, segment := range p.segments {
		totalSize += uint64(len(segment.Data()))
	}
	return totalSize
}

func (p *SeriesPartition) DisableCompactions() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.compactionsDisabled++
}

func (p *SeriesPartition) EnableCompactions() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.compactionsEnabled() {
		return
	}
	p.compactionsDisabled--
}

func (p *SeriesPartition) compactionsEnabled() bool {
	return p.compactionsDisabled == 0
}

// AppendSeriesIDs returns a list of all series ids.
func (p *SeriesPartition) AppendSeriesIDs(a []tsdb.SeriesID) []tsdb.SeriesID {
	for _, segment := range p.segments {
		a = segment.AppendSeriesIDs(a)
	}
	return a
}

// activeSegment returns the last segment.
func (p *SeriesPartition) activeSegment() *SeriesSegment {
	if len(p.segments) == 0 {
		return nil
	}
	return p.segments[len(p.segments)-1]
}

func (p *SeriesPartition) insert(key []byte, typ models.FieldType) (id tsdb.SeriesIDTyped, offset int64, err error) {
	id = tsdb.NewSeriesID(p.seq).WithType(typ)
	offset, err = p.writeLogEntry(AppendSeriesEntry(nil, SeriesEntryInsertFlag, id, key))
	if err != nil {
		return tsdb.SeriesIDTyped{}, 0, err
	}

	p.seq += SeriesFilePartitionN
	return id, offset, nil
}

// writeLogEntry appends an entry to the end of the active segment.
// If there is no more room in the segment then a new segment is added.
func (p *SeriesPartition) writeLogEntry(data []byte) (offset int64, err error) {
	segment := p.activeSegment()
	if segment == nil || !segment.CanWrite(data) {
		if segment, err = p.createSegment(); err != nil {
			return 0, err
		}
	}
	return segment.WriteLogEntry(data)
}

// createSegment appends a new segment
func (p *SeriesPartition) createSegment() (*SeriesSegment, error) {
	// Close writer for active segment, if one exists.
	if segment := p.activeSegment(); segment != nil {
		if err := segment.CloseForWrite(); err != nil {
			return nil, err
		}
	}

	// Generate a new sequential segment identifier.
	var id uint16
	if len(p.segments) > 0 {
		id = p.segments[len(p.segments)-1].ID() + 1
	}
	filename := fmt.Sprintf("%04x", id)

	// Generate new empty segment.
	segment, err := CreateSeriesSegment(id, filepath.Join(p.path, filename))
	if err != nil {
		return nil, err
	}
	segment.SetPageFaultLimiter(p.pageFaultLimiter, p.tracker.AddPageFaults)
	p.segments = append(p.segments, segment)

	// Allow segment to write.
	if err := segment.InitForWrite(); err != nil {
		return nil, err
	}
	p.tracker.SetSegments(uint64(len(p.segments)))
	p.tracker.SetDiskSize(p.diskSize()) // Disk size will change with new segment.
	return segment, nil
}

func (p *SeriesPartition) seriesKeyByOffset(offset int64) []byte {
	if offset == 0 {
		return nil
	}

	segmentID, pos := SplitSeriesOffset(offset)
	for _, segment := range p.segments {
		if segment.ID() != segmentID {
			continue
		}

		buf := segment.Slice(pos + SeriesEntryHeaderSize)
		key, _ := ReadSeriesKey(buf)
		_ = wait(segment.limiter, buf[:len(key)])
		return key
	}

	return nil
}

type seriesPartitionTracker struct {
	metrics *seriesFileMetrics
	labels  prometheus.Labels
	enabled bool
}

func newSeriesPartitionTracker(metrics *seriesFileMetrics, defaultLabels prometheus.Labels) *seriesPartitionTracker {
	return &seriesPartitionTracker{
		metrics: metrics,
		labels:  defaultLabels,
		enabled: true,
	}
}

// Labels returns a copy of labels for use with Series File metrics.
func (t *seriesPartitionTracker) Labels() prometheus.Labels {
	l := make(map[string]string, len(t.labels))
	for k, v := range t.labels {
		l[k] = v
	}
	return l
}

// AddSeriesCreated increases the number of series created in the partition by n.
func (t *seriesPartitionTracker) AddSeriesCreated(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.SeriesCreated.With(labels).Add(float64(n))
}

// SetSeries sets the number of series in the partition.
func (t *seriesPartitionTracker) SetSeries(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Series.With(labels).Set(float64(n))
}

// AddSeries increases the number of series in the partition by n.
func (t *seriesPartitionTracker) AddSeries(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Series.With(labels).Add(float64(n))
}

// SubSeries decreases the number of series in the partition by n.
func (t *seriesPartitionTracker) SubSeries(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Series.With(labels).Sub(float64(n))
}

// SetDiskSize sets the number of bytes used by files for in partition.
func (t *seriesPartitionTracker) SetDiskSize(sz uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.DiskSize.With(labels).Set(float64(sz))
}

// SetSegments sets the number of segments files for the partition.
func (t *seriesPartitionTracker) SetSegments(n uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Segments.With(labels).Set(float64(n))
}

// AddPageFaults increases the number of page faults that occurred in the partition by n.
func (t *seriesPartitionTracker) AddPageFaults(n int) {
	if !t.enabled {
		return
	}
	t.metrics.PageFaults.With(t.Labels()).Add(float64(n))
}

// IncCompactionsActive increments the number of active compactions for the
// components of a partition (index and segments).
func (t *seriesPartitionTracker) IncCompactionsActive() {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["component"] = "index" // TODO(edd): when we add segment compactions we will add a new label value.
	t.metrics.CompactionsActive.With(labels).Inc()
}

// DecCompactionsActive decrements the number of active compactions for the
// components of a partition (index and segments).
func (t *seriesPartitionTracker) DecCompactionsActive() {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["component"] = "index" // TODO(edd): when we add segment compactions we will add a new label value.
	t.metrics.CompactionsActive.With(labels).Dec()
}

// incCompactions increments the number of compactions for the partition.
// Callers should use IncCompactionOK and IncCompactionErr.
func (t *seriesPartitionTracker) incCompactions(status string, duration time.Duration) {
	if !t.enabled {
		return
	}

	if duration > 0 {
		labels := t.Labels()
		labels["component"] = "index"
		t.metrics.CompactionDuration.With(labels).Observe(duration.Seconds())
	}

	labels := t.Labels()
	labels["status"] = status
	t.metrics.Compactions.With(labels).Inc()
}

// IncCompactionOK increments the number of successful compactions for the partition.
func (t *seriesPartitionTracker) IncCompactionOK(duration time.Duration) {
	t.incCompactions("ok", duration)
}

// IncCompactionErr increments the number of failed compactions for the partition.
func (t *seriesPartitionTracker) IncCompactionErr() { t.incCompactions("error", 0) }

// SeriesPartitionCompactor represents an object reindexes a series partition and optionally compacts segments.
type SeriesPartitionCompactor struct {
	cancel <-chan struct{}
}

// NewSeriesPartitionCompactor returns a new instance of SeriesPartitionCompactor.
func NewSeriesPartitionCompactor() *SeriesPartitionCompactor {
	return &SeriesPartitionCompactor{}
}

// Compact rebuilds the series partition index.
func (c *SeriesPartitionCompactor) Compact(p *SeriesPartition) (time.Duration, error) {
	// Snapshot the partitions and index so we can check tombstones and replay at the end under lock.
	p.mu.RLock()
	segments := CloneSeriesSegments(p.segments)
	index := p.index.Clone()
	seriesN := p.index.Count()
	p.mu.RUnlock()

	now := time.Now()

	// Compact index to a temporary location.
	indexPath := index.path + ".compacting"
	if err := c.compactIndexTo(index, seriesN, segments, indexPath); err != nil {
		return 0, err
	}
	duration := time.Since(now)

	// Swap compacted index under lock & replay since compaction.
	if err := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		// Reopen index with new file.
		if err := p.index.Close(); err != nil {
			return err
		} else if err := fs.RenameFileWithReplacement(indexPath, index.path); err != nil {
			return err
		}

		p.index.SetPageFaultLimiter(p.pageFaultLimiter, p.tracker.AddPageFaults)
		if err := p.index.Open(); err != nil {
			return err
		}

		// Replay new entries.
		if err := p.index.Recover(p.segments); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return 0, err
	}

	return duration, nil
}

func (c *SeriesPartitionCompactor) compactIndexTo(index *SeriesIndex, seriesN uint64, segments []*SeriesSegment, path string) error {
	hdr := NewSeriesIndexHeader()
	hdr.Count = seriesN
	hdr.Capacity = pow2((int64(hdr.Count) * 100) / SeriesIndexLoadFactor)

	// Allocate space for maps.
	keyIDMap := make([]byte, (hdr.Capacity * SeriesIndexElemSize))
	idOffsetMap := make([]byte, (hdr.Capacity * SeriesIndexElemSize))

	// Reindex all partitions.
	var entryN int
	for _, segment := range segments {
		errDone := errors.New("done")

		if err := segment.ForEachEntry(func(flag uint8, id tsdb.SeriesIDTyped, offset int64, key []byte) error {
			// Make sure we don't go past the offset where the compaction began.
			if offset > index.maxOffset {
				return errDone
			}

			// Check for cancellation periodically.
			if entryN++; entryN%1000 == 0 {
				select {
				case <-c.cancel:
					return ErrSeriesPartitionCompactionCancelled
				default:
				}
			}

			// Only process insert entries.
			switch flag {
			case SeriesEntryInsertFlag: // fallthrough
			case SeriesEntryTombstoneFlag:
				return nil
			default:
				return fmt.Errorf("unexpected series partition log entry flag: %d", flag)
			}

			untypedID := id.SeriesID()

			// Save max series identifier processed.
			hdr.MaxSeriesID, hdr.MaxOffset = untypedID, offset

			// Ignore entry if tombstoned.
			if index.IsDeleted(untypedID) {
				return nil
			}

			// Insert into maps.
			c.insertIDOffsetMap(idOffsetMap, hdr.Capacity, untypedID, offset)
			return c.insertKeyIDMap(keyIDMap, hdr.Capacity, segments, key, offset, id)
		}); err == errDone {
			break
		} else if err != nil {
			return err
		}
	}

	// Open file handler.
	f, err := fs.CreateFile(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Calculate map positions.
	hdr.KeyIDMap.Offset, hdr.KeyIDMap.Size = SeriesIndexHeaderSize, int64(len(keyIDMap))
	hdr.IDOffsetMap.Offset, hdr.IDOffsetMap.Size = hdr.KeyIDMap.Offset+hdr.KeyIDMap.Size, int64(len(idOffsetMap))

	// Write header.
	if _, err := hdr.WriteTo(f); err != nil {
		return err
	}

	// Write maps.
	if _, err := f.Write(keyIDMap); err != nil {
		return err
	} else if _, err := f.Write(idOffsetMap); err != nil {
		return err
	}

	// Sync & close.
	if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	return nil
}

func (c *SeriesPartitionCompactor) insertKeyIDMap(dst []byte, capacity int64, segments []*SeriesSegment, key []byte, offset int64, id tsdb.SeriesIDTyped) error {
	mask := capacity - 1
	hash := rhh.HashKey(key)

	// Continue searching until we find an empty slot or lower probe distance.
	for i, dist, pos := int64(0), int64(0), hash&mask; ; i, dist, pos = i+1, dist+1, (pos+1)&mask {
		assert(i <= capacity, "key/id map full")
		elem := dst[(pos * SeriesIndexElemSize):]

		// If empty slot found or matching offset, insert and exit.
		elemOffset := int64(binary.BigEndian.Uint64(elem[:SeriesOffsetSize]))
		elemID := tsdb.NewSeriesIDTyped(binary.BigEndian.Uint64(elem[SeriesOffsetSize:]))
		if elemOffset == 0 || elemOffset == offset {
			binary.BigEndian.PutUint64(elem[:SeriesOffsetSize], uint64(offset))
			binary.BigEndian.PutUint64(elem[SeriesOffsetSize:], id.RawID())
			return nil
		}

		// Read key at position & hash.
		elemKey := ReadSeriesKeyFromSegments(segments, elemOffset+SeriesEntryHeaderSize)
		elemHash := rhh.HashKey(elemKey)

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		if d := rhh.Dist(elemHash, pos, capacity); d < dist {
			// Insert current values.
			binary.BigEndian.PutUint64(elem[:SeriesOffsetSize], uint64(offset))
			binary.BigEndian.PutUint64(elem[SeriesOffsetSize:], id.RawID())

			// Swap with values in that position.
			offset, id = elemOffset, elemID

			// Update current distance.
			dist = d
		}
	}
}

func (c *SeriesPartitionCompactor) insertIDOffsetMap(dst []byte, capacity int64, id tsdb.SeriesID, offset int64) {
	mask := capacity - 1
	hash := rhh.HashUint64(id.RawID())

	// Continue searching until we find an empty slot or lower probe distance.
	for i, dist, pos := int64(0), int64(0), hash&mask; ; i, dist, pos = i+1, dist+1, (pos+1)&mask {
		assert(i <= capacity, "id/offset map full")
		elem := dst[(pos * SeriesIndexElemSize):]

		// If empty slot found or matching id, insert and exit.
		elemID := tsdb.NewSeriesID(binary.BigEndian.Uint64(elem[:SeriesIDSize]))
		elemOffset := int64(binary.BigEndian.Uint64(elem[SeriesIDSize:]))
		if elemOffset == 0 || elemOffset == offset {
			binary.BigEndian.PutUint64(elem[:SeriesIDSize], id.RawID())
			binary.BigEndian.PutUint64(elem[SeriesIDSize:], uint64(offset))
			return
		}

		// Hash key.
		elemHash := rhh.HashUint64(elemID.RawID())

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		if d := rhh.Dist(elemHash, pos, capacity); d < dist {
			// Insert current values.
			binary.BigEndian.PutUint64(elem[:SeriesIDSize], id.RawID())
			binary.BigEndian.PutUint64(elem[SeriesIDSize:], uint64(offset))

			// Swap with values in that position.
			id, offset = elemID, elemOffset

			// Update current distance.
			dist = d
		}
	}
}

// pow2 returns the number that is the next highest power of 2.
// Returns v if it is a power of 2.
func pow2(v int64) int64 {
	for i := int64(2); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}
