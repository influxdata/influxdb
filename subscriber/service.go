// Package subscriber implements the subscriber service
// to forward incoming data to remote services.
package subscriber // import "github.com/influxdata/influxdb/services/subscriber"

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Statistics for the Subscriber service.
const (
	statCreateFailures = "createFailures"
	statPointsWritten  = "pointsWritten"
	statWriteFailures  = "writeFailures"
)

// WriteRequest is a parsed write request.
type WriteRequest struct {
	Database        string
	RetentionPolicy string
	// lineProtocol must be valid newline-separated line protocol.
	lineProtocol []byte
	// pointOffsets gives the starting index within lineProtocol of each point,
	// for splitting batches if required.
	pointOffsets []int
}

func NewWriteRequest(r *coordinator.WritePointsRequest, log *zap.Logger) (wr WriteRequest, numInvalid int64) {
	log = log.With(zap.String("database", r.Database), zap.String("retention_policy", r.RetentionPolicy))
	// Pre-allocate at least smallPointSize bytes per point.
	const smallPointSize = 10
	writeReq := WriteRequest{
		Database:        r.Database,
		RetentionPolicy: r.RetentionPolicy,
		pointOffsets:    make([]int, 0, len(r.Points)),
		lineProtocol:    make([]byte, 0, len(r.Points)*smallPointSize),
	}
	numInvalid = 0
	for _, p := range r.Points {
		if err := models.ValidPointStrings(p); err != nil {
			log.Debug("discarding point", zap.Error(err))
			numInvalid++
			continue
		}
		// We are about to append a point of line protocol, so the new point's start index
		// is the current length.
		writeReq.pointOffsets = append(writeReq.pointOffsets, len(writeReq.lineProtocol))
		// Append the new point and a newline
		writeReq.lineProtocol = p.AppendString(writeReq.lineProtocol)
		writeReq.lineProtocol = append(writeReq.lineProtocol, byte('\n'))
	}
	return writeReq, numInvalid
}

// pointAt uses pointOffsets to slice the lineProtocol buffer and retrieve the i_th point in the request.
// It includes the trailing newline.
func (w *WriteRequest) PointAt(i int) []byte {
	start := w.pointOffsets[i]
	// The end of the last point is the length of the buffer
	end := len(w.lineProtocol)
	// For points that are not the last point, the end is the start of the next point
	if i+1 < len(w.pointOffsets) {
		end = w.pointOffsets[i+1]
	}
	return w.lineProtocol[start:end]
}

func (w *WriteRequest) Length() int {
	return len(w.pointOffsets)
}

func (w *WriteRequest) SizeOf() int {
	const intSize = unsafe.Sizeof(w.pointOffsets[0])
	return len(w.lineProtocol) + len(w.pointOffsets)*int(intSize) + len(w.Database) + len(w.RetentionPolicy)
}

// PointsWriter is an interface for writing points to a subscription destination.
// Only WritePoints() needs to be satisfied.  PointsWriter implementations
// must be goroutine safe.
type PointsWriter interface {
	WritePointsContext(ctx context.Context, request WriteRequest) error
}

// subEntry is a unique set that identifies a given subscription.
type subEntry struct {
	db   string
	rp   string
	name string
}

// Service manages forking the incoming data from InfluxDB
// to defined third party destinations.
// Subscriptions are defined per database and retention policy.
type Service struct {
	MetaClient interface {
		Databases() []meta.DatabaseInfo
		WaitForDataChanged() chan struct{}
	}
	NewPointsWriter func(u url.URL) (PointsWriter, error)
	Logger          *zap.Logger
	stats           *Statistics
	wg              sync.WaitGroup
	closing         chan struct{}
	mu              sync.Mutex
	conf            Config
	subs            map[subEntry]*chanWriter

	// subscriptionRouter is not locked by mu
	router *subscriptionRouter
}

// NewService returns a subscriber service with given settings
func NewService(c Config) *Service {
	stats := &Statistics{}
	s := &Service{
		Logger: zap.NewNop(),
		stats:  stats,
		conf:   c,
		router: newSubscriptionRouter(stats),
	}
	s.NewPointsWriter = s.newPointsWriter
	return s
}

// Open starts the subscription service.
func (s *Service) Open() error {
	if !s.conf.Enabled {
		return nil // Service disabled.
	}

	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.MetaClient == nil {
			return errors.New("no meta store")
		}

		s.closing = make(chan struct{})

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.waitForMetaUpdates()
		}()
		return nil
	}()
	if err != nil {
		return err
	}

	// Create all subs with initial metadata
	s.updateSubs()

	s.Logger.Info("Opened service")
	return nil
}

// Close terminates the subscription service.
// It will return an error if Open was not called first.
func (s *Service) Close() error {
	// stop receiving new input
	s.router.Close()

	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closing == nil {
			return fmt.Errorf("closing unopened subscription service")
		}

		select {
		case <-s.closing:
			// already closed
			return nil
		default:
		}

		close(s.closing)
		return nil
	}()
	if err != nil {
		return err
	}

	// Note this section is safe for concurrent calls to Close - both calls will wait for the exits, one caller
	// will win the right to close the channel writers, and the other will have to wait at the lock for that to finish.
	// When the second caller gets the lock subs is nil which is safe.

	// wait, not under the lock, for waitForMetaUpdates to finish gracefully
	s.wg.Wait()

	// close all the subscriptions
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cw := range s.subs {
		cw.Close()
	}
	s.subs = nil
	s.Logger.Info("Closed service")
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "subscriber"))
	s.router.Logger = s.Logger
}

// Statistics maintains the statistics for the subscriber service.
type Statistics struct {
	CreateFailures int64
	PointsWritten  int64
	WriteFailures  int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	statistics := []models.Statistic{{
		Name: "subscriber",
		Tags: tags,
		Values: map[string]interface{}{
			statCreateFailures: atomic.LoadInt64(&s.stats.CreateFailures),
			statPointsWritten:  atomic.LoadInt64(&s.stats.PointsWritten),
			statWriteFailures:  atomic.LoadInt64(&s.stats.WriteFailures),
		},
	}}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cw := range s.subs {
		statistics = append(statistics, cw.Statistics(tags)...)
	}

	return statistics
}

func (s *Service) waitForMetaUpdates() {
	ch := s.MetaClient.WaitForDataChanged()
	for {
		select {
		case <-ch:
			// ch is closed on changes, so fetch the new channel to wait on to ensure we don't miss a new
			// change while updating
			ch = s.MetaClient.WaitForDataChanged()
			s.updateSubs()
		case <-s.closing:
			return
		}
	}
}

func (s *Service) createSubscription(se subEntry, mode string, destinations []string) (PointsWriter, error) {
	var bm BalanceMode
	switch mode {
	case "ALL":
		bm = ALL
	case "ANY":
		bm = ANY
	default:
		return nil, fmt.Errorf("unknown balance mode %q", mode)
	}
	writers := make([]PointsWriter, 0, len(destinations))
	stats := make([]writerStats, 0, len(destinations))
	// add only valid destinations
	for _, dest := range destinations {
		u, err := url.Parse(dest)
		if err != nil {
			return nil, fmt.Errorf("failed to parse destination: %s", dest)
		}
		w, err := s.NewPointsWriter(*u)
		if err != nil {
			return nil, fmt.Errorf("failed to create writer for destination: %s", dest)
		}
		writers = append(writers, w)
		stats = append(stats, writerStats{dest: dest})
	}

	return &balancewriter{
		bm:      bm,
		writers: writers,
		stats:   stats,
		defaultTags: models.StatisticTags{
			"database":         se.db,
			"retention_policy": se.rp,
			"name":             se.name,
			"mode":             mode,
		},
	}, nil
}

func (s *Service) Send(request *coordinator.WritePointsRequest) {
	s.router.Send(request)
}

func (s *Service) updateSubs() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// check if we're closing while under the lock
	select {
	case <-s.closing:
		return
	default:
	}

	if s.subs == nil {
		s.subs = make(map[subEntry]*chanWriter)
	}

	dbis := s.MetaClient.Databases()
	allEntries := make(map[subEntry]bool)
	createdNew := false
	// Add in new subscriptions
	for _, dbi := range dbis {
		for _, rpi := range dbi.RetentionPolicies {
			for _, si := range rpi.Subscriptions {
				se := subEntry{
					db:   dbi.Name,
					rp:   rpi.Name,
					name: si.Name,
				}
				allEntries[se] = true
				if _, ok := s.subs[se]; ok {
					continue
				}
				createdNew = true
				s.Logger.Info("Adding new subscription",
					logger.Database(se.db),
					logger.RetentionPolicy(se.rp))
				sub, err := s.createSubscription(se, si.Mode, si.Destinations)
				if err != nil {
					atomic.AddInt64(&s.stats.CreateFailures, 1)
					s.Logger.Info("Subscription creation failed", zap.String("name", si.Name), zap.Error(err))
					continue
				}
				s.subs[se] = newChanWriter(s, sub)
				s.Logger.Info("Added new subscription",
					logger.Database(se.db),
					logger.RetentionPolicy(se.rp))
			}
		}
	}

	toClose := make(map[subEntry]*chanWriter)
	for se, cw := range s.subs {
		if !allEntries[se] {
			toClose[se] = cw
			delete(s.subs, se)
		}
	}

	if createdNew || len(toClose) > 0 {
		memoryLimit := int64(0)
		if s.conf.TotalBufferBytes != 0 {
			memoryLimit = int64(s.conf.TotalBufferBytes / len(s.subs))
			if memoryLimit == 0 {
				memoryLimit = 1
			}
		}
		// update the router before we close any subscriptions
		s.router.Update(s.subs, memoryLimit)
	}

	for se, cw := range toClose {
		s.Logger.Info("Deleting old subscription",
			logger.Database(se.db),
			logger.RetentionPolicy(se.rp))

		cw.CancelAndClose()

		s.Logger.Info("Deleted old subscription",
			logger.Database(se.db),
			logger.RetentionPolicy(se.rp))
	}
}

// newPointsWriter returns a new PointsWriter from the given URL.
func (s *Service) newPointsWriter(u url.URL) (PointsWriter, error) {
	switch u.Scheme {
	case "udp":
		return NewUDP(u.Host), nil
	case "http":
		return NewHTTP(u.String(), time.Duration(s.conf.HTTPTimeout))
	case "https":
		if s.conf.InsecureSkipVerify {
			s.Logger.Warn("'insecure-skip-verify' is true. This will skip all certificate verifications.")
		}
		return NewHTTPS(u.String(), time.Duration(s.conf.HTTPTimeout), s.conf.InsecureSkipVerify, s.conf.CaCerts, s.conf.TLS)
	default:
		return nil, fmt.Errorf("unknown destination scheme %s", u.Scheme)
	}
}

// chanWriter sends WritePointsRequest to a PointsWriter received over a channel.
type chanWriter struct {
	writeRequests chan WriteRequest
	ctx           context.Context
	cancel        context.CancelFunc
	pw            PointsWriter
	pointsWritten *int64
	failures      *int64
	logger        *zap.Logger
	queueSize     int64
	queueLimit    int64
	wg            sync.WaitGroup
}

func newChanWriter(s *Service, sub PointsWriter) *chanWriter {
	ctx, cancel := context.WithCancel(context.Background())
	cw := &chanWriter{
		writeRequests: make(chan WriteRequest, s.conf.WriteBufferSize),
		ctx:           ctx,
		cancel:        cancel,
		pw:            sub,
		pointsWritten: &s.stats.PointsWritten,
		failures:      &s.stats.WriteFailures,
		logger:        s.Logger,
	}
	for i := 0; i < s.conf.WriteConcurrency; i++ {
		cw.wg.Add(1)
		go func() {
			defer cw.wg.Done()
			cw.Run()
		}()
	}
	return cw
}

// Write is on the hot path for data ingest (to the whole database, not just subscriptions).
// Be extra careful about latency.
func (c *chanWriter) Write(wr WriteRequest) {
	sz := wr.SizeOf()
	newSize := atomic.AddInt64(&c.queueSize, int64(sz))
	limit := atomic.LoadInt64(&c.queueLimit)

	// If we would add more size than we should hold, reject the write
	if limit > 0 && newSize > limit {
		atomic.AddInt64(c.failures, 1)
		atomic.AddInt64(&c.queueSize, -int64(sz))
		return
	}

	// If the write queue is full, reject the write
	select {
	case c.writeRequests <- wr:
	default:
		atomic.AddInt64(c.failures, 1)
	}
}

// limitTo sets a new limit on the size of the queue.
func (c *chanWriter) limitTo(newLimit int64) {
	atomic.StoreInt64(&c.queueLimit, newLimit)
	// We don't immediately evict things if the queue is over the limit,
	// since they should be shortly evicted in normal operation.
}

func (c *chanWriter) CancelAndClose() {
	close(c.writeRequests)
	c.cancel()
	c.wg.Wait()
}

// Close closes the chanWriter. It blocks until all the in-flight write requests are finished.
func (c *chanWriter) Close() {
	close(c.writeRequests)
	c.wg.Wait()
}

func (c *chanWriter) Run() {
	for wr := range c.writeRequests {
		err := c.pw.WritePointsContext(c.ctx, wr)
		if err != nil {
			c.logger.Info(err.Error())
			atomic.AddInt64(c.failures, 1)
		} else {
			atomic.AddInt64(c.pointsWritten, int64(len(wr.pointOffsets)))
		}
		atomic.AddInt64(&c.queueSize, -int64(wr.SizeOf()))
	}
}

// Statistics returns statistics for periodic monitoring.
func (c *chanWriter) Statistics(tags map[string]string) []models.Statistic {
	if m, ok := c.pw.(monitor.Reporter); ok {
		return m.Statistics(tags)
	}
	return []models.Statistic{}
}

// BalanceMode specifies what balance mode to use on a subscription.
type BalanceMode int

const (
	// ALL indicates to send writes to all subscriber destinations.
	ALL BalanceMode = iota

	// ANY indicates to send writes to a single subscriber destination, round robin.
	ANY
)

type writerStats struct {
	dest          string
	failures      int64
	pointsWritten int64
}

// balances writes across PointsWriters according to BalanceMode
type balancewriter struct {
	bm          BalanceMode
	writers     []PointsWriter
	stats       []writerStats
	defaultTags models.StatisticTags
	i           int
}

func (b *balancewriter) WritePointsContext(ctx context.Context, request WriteRequest) error {
	var lastErr error
	for range b.writers {
		// round robin through destinations.
		i := b.i
		w := b.writers[i]
		b.i = (b.i + 1) % len(b.writers)

		// write points to destination.
		err := w.WritePointsContext(ctx, request)
		if err != nil {
			lastErr = err
			atomic.AddInt64(&b.stats[i].failures, 1)
		} else {
			atomic.AddInt64(&b.stats[i].pointsWritten, int64(len(request.pointOffsets)))
			if b.bm == ANY {
				break
			}
		}
	}
	return lastErr
}

// Statistics returns statistics for periodic monitoring.
func (b *balancewriter) Statistics(tags map[string]string) []models.Statistic {
	statistics := make([]models.Statistic, len(b.stats))
	for i := range b.stats {
		subTags := b.defaultTags.Merge(tags)
		subTags["destination"] = b.stats[i].dest
		statistics[i] = models.Statistic{
			Name: "subscriber",
			Tags: subTags,
			Values: map[string]interface{}{
				statPointsWritten: atomic.LoadInt64(&b.stats[i].pointsWritten),
				statWriteFailures: atomic.LoadInt64(&b.stats[i].failures),
			},
		}
	}
	return statistics
}

type dbrp struct {
	db string
	rp string
}

// subscriptionRouter has a mutex lock on the hot path for database writes - make sure that the lock is very tight.
type subscriptionRouter struct {
	mu            sync.RWMutex
	ready         bool
	m             map[dbrp][]*chanWriter
	writeFailures *int64
	Logger        *zap.Logger
}

func newSubscriptionRouter(statistics *Statistics) *subscriptionRouter {
	return &subscriptionRouter{
		ready:         true,
		writeFailures: &statistics.WriteFailures,
		Logger:        zap.NewNop(),
	}
}

func (s *subscriptionRouter) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = false
}

func (s *subscriptionRouter) Send(request *coordinator.WritePointsRequest) {
	// serialize points and put on writer
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.ready {
		return
	}
	writers := s.m[dbrp{
		db: request.Database,
		rp: request.RetentionPolicy,
	}]
	if len(writers) == 0 {
		return
	}
	writeReq, numInvalid := NewWriteRequest(request, s.Logger)
	atomic.AddInt64(s.writeFailures, numInvalid)
	for _, w := range writers {
		w.Write(writeReq)
	}
}

func (s *subscriptionRouter) Update(cws map[subEntry]*chanWriter, memoryLimit int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		panic("must be created with NewServer before calling update, must not call update after close")
	}
	s.m = make(map[dbrp][]*chanWriter)
	for se, cw := range cws {
		cw.limitTo(memoryLimit)
		key := dbrp{
			db: se.db,
			rp: se.rp,
		}
		s.m[key] = append(s.m[key], cw)
	}
}
