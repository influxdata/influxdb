// Package subscriber implements the subscriber service
// to forward incoming data to remote services.
package subscriber // import "github.com/influxdata/influxdb/services/subscriber"

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/subscriber/durablequeue"
	"go.uber.org/zap"
	"io"
	"math"
	"net/url"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// WriteRequest is a parsed write request.
type WriteRequest struct {
	// lineProtocol must be valid newline-separated line protocol.
	lineProtocol []byte
	numPoints int

	errchan chan error
}

// Finished signals that a write request is done for a single subscription.
func (w *WriteRequest) Finished(err error) {
	select {
		case w.errchan <- err:
	default:
		panic("Assertion failure - more subscriptions finished than started")
	}
}

func NewWriteRequest(points []models.Point, numSubs int)  WriteRequest {
	// Pre-allocate at least smallPointSize bytes per point.
	const smallPointSize = 10
	writeReq := WriteRequest{
		lineProtocol:    make([]byte, 0, len(points)*smallPointSize),
		errchan: make(chan error, numSubs),
	}
	for _, p := range points {
		// TODO: validate points (at least 1.x does that here)
		// Append the new point and a newline
		writeReq.lineProtocol = p.AppendString(writeReq.lineProtocol)
		writeReq.lineProtocol = append(writeReq.lineProtocol, byte('\n'))
		writeReq.numPoints++
	}
	return writeReq
}

func (w *WriteRequest) Length() int {
	return w.numPoints
}

func (w *WriteRequest) SizeOf() int {
	return len(w.lineProtocol)
}

// PointsWriter is an interface for writing points to a subscription destination.
// Only WritePoints() needs to be satisfied.  PointsWriter implementations
// must be goroutine safe.
type PointsWriter interface {
	WritePointsContext(ctx context.Context, lineProtocol []byte) error
}

// Service manages forking the incoming data from InfluxDB
// to defined third party destinations.
// Subscriptions are defined per database and retention policy.
type Service struct {
	NewPointsWriter func(dest Destination) (PointsWriter, error)
	Logger          *zap.Logger
	stats           *Statistics
	wg              sync.WaitGroup
	closing         chan struct{}
	mu              sync.Mutex
	conf            Config
	subs            map[SubEntry]*chanWriter

	// subscriptionRouter is not locked by mu
	router *subscriptionRouter
}

// NewService returns a subscriber service with given settings
func NewService(c Config, logger *zap.Logger) *Service {
	stats := &Statistics{}
	s := &Service{
		Logger: logger,
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
		// TODO:
		/*
		if s.MetaClient == nil {
			return errors.New("no meta store")
		}

		 */

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
	if s == nil {
		return nil
	}
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

// TODO: hook to prometheus metrics?
// Statistics maintains the statistics for the subscriber service.
type Statistics struct {
	CreateFailures int64
	PointsWritten  int64
	WriteFailures  int64
}

func (s *Service) waitForMetaUpdates() {
	//TODO:
	//ch := s.MetaClient.WaitForDataChanged()
	for {
		select {
		/*case <-ch:
			// ch is closed on changes, so fetch the new channel to wait on to ensure we don't miss a new
			// change while updating
			ch = s.MetaClient.WaitForDataChanged()
			s.updateSubs()
			*/
		case <-s.closing:
			return
		}
	}
}

func (s *Service) createSubscription(dest Destination) (PointsWriter, error) {
	w, err := s.NewPointsWriter(dest)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer for destination: %s, err: %w", dest.String(), err)
	}
	return w, nil
}

func (s *Service) WritePoints(bucketID platform.ID, points []models.Point) error {
	return s.router.Send(bucketID, points)
}

type Destination struct {
	Org platform.ID
	Bucket platform.ID
	Token string
	Url string
}

func (d Destination) String() string {
	// TODO: is this ok?
	return fmt.Sprintf("%#v", d)
}

type SubEntry struct {
	Name string
	ID platform.ID
	SourceBucket platform.ID
	Destination Destination
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
		s.subs = make(map[SubEntry]*chanWriter)
	}

	// TODO: replace with metastore query - note subscription ID's must be unique
	// like dbis := s.MetaClient.Databases()
	desiredEntries := []SubEntry{
		{
			Name:         "sub1",
			ID:           1, // TODO: figure out how updating a destination works with the disk queue (which is keyed on ID)
			SourceBucket: 0x8d181582a47c6144,
			Destination: Destination{
				Org:    5,
				Bucket: 8,
				Token:  "foo",
				Url:    "http://localhost:2000",
			},
		},
	}
	allEntries := make(map[SubEntry]bool)
	createdNew := false
	// Add in new subscriptions
	for _, se := range desiredEntries {

		allEntries[se] = true
		if _, ok := s.subs[se]; ok {
			continue
		}
		createdNew = true
		// TODO: re-examine data to put in logs
		s.Logger.Info("Adding new subscription", zap.String("bucket", se.SourceBucket.String()))
		sub, err := s.createSubscription(se.Destination)
		if err != nil {
			atomic.AddInt64(&s.stats.CreateFailures, 1)
			s.Logger.Info("Subscription creation failed", zap.String("name", se.Name), zap.Error(err))
			continue
		}
		// TODO: extract queue creation to function
		queueDir := path.Join(s.conf.Dir, se.ID.String())
		if err := os.MkdirAll(queueDir, 0777); err != nil {
			atomic.AddInt64(&s.stats.CreateFailures, 1)
			s.Logger.Error("Subscription queue dir open failed", zap.String("dir", queueDir), zap.String("name", se.Name),zap.String("id", se.ID.String()), zap.Error(err))
			continue
		}
		queue, err := durablequeue.NewQueue(queueDir, int64(s.conf.MaxQueueSize), &durablequeue.SharedCount{}, math.MaxInt64, func(b []byte) error { _, err := unmarshalWrite(b); return err })
		if err != nil {
			atomic.AddInt64(&s.stats.CreateFailures, 1)
			s.Logger.Error("Subscription queue creation failed", zap.String("name", se.Name), zap.String("id", se.ID.String()), zap.Error(err))
			continue
		}
		err = queue.Open()
		if err != nil {
			atomic.AddInt64(&s.stats.CreateFailures, 1)
			s.Logger.Error("Subscription queue creation failed", zap.String("name", se.Name), zap.String("id", se.ID.String()), zap.Error(err))
			continue
		}
		s.subs[se] = newChanWriter(s, sub, queue)
		s.Logger.Info("Added new subscription",zap.String("bucket", se.SourceBucket.String()))

	}

	toClose := make(map[SubEntry]*chanWriter)
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
		s.Logger.Info("Deleting old subscription", zap.String("bucket", se.SourceBucket.String()))

		cw.CancelAndClose()

		s.Logger.Info("Deleted old subscription", zap.String("bucket", se.SourceBucket.String()))
	}
}

// newPointsWriter returns a new PointsWriter from the given URL.
func (s *Service) newPointsWriter(dest Destination) (PointsWriter, error) {
	// add only valid destinations
	u, err := url.Parse(dest.Url)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination: %s", dest)
	}
	// TODO: support HTTPS & timeouts properly
	switch u.Scheme {
	case "http":
		return NewHTTP(u.String(), dest.Token, dest.Org.String(), dest.Bucket.String())
	case "https":
		panic("Does not support https yet")
	default:
		return nil, fmt.Errorf("unknown destination scheme %s", u.Scheme)
	}
}

type RetryConfig struct {
	RetryInterval time.Duration
	PurgeInterval time.Duration
	MaxAge time.Duration
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
	queue *durablequeue.Queue
	durableQueueDone chan struct{}
	retry RetryConfig
}

func newChanWriter(s *Service, sub PointsWriter, queue *durablequeue.Queue) *chanWriter {
	ctx, cancel := context.WithCancel(context.Background())
	cw := &chanWriter{
		writeRequests: make(chan WriteRequest, s.conf.WriteBufferSize),
		ctx:           ctx,
		cancel:        cancel,
		pw:            sub,
		pointsWritten: &s.stats.PointsWritten,
		failures:      &s.stats.WriteFailures,
		logger:        s.Logger,
		// TODO: config?
		retry : RetryConfig{
			RetryInterval: 1*time.Minute,
			PurgeInterval: 30*time.Minute,
			MaxAge:        math.MaxInt64,
		},
		durableQueueDone: make(chan struct{}),
		queue: queue,
	}
	for i := 0; i < s.conf.WriteConcurrency; i++ {
		cw.wg.Add(1)
		go func() {
			defer cw.wg.Done()
			cw.Run()
		}()
	}
	cw.wg.Add(1)
	go cw.ProcessQueue()
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
	close(c.durableQueueDone)
	c.wg.Wait()
	c.queue.Close()
}

func (c *chanWriter) Run() {
	for wr := range c.writeRequests {
		var err error
		func () {
			defer atomic.AddInt64(&c.queueSize, -int64(wr.SizeOf()))
			ctx, cancel := context.WithTimeout(c.ctx, 1*time.Second) // TODO: configuration parameter
			defer cancel()
			err = c.pw.WritePointsContext(ctx, wr.lineProtocol)
			// TODO: check for droppable error (e.g. field conflict)
			if err != nil {
				c.logger.Info(err.Error())
				atomic.AddInt64(c.failures, 1)
				err = c.queue.Append(wr.lineProtocol)
			} else {
				atomic.AddInt64(c.pointsWritten, int64(wr.Length()))
			}
			wr.Finished(err)
		}()
	}
}

func (c chanWriter) ProcessQueue() {
	defer c.wg.Done()
	purgeTimer := time.NewTicker(c.retry.PurgeInterval)
	defer purgeTimer.Stop()
	retryTimer := time.NewTicker(c.retry.RetryInterval)
	defer retryTimer.Stop()

	for {
		select {
		case <-c.durableQueueDone:
			return
		case <-purgeTimer.C:
			if err := c.queue.PurgeOlderThan(time.Now().Add(-c.retry.MaxAge)); err != nil {
				//TODO: add logging: c.Logger.Info("Failed to purge", zap.Uint64("node_id", n.nodeID), zap.Error(err))
			}
		case <-retryTimer.C:
			for {
				err := c.SendWrite()
				if err != nil {
					if err == io.EOF {
						// No more data.
					}
					break
				}
				//TODO: add rate limits
			}
		}
	}
}

func (c chanWriter) SendWrite() error {
	scan, err := c.queue.NewScanner()
	if err != nil {
		return err
	}
	for scan.Next() {
		nextWrite := scan.Bytes()
		if err != nil {
			// TODO: log skipped write
			continue
		}
		// TODO: timeouts
		err = c.pw.WritePointsContext(context.Background(), nextWrite)
		// TODO: skip droppable errors (e.g. field write conflicts)
		if err != nil {
			return fmt.Errorf("error writing from disk queue: %w", err)
		}
	}
	if scan.Err() != nil {
		// TODO: log segment read error
	}
	if _, err := scan.Advance(); err != nil {
		return err
	}
	return nil
}


// TODO: statistics reporting?
/*
// Statistics returns statistics for periodic monitoring.
func (c *chanWriter) Statistics(tags map[string]string) []models.Statistic {
	if m, ok := c.pw.(monitor.Reporter); ok {
		return m.Statistics(tags)
	}
	return []models.Statistic{}
}

 */

// subscriptionRouter has a mutex lock on the hot path for database writes - make sure that the lock is very tight.
type subscriptionRouter struct {
	mu            sync.RWMutex
	ready         bool
	// m is a map from bucket to list of subscriber chanWriters
	m             map[platform.ID][]*chanWriter
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

func (s *subscriptionRouter) Send(bucketID platform.ID, points []models.Point) error {
	// serialize points and put on writer
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.ready {
		return fmt.Errorf("subscriptions not opened")
	}
	writers := s.m[bucketID]
	if len(writers) == 0 {
		return nil
	}
	writeReq := NewWriteRequest(points, len(writers))
	for _, w := range writers {
		w.Write(writeReq)
	}
	// TODO: should we block here or elsewhere to inform the write request of failure?
	for range writers {
		// TODO: timeouts
		singleErr := <-writeReq.errchan
		if singleErr != nil {
			return singleErr
		}
	}
	return nil
}

func (s *subscriptionRouter) Update(cws map[SubEntry]*chanWriter, memoryLimit int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		panic("must be created with NewServer before calling update, must not call update after close")
	}
	s.m = make(map[platform.ID][]*chanWriter)
	for se, cw := range cws {
		cw.limitTo(memoryLimit)
		s.m[se.SourceBucket] = append(s.m[se.SourceBucket], cw)
	}
}

func unmarshalWrite(b []byte) ([]models.Point, error) {
	return models.ParsePointsWithPrecision(b, time.Time{}, "n")
}