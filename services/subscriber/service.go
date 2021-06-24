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

// PointsWriter is an interface for writing points to a subscription destination.
// Only WritePoints() needs to be satisfied.  PointsWriter implementations
// must be goroutine safe.
type PointsWriter interface {
	WritePointsContext(ctx context.Context, p *coordinator.WritePointsRequest) error
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
	points          chan *coordinator.WritePointsRequest
	wg              sync.WaitGroup
	closing         chan struct{}
	mu              sync.RWMutex
	conf            Config

	subs  map[subEntry]*chanWriter
}

// NewService returns a subscriber service with given settings
func NewService(c Config) *Service {
	s := &Service{
		Logger: zap.NewNop(),
		stats:  &Statistics{},
		conf:   c,
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
		s.points = make(chan *coordinator.WritePointsRequest, 100)

		s.wg.Add(2)
		go func() {
			defer s.wg.Done()
			s.run()
		}()
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

		close(s.points)
		close(s.closing)
		return nil
	}()
	if err != nil {
		return err
	}

	// Note this section is safe for concurrent calls to Close - both calls will wait for the exits, one caller
	// will win the right to close the channel writers, and the other will have to wait at the lock for that to finish.
	// When the second caller gets the lock subs is nil which is safe.

	// wait, not under the lock, for run and waitForMetaUpdates to finish gracefully
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

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, sub := range s.subs {
		statistics = append(statistics, sub.Statistics(tags)...)
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

// Points returns a channel into which write point requests can be sent.
func (s *Service) Points() chan<- *coordinator.WritePointsRequest {
	return s.points
}

// run read points from the points channel and writes them to the subscriptions.
func (s *Service) run() {
	for {
		select {
		case p, ok := <-s.points:
			if !ok {
				return
			}
			func() {
				s.mu.RLock()
				s.mu.RUnlock()
				p = s.removeBadPoints(p)
				for se, cw := range s.subs {
					if p.Database == se.db && p.RetentionPolicy == se.rp {
						select {
						case cw.writeRequests <- p:
						default:
							atomic.AddInt64(&s.stats.WriteFailures, 1)
						}
					}
				}
			}()
		}
	}
}

// removeBadPoints - if any non-UTF8 strings are found in the points in the WritePointRequest,
// make a copy without those points
func (s *Service) removeBadPoints(p *coordinator.WritePointsRequest) *coordinator.WritePointsRequest {
	log := s.Logger.With(zap.String("database", p.Database), zap.String("retention_policy", p.RetentionPolicy))

	firstBad, err := func() (int, error) {
		for i, point := range p.Points {
			if err := models.ValidPointStrings(point); err != nil {
				atomic.AddInt64(&s.stats.WriteFailures, 1)
				log.Debug("discarding point", zap.Error(err))
				return i, err
			}
		}
		return -1, nil
	}()
	if err != nil {
		wrq := &coordinator.WritePointsRequest{
			Database:        p.Database,
			RetentionPolicy: p.RetentionPolicy,
			Points:          make([]models.Point, 0, len(p.Points)-1),
		}

		// Copy all the points up to the first bad one.
		wrq.Points = append(wrq.Points, p.Points[:firstBad]...)
		for _, point := range p.Points[firstBad+1:] {
			if err := models.ValidPointStrings(point); err != nil {
				// Log and omit this point from subscription writes
				atomic.AddInt64(&s.stats.WriteFailures, 1)
				log.Debug("discarding point", zap.Error(err))
			} else {
				wrq.Points = append(wrq.Points, point)
			}
		}
		p = wrq
	}
	return p
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

	// Remove deleted subs
	for se := range s.subs {
		if !allEntries[se] {
			s.Logger.Info("Deleting old subscription",
				logger.Database(se.db),
				logger.RetentionPolicy(se.rp))

			// Close the chanWriter and cancel all in-flight writes
			s.subs[se].CancelAndClose()

			// Remove it from the set
			delete(s.subs, se)
			s.Logger.Info("Deleted old subscription",
				logger.Database(se.db),
				logger.RetentionPolicy(se.rp))
		}
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
	writeRequests chan *coordinator.WritePointsRequest
	ctx context.Context
	cancel context.CancelFunc
	pw            PointsWriter
	pointsWritten *int64
	failures      *int64
	logger        *zap.Logger
	wg sync.WaitGroup
}

func newChanWriter(s *Service, sub PointsWriter) *chanWriter {
	ctx, cancel := context.WithCancel(context.Background())
	cw := &chanWriter{
		writeRequests: make(chan *coordinator.WritePointsRequest, s.conf.WriteBufferSize),
		ctx: ctx,
		cancel: cancel,
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
			atomic.AddInt64(c.pointsWritten, int64(len(wr.Points)))
		}
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

func (b *balancewriter) WritePointsContext(ctx context.Context, p *coordinator.WritePointsRequest) error {
	var lastErr error
	for range b.writers {
		// round robin through destinations.
		i := b.i
		w := b.writers[i]
		b.i = (b.i + 1) % len(b.writers)

		// write points to destination.
		err := w.WritePointsContext(ctx, p)
		if err != nil {
			lastErr = err
			atomic.AddInt64(&b.stats[i].failures, 1)
		} else {
			atomic.AddInt64(&b.stats[i].pointsWritten, int64(len(p.Points)))
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
