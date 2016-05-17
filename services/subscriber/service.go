package subscriber // import "github.com/influxdata/influxdb/services/subscriber"

import (
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/services/meta"
)

// Statistics for the Subscriber service.
const (
	statPointsWritten = "pointsWritten"
	statWriteFailures = "writeFailures"
)

// PointsWriter is an interface for writing points to a subscription destination.
// Only WritePoints() needs to be satisfied.
type PointsWriter interface {
	WritePoints(p *coordinator.WritePointsRequest) error
}

// unique set that identifies a given subscription
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
	Logger          *log.Logger
	update          chan struct{}
	statMap         *expvar.Map
	points          chan *coordinator.WritePointsRequest
	wg              sync.WaitGroup
	closed          bool
	closing         chan struct{}
	mu              sync.Mutex
}

// NewService returns a subscriber service with given settings
func NewService(c Config) *Service {
	return &Service{
		NewPointsWriter: newPointsWriter,
		Logger:          log.New(os.Stderr, "[subscriber] ", log.LstdFlags),
		statMap:         influxdb.NewStatistics("subscriber", "subscriber", nil),
		points:          make(chan *coordinator.WritePointsRequest, 100),
		closed:          true,
	}
}

// Open starts the subscription service.
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MetaClient == nil {
		return errors.New("no meta store")
	}

	s.closed = false

	s.closing = make(chan struct{})
	s.update = make(chan struct{})

	s.wg.Add(2)
	go s.run()
	go s.waitForMetaUpdates()

	s.Logger.Println("opened service")
	return nil
}

// Close terminates the subscription service
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true

	close(s.points)
	close(s.closing)

	s.wg.Wait()
	s.Logger.Println("closed service")
	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (s *Service) SetLogOutput(w io.Writer) {
	s.Logger = log.New(w, "[subscriber] ", log.LstdFlags)
}

func (s *Service) waitForMetaUpdates() {
	defer s.wg.Done()
	for {
		ch := s.MetaClient.WaitForDataChanged()
		select {
		case <-ch:
			err := s.Update()
			if err != nil {
				s.Logger.Println("error updating subscriptions:", err)
			}
		case <-s.closing:
			return
		}
	}
}

// Update will start new and stop deleted subscriptions.
func (s *Service) Update() error {
	// signal update
	select {
	case s.update <- struct{}{}:
		return nil
	case <-s.closing:
		return errors.New("service closed cannot update")
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
	writers := make([]PointsWriter, len(destinations))
	statMaps := make([]*expvar.Map, len(writers))
	for i, dest := range destinations {
		u, err := url.Parse(dest)
		if err != nil {
			return nil, err
		}
		w, err := s.NewPointsWriter(*u)
		if err != nil {
			return nil, err
		}
		writers[i] = w
		tags := map[string]string{
			"database":         se.db,
			"retention_policy": se.rp,
			"name":             se.name,
			"mode":             mode,
			"destination":      dest,
		}
		key := strings.Join([]string{"subscriber", se.db, se.rp, se.name, dest}, ":")
		statMaps[i] = influxdb.NewStatistics(key, "subscriber", tags)
	}
	s.Logger.Println("created new subscription for", se.db, se.rp)
	return &balancewriter{
		bm:       bm,
		writers:  writers,
		statMaps: statMaps,
	}, nil
}

// Points returns a channel into which write point requests can be sent.
func (s *Service) Points() chan<- *coordinator.WritePointsRequest {
	return s.points
}

// read points off chan and write them
func (s *Service) run() {
	defer s.wg.Done()
	subs := make(map[subEntry]PointsWriter)
	// Perform initial update
	s.updateSubs(subs)
	for {
		select {
		case <-s.update:
			s.updateSubs(subs)
		case p, ok := <-s.points:
			if !ok {
				return
			}
			for se, sub := range subs {
				if p.Database == se.db && p.RetentionPolicy == se.rp {
					err := sub.WritePoints(p)
					if err != nil {
						s.Logger.Println(err)
						s.statMap.Add(statWriteFailures, 1)
					}
				}
			}
			s.statMap.Add(statPointsWritten, int64(len(p.Points)))
		}
	}
}

func (s *Service) updateSubs(subs map[subEntry]PointsWriter) error {
	dbis := s.MetaClient.Databases()
	allEntries := make(map[subEntry]bool, 0)
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
				if _, ok := subs[se]; ok {
					continue
				}
				sub, err := s.createSubscription(se, si.Mode, si.Destinations)
				if err != nil {
					return err
				}
				subs[se] = sub
				s.Logger.Println("added new subscription for", se.db, se.rp)
			}
		}
	}

	// Remove deleted subs
	for se := range subs {
		if !allEntries[se] {
			delete(subs, se)
			s.Logger.Println("deleted old subscription for", se.db, se.rp)
		}
	}

	return nil
}

// BalanceMode sets what balance mode to use on a subscription.
// valid options are currently ALL or ANY
type BalanceMode int

//ALL is a Balance mode option
const (
	ALL BalanceMode = iota
	ANY
)

// balances writes across PointsWriters according to BalanceMode
type balancewriter struct {
	bm       BalanceMode
	writers  []PointsWriter
	statMaps []*expvar.Map
	i        int
}

func (b *balancewriter) WritePoints(p *coordinator.WritePointsRequest) error {
	var lastErr error
	for range b.writers {
		// round robin through destinations.
		i := b.i
		w := b.writers[i]
		b.i = (b.i + 1) % len(b.writers)

		// write points to destination.
		err := w.WritePoints(p)
		if err != nil {
			lastErr = err
			b.statMaps[i].Add(statWriteFailures, 1)
		} else {
			b.statMaps[i].Add(statPointsWritten, int64(len(p.Points)))
			if b.bm == ANY {
				break
			}
		}
	}
	return lastErr
}

// Creates a PointsWriter from the given URL
func newPointsWriter(u url.URL) (PointsWriter, error) {
	switch u.Scheme {
	case "udp":
		return NewUDP(u.Host), nil
	case "http", "https":
		return NewHTTP(u.Host)
	default:
		return nil, fmt.Errorf("unknown destination scheme %s", u.Scheme)
	}
}
