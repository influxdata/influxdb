package hh

import (
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/models"
)

var ErrHintedHandoffDisabled = fmt.Errorf("hinted handoff disabled")

const (
	writeShardReq       = "write_shard_req"
	writeShardReqPoints = "write_shard_req_points"
	processReq          = "process_req"
	processReqFail      = "process_req_fail"
)

type Service struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	closing chan struct{}

	statMap *expvar.Map
	Logger  *log.Logger
	cfg     Config

	ShardWriter shardWriter

	HintedHandoff interface {
		WriteShard(shardID, ownerID uint64, points []models.Point) error
		Process() error
		PurgeOlderThan(when time.Duration) error
	}
}

type shardWriter interface {
	WriteShard(shardID, ownerID uint64, points []models.Point) error
}

// NewService returns a new instance of Service.
func NewService(c Config, w shardWriter) *Service {
	key := strings.Join([]string{"hh", c.Dir}, ":")
	tags := map[string]string{"path": c.Dir}

	s := &Service{
		cfg:     c,
		statMap: influxdb.NewStatistics(key, "hh", tags),
		Logger:  log.New(os.Stderr, "[handoff] ", log.LstdFlags),
	}
	processor, err := NewProcessor(c.Dir, w, ProcessorOptions{
		MaxSize:        c.MaxSize,
		RetryRateLimit: c.RetryRateLimit,
	})
	if err != nil {
		s.Logger.Fatalf("Failed to start hinted handoff processor: %v", err)
	}

	processor.Logger = s.Logger
	s.HintedHandoff = processor
	return s
}

func (s *Service) Open() error {
	if !s.cfg.Enabled {
		// Allow Open to proceed, but don't anything.
		return nil
	}

	s.Logger.Printf("Starting hinted handoff service")

	s.mu.Lock()
	defer s.mu.Unlock()

	s.closing = make(chan struct{})

	s.Logger.Printf("Using data dir: %v", s.cfg.Dir)

	s.wg.Add(2)
	go s.retryWrites()
	go s.expireWrites()

	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing != nil {
		close(s.closing)
	}
	s.wg.Wait()
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// WriteShard queues the points write for shardID to node ownerID to handoff queue
func (s *Service) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	s.statMap.Add(writeShardReq, 1)
	s.statMap.Add(writeShardReqPoints, int64(len(points)))
	if !s.cfg.Enabled {
		return ErrHintedHandoffDisabled
	}

	return s.HintedHandoff.WriteShard(shardID, ownerID, points)
}

func (s *Service) retryWrites() {
	defer s.wg.Done()
	currInterval := time.Duration(s.cfg.RetryInterval)
	if currInterval > time.Duration(s.cfg.RetryMaxInterval) {
		currInterval = time.Duration(s.cfg.RetryMaxInterval)
	}

	for {

		select {
		case <-s.closing:
			return
		case <-time.After(currInterval):
			s.statMap.Add(processReq, 1)
			if err := s.HintedHandoff.Process(); err != nil && err != io.EOF {
				s.statMap.Add(processReqFail, 1)
				s.Logger.Printf("retried write failed: %v", err)

				currInterval = currInterval * 2
				if currInterval > time.Duration(s.cfg.RetryMaxInterval) {
					currInterval = time.Duration(s.cfg.RetryMaxInterval)
				}
			} else {
				// Success! Return to configured interval.
				currInterval = time.Duration(s.cfg.RetryInterval)
			}
		}
	}
}

// expireWrites will cause the handoff queues to remove writes that are older
// than the configured threshold
func (s *Service) expireWrites() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			if err := s.HintedHandoff.PurgeOlderThan(time.Duration(s.cfg.MaxAge)); err != nil {
				s.Logger.Printf("purge write failed: %v", err)
			}
		}
	}
}

// purgeWrites will cause the handoff queues to remove writes that are no longer
// valid.  e.g. queued writes for a node that has been removed
func (s *Service) purgeWrites() {
	panic("not implemented")
}
