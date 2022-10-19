package gather

import (
	"context"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/storage"
	"go.uber.org/zap"
)

// Scheduler is struct to run scrape jobs.
type Scheduler struct {
	Targets influxdb.ScraperTargetStoreService
	// Interval is between each metrics gathering event.
	Interval time.Duration

	log *zap.Logger

	scrapeRequest chan *influxdb.ScraperTarget
	done          chan struct{}
	wg            sync.WaitGroup
	writer        storage.PointsWriter
}

// NewScheduler creates a new Scheduler and subscriptions for scraper jobs.
func NewScheduler(
	log *zap.Logger,
	scrapeQueueLength int,
	scrapesInProgress int,
	targets influxdb.ScraperTargetStoreService,
	writer storage.PointsWriter,
	interval time.Duration,
) (*Scheduler, error) {
	if interval == 0 {
		interval = 60 * time.Second
	}
	scheduler := &Scheduler{
		Targets:       targets,
		Interval:      interval,
		log:           log,
		scrapeRequest: make(chan *influxdb.ScraperTarget, scrapeQueueLength),
		done:          make(chan struct{}),

		writer: writer,
	}

	scheduler.wg.Add(1)
	scraperPool := make(chan *prometheusScraper, scrapesInProgress)
	for i := 0; i < scrapesInProgress; i++ {
		scraperPool <- newPrometheusScraper()
	}
	go func() {
		defer scheduler.wg.Done()
		for {
			select {
			case req := <-scheduler.scrapeRequest:
				select {
				// Each request much acquire a scraper from the (limited) pool to run the scrape,
				// then return it to the pool
				case scraper := <-scraperPool:
					scheduler.doScrape(scraper, req, func(s *prometheusScraper) {
						scraperPool <- s
					})
				case <-scheduler.done:
					return
				}
			case <-scheduler.done:
				return
			}
		}
	}()

	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		ticker := time.NewTicker(scheduler.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-scheduler.done:
				return
			case <-ticker.C:
				scheduler.doGather()
			}
		}
	}()

	return scheduler, nil
}

func (s *Scheduler) doScrape(scraper *prometheusScraper, req *influxdb.ScraperTarget, releaseScraper func(s *prometheusScraper)) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer releaseScraper(scraper)
		logger := s.log.With(zap.String("scraper-name", req.Name))
		if req == nil {
			return
		}
		ms, err := scraper.Gather(*req)
		if err != nil {
			logger.Error("Unable to gather", zap.Error(err))
			return
		}
		ps, err := ms.MetricsSlice.Points()
		if err != nil {
			logger.Error("Unable to gather list of points", zap.Error(err))
		}
		err = s.writer.WritePoints(context.Background(), ms.OrgID, ms.BucketID, ps)
		if err != nil {
			logger.Error("Unable to write gathered points", zap.Error(err))
		}
	}()
}

func (s *Scheduler) doGather() {
	targets, err := s.Targets.ListTargets(context.Background(), influxdb.ScraperTargetFilter{})
	if err != nil {
		s.log.Error("Cannot list targets", zap.Error(err))
		return
	}
	for i := range targets {
		select {
		case s.scrapeRequest <- &targets[i]:
		default:
			s.log.Warn("Skipping scrape due to scraper backlog", zap.String("target", targets[i].Name))
		}
	}
}

func (s *Scheduler) Close() {
	close(s.done)
	s.wg.Wait()
}
