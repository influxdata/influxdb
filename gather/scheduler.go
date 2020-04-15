package gather

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/nats"
	"go.uber.org/zap"
)

// nats subjects
const (
	MetricsSubject    = "metrics"
	promTargetSubject = "promTarget"
)

// Scheduler is struct to run scrape jobs.
type Scheduler struct {
	Targets influxdb.ScraperTargetStoreService
	// Interval is between each metrics gathering event.
	Interval time.Duration
	// Timeout is the maxisium time duration allowed by each TCP request
	Timeout time.Duration

	// Publisher will send the gather requests and gathered metrics to the queue.
	Publisher nats.Publisher

	log *zap.Logger

	gather chan struct{}
}

// NewScheduler creates a new Scheduler and subscriptions for scraper jobs.
func NewScheduler(
	log *zap.Logger,
	numScrapers int,
	targets influxdb.ScraperTargetStoreService,
	p nats.Publisher,
	s nats.Subscriber,
	interval time.Duration,
	timeout time.Duration,
) (*Scheduler, error) {
	if interval == 0 {
		interval = 60 * time.Second
	}
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	scheduler := &Scheduler{
		Targets:   targets,
		Interval:  interval,
		Timeout:   timeout,
		Publisher: p,
		log:       log,
		gather:    make(chan struct{}, 100),
	}

	for i := 0; i < numScrapers; i++ {
		err := s.Subscribe(promTargetSubject, "metrics", &handler{
			Scraper:   new(prometheusScraper),
			Publisher: p,
			log:       log,
		})
		if err != nil {
			return nil, err
		}
	}

	return scheduler, nil
}

// Run will retrieve scraper targets from the target storage,
// and publish them to nats job queue for gather.
func (s *Scheduler) Run(ctx context.Context) error {
	go func(s *Scheduler, ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.Interval): // TODO: change to ticker because of garbage collection
				s.gather <- struct{}{}
			}
		}
	}(s, ctx)
	return s.run(ctx)
}

func (s *Scheduler) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.gather:
			s.doGather(ctx)
		}
	}
}

func (s *Scheduler) doGather(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	targets, err := s.Targets.ListTargets(ctx, influxdb.ScraperTargetFilter{})
	if err != nil {
		s.log.Error("Cannot list targets", zap.Error(err))
		tracing.LogError(span, err)
		return
	}
	for _, target := range targets {
		if err := requestScrape(target, s.Publisher); err != nil {
			s.log.Error("JSON encoding error", zap.Error(err))
			tracing.LogError(span, err)
		}
	}
}

func requestScrape(t influxdb.ScraperTarget, publisher nats.Publisher) error {
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(t)
	if err != nil {
		return err
	}
	switch t.Type {
	case influxdb.PrometheusScraperType:
		return publisher.Publish(promTargetSubject, buf)
	}
	return fmt.Errorf("unsupported target scrape type: %s", t.Type)
}
