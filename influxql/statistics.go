package influxql

import (
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

// Statistics is a collection of statistics about the processing of a query.
type Statistics struct {
	PlanDuration    time.Duration `json:"plan_duration"`    // PlanDuration is the duration spent planning the query.
	ExecuteDuration time.Duration `json:"execute_duration"` // ExecuteDuration is the duration spent executing the query.
	StatementCount  int           `json:"statement_count"`  // StatementCount is the number of InfluxQL statements executed
	ScannedValues   int           `json:"scanned_values"`   // ScannedValues is the number of values scanned from storage
	ScannedBytes    int           `json:"scanned_bytes"`    // ScannedBytes is the number of bytes scanned from storage
}

// Adding returns the sum of s and other.
func (s Statistics) Adding(other Statistics) Statistics {
	return Statistics{
		PlanDuration:    s.PlanDuration + other.PlanDuration,
		ExecuteDuration: s.ExecuteDuration + other.ExecuteDuration,
		StatementCount:  s.StatementCount + other.StatementCount,
		ScannedValues:   s.ScannedValues + other.ScannedValues,
		ScannedBytes:    s.ScannedBytes + other.ScannedBytes,
	}
}

// Add adds other to s.
func (s *Statistics) Add(other Statistics) {
	s.PlanDuration += other.PlanDuration
	s.ExecuteDuration += other.ExecuteDuration
	s.StatementCount += other.StatementCount
	s.ScannedValues += other.ScannedValues
	s.ScannedBytes += other.ScannedBytes
}

func (s *Statistics) LogToSpan(span opentracing.Span) {
	if span == nil {
		return
	}
	span.LogFields(
		log.Float64("stats_plan_duration_seconds", s.PlanDuration.Seconds()),
		log.Float64("stats_execute_duration_seconds", s.ExecuteDuration.Seconds()),
		log.Int("stats_statement_count", s.StatementCount),
		log.Int("stats_scanned_values", s.ScannedValues),
		log.Int("stats_scanned_bytes", s.ScannedBytes),
	)
}

// TotalDuration returns the sum of all durations for s.
func (s *Statistics) TotalDuration() time.Duration {
	return s.PlanDuration + s.ExecuteDuration
}

type CollectorFn func() Statistics

func (fn CollectorFn) Statistics() Statistics {
	return fn()
}

type MutableCollector struct {
	s *Statistics
}

func NewMutableCollector(s *Statistics) *MutableCollector {
	return &MutableCollector{s: s}
}

func (c *MutableCollector) Statistics() Statistics {
	return *c.s
}

type ImmutableCollector struct {
	s Statistics
}

func NewImmutableCollector(s Statistics) *ImmutableCollector {
	return &ImmutableCollector{s: s}
}

func (c *ImmutableCollector) Statistics() Statistics {
	return c.s
}

type StatisticsCollector interface {
	Statistics() Statistics
}

type StatisticsGatherer struct {
	mu         sync.Mutex
	collectors []StatisticsCollector
}

func (sg *StatisticsGatherer) Append(sc StatisticsCollector) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.collectors = append(sg.collectors, sc)
}

func (sg *StatisticsGatherer) Statistics() Statistics {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	res := Statistics{}
	for i := range sg.collectors {
		res = res.Adding(sg.collectors[i].Statistics())
	}
	return res
}

func (sg *StatisticsGatherer) Reset() {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	coll := sg.collectors
	sg.collectors = sg.collectors[:0]
	for i := range coll {
		coll[i] = nil
	}
}
