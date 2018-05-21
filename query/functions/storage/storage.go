package storage

import (
	"context"
	"log"

	"github.com/influxdata/platform/query/id"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/semantic"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

type HostLookup interface {
	Hosts() []string
	Watch() <-chan struct{}
}

type BucketLookup interface {
	Lookup(orgID id.ID, name string) (id.ID, bool)
}

type Dependencies struct {
	Reader       Reader
	BucketLookup BucketLookup
}

func (d Dependencies) Validate() error {
	if d.Reader == nil {
		return errors.New("missing reader dependency")
	}
	return nil
}

type StaticLookup struct {
	hosts []string
}

func NewStaticLookup(hosts []string) StaticLookup {
	return StaticLookup{
		hosts: hosts,
	}
}

func (l StaticLookup) Hosts() []string {
	return l.hosts
}
func (l StaticLookup) Watch() <-chan struct{} {
	// A nil channel always blocks, since hosts never change this is appropriate.
	return nil
}

// source performs storage reads
type source struct {
	id       execute.DatasetID
	reader   Reader
	readSpec ReadSpec
	window   execute.Window
	bounds   execute.Bounds

	ts []execute.Transformation

	currentTime execute.Time
}

func NewSource(id execute.DatasetID, r Reader, readSpec ReadSpec, bounds execute.Bounds, w execute.Window, currentTime execute.Time) execute.Source {
	return &source{
		id:          id,
		reader:      r,
		readSpec:    readSpec,
		bounds:      bounds,
		window:      w,
		currentTime: currentTime,
	}
}

func (s *source) AddTransformation(t execute.Transformation) {
	s.ts = append(s.ts, t)
}

func (s *source) Run(ctx context.Context) {
	err := s.run(ctx)
	for _, t := range s.ts {
		t.Finish(s.id, err)
	}
}
func (s *source) run(ctx context.Context) error {

	var trace map[string]string
	if span := opentracing.SpanFromContext(ctx); span != nil {
		trace = make(map[string]string)
		span = opentracing.StartSpan("storage_source.run", opentracing.ChildOf(span.Context()))
		_ = opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(trace))
	}

	//TODO(nathanielc): Pass through context to actual network I/O.
	for blocks, mark, ok := s.next(ctx, trace); ok; blocks, mark, ok = s.next(ctx, trace) {
		err := blocks.Do(func(b execute.Block) error {
			for _, t := range s.ts {
				if err := t.Process(s.id, b); err != nil {
					return err
				}
				//TODO(nathanielc): Also add mechanism to send UpdateProcessingTime calls, when no data is arriving.
				// This is probably not needed for this source, but other sources should do so.
				if err := t.UpdateProcessingTime(s.id, execute.Now()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, t := range s.ts {
			if err := t.UpdateWatermark(s.id, mark); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *source) next(ctx context.Context, trace map[string]string) (execute.BlockIterator, execute.Time, bool) {
	start := s.currentTime - execute.Time(s.window.Period)
	stop := s.currentTime

	s.currentTime = s.currentTime + execute.Time(s.window.Every)
	if stop > s.bounds.Stop {
		return nil, 0, false
	}
	bi, err := s.reader.Read(
		ctx,
		trace,
		s.readSpec,
		start,
		stop,
	)
	if err != nil {
		log.Println("E!", err)
		return nil, 0, false
	}
	return bi, stop, true
}

type ReadSpec struct {
	OrganizationID []byte
	BucketID       []byte

	RAMLimit     uint64
	Hosts        []string
	Predicate    *semantic.FunctionExpression
	PointsLimit  int64
	SeriesLimit  int64
	SeriesOffset int64
	Descending   bool

	AggregateMethod string

	// OrderByTime indicates that series reads should produce all
	// series for a time before producing any series for a larger time.
	// By default this is false meaning all values of time are produced for a given series,
	// before any values are produced from the next series.
	OrderByTime bool
	// MergeAll indicates that all series should be merged into a single group
	MergeAll bool
	// GroupKeys is the list of dimensions along which to group
	GroupKeys []string
	// GroupExcept is the list of dimensions along which to not group
	GroupExcept []string
}

type Reader interface {
	Read(ctx context.Context, trace map[string]string, rs ReadSpec, start, stop execute.Time) (execute.BlockIterator, error)
	Close()
}
