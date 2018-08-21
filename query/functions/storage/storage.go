// Package storage implements reading from a storage engine into a table as a data source.
package storage

import (
	"context"
	"log"
	"math"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
)

type HostLookup interface {
	Hosts() []string
	Watch() <-chan struct{}
}

type BucketLookup interface {
	Lookup(orgID platform.ID, name string) (platform.ID, bool)
}

type OrganizationLookup interface {
	Lookup(ctx context.Context, name string) (platform.ID, bool)
}

type Dependencies struct {
	Reader             Reader
	BucketLookup       BucketLookup
	OrganizationLookup OrganizationLookup
}

func (d Dependencies) Validate() error {
	if d.Reader == nil {
		return errors.New("missing reader dependency")
	}
	if d.BucketLookup == nil {
		return errors.New("missing bucket lookup dependency")
	}
	if d.OrganizationLookup == nil {
		return errors.New("missing organization lookup dependency")
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
	overflow    bool
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
	//TODO(nathanielc): Pass through context to actual network I/O.
	for tables, mark, ok := s.next(ctx); ok; tables, mark, ok = s.next(ctx) {
		err := tables.Do(func(tbl query.Table) error {
			for _, t := range s.ts {
				if err := t.Process(s.id, tbl); err != nil {
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

func (s *source) next(ctx context.Context) (query.TableIterator, execute.Time, bool) {
	if s.overflow {
		return nil, 0, false
	}

	start := s.currentTime - execute.Time(s.window.Period)
	stop := s.currentTime
	if stop > s.bounds.Stop {
		return nil, 0, false
	}

	// Check if we will overflow, if so we are done after this pass
	every := execute.Time(s.window.Every)
	if every > 0 {
		s.overflow = s.currentTime > math.MaxInt64-every
	} else {
		s.overflow = s.currentTime < math.MinInt64-every
	}
	s.currentTime = s.currentTime + every

	bi, err := s.reader.Read(
		ctx,
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

type GroupMode int

const (
	// GroupModeDefault specifies the default grouping mode, which is GroupModeAll.
	GroupModeDefault GroupMode = 0
	// GroupModeNone merges all series into a single group.
	GroupModeNone GroupMode = 1 << iota
	// GroupModeAll produces a separate table for each series.
	GroupModeAll
	// GroupModeBy produces a table for each unique value of the specified GroupKeys.
	GroupModeBy
	// GroupModeExcept produces a table for the unique values of all keys, except those specified by GroupKeys.
	GroupModeExcept
)

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
	// GroupMode instructs
	GroupMode GroupMode
	// GroupKeys is the list of dimensions along which to group.
	//
	// When GroupMode is GroupModeBy, the results will be grouped by the specified keys.
	// When GroupMode is GroupModeExcept, the results will be grouped by all keys, except those specified.
	GroupKeys []string
}

type Reader interface {
	Read(ctx context.Context, rs ReadSpec, start, stop execute.Time) (query.TableIterator, error)
	Close()
}
