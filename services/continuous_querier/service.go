// Package continuous_querier provides the continuous query service.
package continuous_querier // import "github.com/influxdata/influxdb/services/continuous_querier"

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

const (
	// NoChunkingSize specifies when not to chunk results. When planning
	// a select statement, passing zero tells it not to chunk results.
	// Only applies to raw queries.
	NoChunkingSize = 0

	// idDelimiter is used as a delimiter when creating a unique name for a
	// Continuous Query.
	idDelimiter = string(rune(31)) // unit separator
)

// Statistics for the CQ service.
const (
	statQueryOK   = "queryOk"
	statQueryFail = "queryFail"
)

// ContinuousQuerier represents a service that executes continuous queries.
type ContinuousQuerier interface {
	// Run executes the named query in the named database.  Blank database or name matches all.
	Run(database, name string, t time.Time) error
}

// metaClient is an internal interface to make testing easier.
type metaClient interface {
	AcquireLease(name string) (l *meta.Lease, err error)
	Databases() []meta.DatabaseInfo
	Database(name string) *meta.DatabaseInfo
}

// RunRequest is a request to run one or more CQs.
type RunRequest struct {
	// Now tells the CQ serivce what the current time is.
	Now time.Time
	// CQs tells the CQ service which queries to run.
	// If nil, all queries will be run.
	CQs []string
}

// matches returns true if the CQ matches one of the requested CQs.
func (rr *RunRequest) matches(cq *meta.ContinuousQueryInfo) bool {
	if rr.CQs == nil {
		return true
	}
	for _, q := range rr.CQs {
		if q == cq.Name {
			return true
		}
	}
	return false
}

type Monitor interface {
	Enabled() bool
	WritePoints(models.Points) error
}

type nullMonitor int

func (nullMonitor) Enabled() bool                   { return false }
func (nullMonitor) WritePoints(models.Points) error { return nil }

// Service manages continuous query execution.
type Service struct {
	MetaClient    metaClient
	QueryExecutor *query.Executor
	Monitor       Monitor
	Config        *Config
	RunInterval   time.Duration
	// RunCh can be used by clients to signal service to run CQs.
	RunCh             chan *RunRequest
	Logger            *zap.Logger
	loggingEnabled    bool
	queryStatsEnabled bool
	stats             *Statistics
	// lastRuns maps CQ name to last time it was run.
	mu       sync.RWMutex
	lastRuns map[string]time.Time
	stop     chan struct{}
	wg       *sync.WaitGroup
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		Config:            &c,
		Monitor:           nullMonitor(0),
		RunInterval:       time.Duration(c.RunInterval),
		RunCh:             make(chan *RunRequest),
		loggingEnabled:    c.LogEnabled,
		queryStatsEnabled: c.QueryStatsEnabled,
		Logger:            zap.NewNop(),
		stats:             &Statistics{},
		lastRuns:          map[string]time.Time{},
	}

	return s
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting continuous query service")

	if s.stop != nil {
		return nil
	}

	assert(s.MetaClient != nil, "MetaClient is nil")
	assert(s.QueryExecutor != nil, "QueryExecutor is nil")

	s.stop = make(chan struct{})
	s.wg = &sync.WaitGroup{}
	s.wg.Add(1)
	go s.backgroundLoop()
	return nil
}

// Close stops the service.
func (s *Service) Close() error {
	if s.stop == nil {
		return nil
	}
	close(s.stop)
	s.wg.Wait()
	s.wg = nil
	s.stop = nil
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "continuous_querier"))
}

// Statistics maintains the statistics for the continuous query service.
type Statistics struct {
	QueryOK   int64
	QueryFail int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "cq",
		Tags: tags,
		Values: map[string]interface{}{
			statQueryOK:   atomic.LoadInt64(&s.stats.QueryOK),
			statQueryFail: atomic.LoadInt64(&s.stats.QueryFail),
		},
	}}
}

// Run runs the specified continuous query, or all CQs if none is specified.
func (s *Service) Run(database, name string, t time.Time) error {
	var dbs []meta.DatabaseInfo

	if database != "" {
		// Find the requested database.
		db := s.MetaClient.Database(database)
		if db == nil {
			return query.ErrDatabaseNotFound(database)
		}
		dbs = append(dbs, *db)
	} else {
		// Get all databases.
		dbs = s.MetaClient.Databases()
	}

	// Loop through databases.
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, db := range dbs {
		// Loop through CQs in each DB executing the ones that match name.
		for _, cq := range db.ContinuousQueries {
			if name == "" || cq.Name == name {
				// Remove the last run time for the CQ
				id := fmt.Sprintf("%s%s%s", db.Name, idDelimiter, cq.Name)
				delete(s.lastRuns, id)
			}
		}
	}

	// Signal the background routine to run CQs.
	s.RunCh <- &RunRequest{Now: t}

	return nil
}

// backgroundLoop runs on a go routine and periodically executes CQs.
func (s *Service) backgroundLoop() {
	leaseName := "continuous_querier"
	t := time.NewTimer(s.RunInterval)
	defer t.Stop()
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			s.Logger.Info("Terminating continuous query service")
			return
		case req := <-s.RunCh:
			if !s.hasContinuousQueries() {
				continue
			}
			if _, err := s.MetaClient.AcquireLease(leaseName); err == nil {
				s.Logger.Info("Running continuous queries by request", zap.Time("at", req.Now))
				s.runContinuousQueries(req)
			}
		case <-t.C:
			if !s.hasContinuousQueries() {
				t.Reset(s.RunInterval)
				continue
			}
			if _, err := s.MetaClient.AcquireLease(leaseName); err == nil {
				s.runContinuousQueries(&RunRequest{Now: time.Now()})
			}
			t.Reset(s.RunInterval)
		}
	}
}

// hasContinuousQueries returns true if any CQs exist.
func (s *Service) hasContinuousQueries() bool {
	// Get list of all databases.
	dbs := s.MetaClient.Databases()
	// Loop through all databases executing CQs.
	for _, db := range dbs {
		if len(db.ContinuousQueries) > 0 {
			return true
		}
	}
	return false
}

// runContinuousQueries gets CQs from the meta store and runs them.
func (s *Service) runContinuousQueries(req *RunRequest) {
	// Get list of all databases.
	dbs := s.MetaClient.Databases()
	// Loop through all databases executing CQs.
	for _, db := range dbs {
		// TODO: distribute across nodes
		for _, cq := range db.ContinuousQueries {
			if !req.matches(&cq) {
				continue
			}
			if ok, err := s.ExecuteContinuousQuery(&db, &cq, req.Now); err != nil {
				s.Logger.Info("Error executing query", zap.String("query", cq.Query), zap.Error(err))
				atomic.AddInt64(&s.stats.QueryFail, 1)
			} else if ok {
				atomic.AddInt64(&s.stats.QueryOK, 1)
			}
		}
	}
}

// ExecuteContinuousQuery may execute a single CQ. This will return false if there were no errors and the CQ was not run.
func (s *Service) ExecuteContinuousQuery(dbi *meta.DatabaseInfo, cqi *meta.ContinuousQueryInfo, now time.Time) (bool, error) {
	// TODO: re-enable stats
	//s.stats.Inc("continuousQueryExecuted")

	// Local wrapper / helper.
	cq, err := NewContinuousQuery(dbi.Name, cqi)
	if err != nil {
		return false, err
	}

	// Set the time zone on the now time if the CQ has one. Otherwise, force UTC.
	now = now.UTC()
	if cq.q.Location != nil {
		now = now.In(cq.q.Location)
	}

	// Get the last time this CQ was run from the service's cache.
	s.mu.Lock()
	defer s.mu.Unlock()
	id := fmt.Sprintf("%s%s%s", dbi.Name, idDelimiter, cqi.Name)
	cq.LastRun, cq.HasRun = s.lastRuns[id]

	// Set the retention policy to default if it wasn't specified in the query.
	if cq.intoRP() == "" {
		cq.setIntoRP(dbi.DefaultRetentionPolicy)
	}

	// Get the group by interval.
	interval, err := cq.q.GroupByInterval()
	if err != nil {
		return false, err
	} else if interval == 0 {
		return false, nil
	}

	// Get the group by offset.
	offset, err := cq.q.GroupByOffset()
	if err != nil {
		return false, err
	}

	// See if this query needs to be run.
	run, nextRun, err := cq.shouldRunContinuousQuery(now, interval)
	if err != nil {
		return false, err
	} else if !run {
		return false, nil
	}

	resampleEvery := interval
	if cq.Resample.Every != 0 {
		resampleEvery = cq.Resample.Every
	}

	// We're about to run the query so store the current time closest to the nearest interval.
	// If all is going well, this time should be the same as nextRun.
	cq.LastRun = truncate(now.Add(-offset), resampleEvery).Add(offset)
	s.lastRuns[id] = cq.LastRun

	// Retrieve the oldest interval we should calculate based on the next time
	// interval. We do this instead of using the current time just in case any
	// time intervals were missed. The start time of the oldest interval is what
	// we use as the start time.
	resampleFor := interval
	if cq.Resample.For != 0 {
		resampleFor = cq.Resample.For
	} else if interval < resampleEvery {
		resampleFor = resampleEvery
	}

	// If the resample interval is greater than the interval of the query, use the
	// query interval instead.
	if interval < resampleEvery {
		resampleEvery = interval
	}

	// Calculate and set the time range for the query.
	startTime := truncate(nextRun.Add(interval-resampleFor-offset-1), interval).Add(offset)
	endTime := truncate(now.Add(interval-resampleEvery-offset), interval).Add(offset)
	if !endTime.After(startTime) {
		// Exit early since there is no time interval.
		return false, nil
	}

	if err := cq.q.SetTimeRange(startTime, endTime); err != nil {
		return false, fmt.Errorf("unable to set time range: %s", err)
	}

	var (
		start time.Time
		log   = s.Logger
	)
	if s.loggingEnabled || s.queryStatsEnabled {
		start = time.Now()
	}

	if s.loggingEnabled {
		var logEnd func()
		log, logEnd = logger.NewOperation(s.Logger, "Continuous query execution", "continuous_querier_execute")
		defer logEnd()

		log.Info("Executing continuous query",
			zap.String("name", cq.Info.Name),
			logger.Database(cq.Database),
			zap.Time("start", startTime),
			zap.Time("end", endTime))
	}

	// Do the actual processing of the query & writing of results.
	res := s.runContinuousQueryAndWriteResult(cq)
	if res.Err != nil {
		return false, res.Err
	}

	var execDuration time.Duration
	if s.loggingEnabled || s.queryStatsEnabled {
		execDuration = time.Since(start)
	}

	// extract number of points written from SELECT ... INTO result
	var written int64 = -1
	if len(res.Series) == 1 && len(res.Series[0].Values) == 1 {
		s := res.Series[0]
		written = s.Values[0][1].(int64)
	}

	if s.loggingEnabled {
		log.Info("Finished continuous query",
			zap.String("name", cq.Info.Name),
			logger.Database(cq.Database),
			zap.Int64("written", written),
			zap.Time("start", startTime),
			zap.Time("end", endTime),
			logger.DurationLiteral("duration", execDuration))
	}

	if s.queryStatsEnabled && s.Monitor.Enabled() {
		tags := map[string]string{"db": dbi.Name, "cq": cq.Info.Name}
		fields := map[string]interface{}{"durationNs": int64(execDuration), "pointsWrittenOK": written, "startTime": startTime.UnixNano(), "endTime": endTime.UnixNano()}
		p, _ := models.NewPoint("cq_query", models.NewTags(tags), fields, time.Now())
		s.Monitor.WritePoints(models.Points{p})
	}

	return true, nil
}

// runContinuousQueryAndWriteResult will run the query against the cluster and write the results back in
func (s *Service) runContinuousQueryAndWriteResult(cq *ContinuousQuery) *query.Result {
	// Wrap the CQ's inner SELECT statement in a Query for the Executor.
	q := &influxql.Query{
		Statements: influxql.Statements([]influxql.Statement{cq.q}),
	}

	closing := make(chan struct{})
	defer close(closing)

	// Execute the SELECT.
	ch := s.QueryExecutor.ExecuteQuery(q, query.ExecutionOptions{
		Database: cq.Database,
	}, closing)

	// There is only one statement, so we will only ever receive one result
	res, ok := <-ch
	if !ok {
		panic("result channel was closed")
	}
	return res
}

// ContinuousQuery is a local wrapper / helper around continuous queries.
type ContinuousQuery struct {
	Database string
	Info     *meta.ContinuousQueryInfo
	HasRun   bool
	LastRun  time.Time
	Resample ResampleOptions
	q        *influxql.SelectStatement
}

func (cq *ContinuousQuery) intoRP() string      { return cq.q.Target.Measurement.RetentionPolicy }
func (cq *ContinuousQuery) setIntoRP(rp string) { cq.q.Target.Measurement.RetentionPolicy = rp }

// ResampleOptions controls the resampling intervals and duration of this continuous query.
type ResampleOptions struct {
	// The query will be resampled at this time interval. The first query will be
	// performed at this time interval. If this option is not given, the resample
	// interval is set to the group by interval.
	Every time.Duration

	// The query will continue being resampled for this time duration. If this
	// option is not given, the resample duration is the same as the group by
	// interval. A bucket's time is calculated based on the bucket's start time,
	// so a 40m resample duration with a group by interval of 10m will resample
	// the bucket 4 times (using the default time interval).
	For time.Duration
}

// NewContinuousQuery returns a ContinuousQuery object with a parsed influxql.CreateContinuousQueryStatement.
func NewContinuousQuery(database string, cqi *meta.ContinuousQueryInfo) (*ContinuousQuery, error) {
	stmt, err := influxql.NewParser(strings.NewReader(cqi.Query)).ParseStatement()
	if err != nil {
		return nil, err
	}

	q, ok := stmt.(*influxql.CreateContinuousQueryStatement)
	if !ok || q.Source.Target == nil || q.Source.Target.Measurement == nil {
		return nil, errors.New("query isn't a valid continuous query")
	}

	cquery := &ContinuousQuery{
		Database: database,
		Info:     cqi,
		Resample: ResampleOptions{
			Every: q.ResampleEvery,
			For:   q.ResampleFor,
		},
		q: q.Source,
	}

	return cquery, nil
}

// shouldRunContinuousQuery returns true if the CQ should be schedule to run. It will use the
// lastRunTime of the CQ and the rules for when to run set through the query to determine
// if this CQ should be run.
func (cq *ContinuousQuery) shouldRunContinuousQuery(now time.Time, interval time.Duration) (bool, time.Time, error) {
	// If it's not aggregated, do not run the query.
	if cq.q.IsRawQuery {
		return false, cq.LastRun, errors.New("continuous queries must be aggregate queries")
	}

	// Override the query's default run interval with the resample options.
	resampleEvery := interval
	if cq.Resample.Every != 0 {
		resampleEvery = cq.Resample.Every
	}

	// Determine if we should run the continuous query based on the last time it ran.
	// If the query never ran, execute it using the current time.
	if cq.HasRun {
		// Retrieve the zone offset for the previous window.
		_, startOffset := cq.LastRun.Add(-1).Zone()
		nextRun := cq.LastRun.Add(resampleEvery)
		// Retrieve the end zone offset for the end of the current interval.
		if _, endOffset := nextRun.Add(-1).Zone(); startOffset != endOffset {
			diff := int64(startOffset-endOffset) * int64(time.Second)
			if abs(diff) < int64(resampleEvery) {
				nextRun = nextRun.Add(time.Duration(diff))
			}
		}
		if nextRun.UnixNano() <= now.UnixNano() {
			return true, nextRun, nil
		}
	} else {
		// Retrieve the location from the CQ.
		loc := cq.q.Location
		if loc == nil {
			loc = time.UTC
		}
		return true, now.In(loc), nil
	}

	return false, cq.LastRun, nil
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

// truncate truncates the time based on the unix timestamp instead of the
// Go time library. The Go time library has the start of the week on Monday
// while the start of the week for the unix timestamp is a Thursday.
func truncate(ts time.Time, d time.Duration) time.Time {
	t := ts.UnixNano()
	offset := zone(ts)
	dt := (t + offset) % int64(d)
	if dt < 0 {
		// Negative modulo rounds up instead of down, so offset
		// with the duration.
		dt += int64(d)
	}
	ts = time.Unix(0, t-dt).In(ts.Location())
	if adjustedOffset := zone(ts); adjustedOffset != offset {
		diff := offset - adjustedOffset
		if abs(diff) < int64(d) {
			ts = ts.Add(time.Duration(diff))
		}
	}
	return ts
}

func zone(ts time.Time) int64 {
	_, offset := ts.Zone()
	return int64(offset) * int64(time.Second)
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
