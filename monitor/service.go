package monitor

import (
	"expvar"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// StatsClient is the interface modules must implement if they wish to register with monitor.
type StatsClient interface {
	// Statistics returns a map of keys to values. Each Value must be either int64 or float64.
	// Statistical information is written to an InfluxDB system if enabled.
	Statistics() (map[string]interface{}, error)
}

type DiagnosticsClient interface {
	// Diagnostics returns a table of diagnostic information. The first returned value
	// is the name of the columns, the second is a slice of interface slices containing
	// the values for each column, by row. This information is never written to an InfluxDB
	// system and display-only.
	Diagnostics() ([]string, [][]interface{}, error)
}

// Service represents an instance of the monitor service.
type Service struct {
	wg            sync.WaitGroup
	done          chan struct{}
	mu            sync.Mutex
	registrations []*clientWithMeta

	hostname  string
	clusterID uint64
	nodeID    uint64

	storeEnabled  bool
	storeDatabase string
	storeAddress  string
	storeInterval time.Duration

	Logger *log.Logger
}

// NewService returns a new instance of the monitor service.
func NewService(c Config) *Service {
	return &Service{
		registrations: make([]*clientWithMeta, 0),
		storeEnabled:  c.StoreEnabled,
		storeDatabase: c.StoreDatabase,
		storeAddress:  c.StoreAddress,
		storeInterval: time.Duration(c.StoreInterval),
		Logger:        log.New(os.Stderr, "[monitor] ", log.LstdFlags),
	}
}

// Open opens the monitoring service, using the given clusterID, node ID, and hostname
// for identification purposes.
func (s *Service) Open(clusterID, nodeID uint64, hostname string) error {
	s.Logger.Printf("starting monitor service for cluster %d, host %s", clusterID, hostname)
	s.clusterID = clusterID
	s.nodeID = nodeID
	s.hostname = hostname

	// Self-register Go runtime stats and diags.
	gr := &goRuntime{}
	s.RegisterStatsClient("runtime", nil, gr)
	s.RegisterDiagnosticsClient("runtime", gr)

	// If enabled, record stats in a InfluxDB system.
	if s.storeEnabled {
		s.Logger.Printf("storing in %s, database '%s', interval %s",
			s.storeAddress, s.storeDatabase, s.storeInterval)

		s.Logger.Printf("ensuring database %s exists on %s", s.storeDatabase, s.storeAddress)
		if err := ensureDatabaseExists(s.storeAddress, s.storeDatabase); err != nil {
			return err
		}

		// Start periodic writes to system.
		s.wg.Add(1)
		go s.storeStatistics()
	}

	return nil
}

// Close closes the monitor service.
func (s *Service) Close() {
	s.Logger.Println("shutting down monitor service")
	close(s.done)
	s.wg.Wait()
	s.done = nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// RegisterStatsClient registers a stats client with the given name and tags.
func (s *Service) RegisterStatsClient(name string, tags map[string]string, client StatsClient) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := &clientWithMeta{
		StatsClient: client,
		name:        name,
		tags:        tags,
	}
	s.registrations = append(s.registrations, c)
	s.Logger.Printf(`'%s:%v' registered for monitoring`, name, tags)
	return nil
}

// RegisterDiagsClient registers a diagnostics client with the given name and tags.
func (s *Service) RegisterDiagnosticsClient(name string, client DiagnosticsClient) error {
	return nil
}

// ExecuteStatement executes monitor-related query statements.
func (s *Service) ExecuteStatement(stmt influxql.Statement) *influxql.Result {
	switch stmt := stmt.(type) {
	case *influxql.ShowStatsStatement:
		return s.executeShowStatistics(stmt)
	default:
		panic(fmt.Sprintf("unsupported statement type: %T", stmt))
	}
}

// executeShowStatistics returns the statistics of the registered monitor client in
// the standard form expected by users of the InfluxDB system.
func (s *Service) executeShowStatistics(q *influxql.ShowStatsStatement) *influxql.Result {
	stats, _ := s.statistics()
	rows := make([]*influxql.Row, len(stats))

	for n, stat := range stats {
		row := &influxql.Row{}
		values := make([]interface{}, 0, len(stat.Tags)+len(stat.Values))

		row.Columns = append(row.Columns, "name")
		values = append(values, stat.Name)

		for _, k := range stat.tagNames() {
			row.Columns = append(row.Columns, k)
			values = append(values, stat.Tags[k])
		}
		for _, k := range stat.valueNames() {
			row.Columns = append(row.Columns, k)
			values = append(values, stat.Values[k])
		}
		row.Values = [][]interface{}{values}
		rows[n] = row
	}
	return &influxql.Result{Series: rows}
}

// statistics returns the combined statistics for all registered clients.
func (s *Service) statistics() ([]*statistic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	statistics := make([]*statistic, 0, len(s.registrations))
	for _, r := range s.registrations {
		stats, err := r.StatsClient.Statistics()
		if err != nil {
			continue
		}

		// If a registered client has no field data, don't include it in the results
		if len(stats) == 0 {
			continue
		}

		statistics = append(statistics, newStatistic(r.name, r.tags, stats))
	}
	return statistics, nil
}

// storeStatistics writes the statistics to an InfluxDB system.
func (s *Service) storeStatistics() {
	// XXX add tags such as local hostname and cluster ID
	//a.Tags["clusterID"] = strconv.FormatUint(s.clusterID, 10)
	//a.Tags["nodeID"] = strconv.FormatUint(s.nodeID, 10)
	//a.Tags["hostname"] = s.hostname
	defer s.wg.Done()

	tick := time.NewTicker(s.storeInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			// Write stats here.
		case <-s.done:
			s.Logger.Printf("terminating storage of statistics to %s", s.storeAddress)
			return
		}

	}
}

// statistic represents the information returned by a single monitor client.
type statistic struct {
	Name   string
	Tags   map[string]string
	Values map[string]interface{}
}

// newStatistic returns a new statistic object. It ensures that tags are always non-nil.
func newStatistic(name string, tags map[string]string, values map[string]interface{}) *statistic {
	a := tags
	if a == nil {
		a = make(map[string]string)
	}

	return &statistic{
		Name:   name,
		Tags:   a,
		Values: values,
	}
}

// tagNames returns a sorted list of the tag names, if any.
func (s *statistic) tagNames() []string {
	a := make([]string, 0, len(s.Tags))
	for k, _ := range s.Tags {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// valueNames returns a sorted list of the value names, if any.
func (s *statistic) valueNames() []string {
	a := make([]string, 0, len(s.Values))
	for k, _ := range s.Values {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// clientWithMeta wraps a registered client with its associated name and tags.
type clientWithMeta struct {
	StatsClient
	name string
	tags map[string]string
}

// StatsMonitorClient wraps a *expvar.Map so that it implements the Client interface. It is for
// use by external packages that just record stats in an expvar.Map type.
type StatsMonitorClient struct {
	ep *expvar.Map
}

// NewStatsMonitorClient returns a new StatsMonitorClient using the given expvar.Map.
func NewStatsMonitorClient(ep *expvar.Map) *StatsMonitorClient {
	return &StatsMonitorClient{ep: ep}
}

// Statistics implements the StatsClient interface for a StatsMonitorClient.
func (m StatsMonitorClient) Statistics() (map[string]interface{}, error) {
	values := make(map[string]interface{})
	m.ep.Do(func(kv expvar.KeyValue) {
		var f interface{}
		var err error
		switch v := kv.Value.(type) {
		case *expvar.Float:
			f, err = strconv.ParseFloat(v.String(), 64)
			if err != nil {
				return
			}
		case *expvar.Int:
			f, err = strconv.ParseUint(v.String(), 10, 64)
			if err != nil {
				return
			}
		default:
			return
		}
		values[kv.Key] = f
	})

	return values, nil
}

func ensureDatabaseExists(host, database string) error {
	values := url.Values{}
	values.Set("q", fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
	resp, err := http.Get(host + "/query?" + values.Encode())
	if err != nil {
		return fmt.Errorf("failed to create monitoring database on %s: %s", host, err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to create monitoring database on %s, received code: %d",
			host, resp.StatusCode)
	}
	return nil
}
