package monitor // import "github.com/influxdata/influxdb/monitor"

import (
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/services/meta"
)

// Policy constants.
const (
	MonitorRetentionPolicy         = "monitor"
	MonitorRetentionPolicyDuration = 7 * 24 * time.Hour
)

// Monitor represents an instance of the monitor system.
type Monitor struct {
	// Build information for diagnostics.
	Version   string
	Commit    string
	Branch    string
	BuildTime string

	wg sync.WaitGroup

	mu                sync.Mutex
	globalTags        map[string]string
	diagRegistrations map[string]diagnostics.Client
	done              chan struct{}
	storeCreated      bool
	storeEnabled      bool
	storeAddress      string

	storeDatabase          string
	storeRetentionPolicy   string
	storeRetentionDuration time.Duration
	storeReplicationFactor int
	storeInterval          time.Duration

	MetaClient interface {
		CreateDatabaseWithRetentionPolicy(name string, rpi *meta.RetentionPolicyInfo) (*meta.DatabaseInfo, error)
		Database(name string) *meta.DatabaseInfo
	}

	// Writer for pushing stats back into the database.
	PointsWriter PointsWriter

	Logger *log.Logger
}

// PointsWriter is a simplified interface for writing the points the monitor gathers
type PointsWriter interface {
	WritePoints(database, retentionPolicy string, points models.Points) error
}

// New returns a new instance of the monitor system.
func New(c Config) *Monitor {
	return &Monitor{
		globalTags:           make(map[string]string),
		diagRegistrations:    make(map[string]diagnostics.Client),
		storeEnabled:         c.StoreEnabled,
		storeDatabase:        c.StoreDatabase,
		storeInterval:        time.Duration(c.StoreInterval),
		storeRetentionPolicy: MonitorRetentionPolicy,
		Logger:               log.New(os.Stderr, "[monitor] ", log.LstdFlags),
	}
}

func (m *Monitor) open() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done != nil
}

// Open opens the monitoring system, using the given clusterID, node ID, and hostname
// for identification purpose.
func (m *Monitor) Open() error {
	if m.open() {
		m.Logger.Println("Monitor is already open")
		return nil
	}

	m.Logger.Printf("Starting monitor system")

	// Self-register various stats and diagnostics.
	m.RegisterDiagnosticsClient("build", &build{
		Version: m.Version,
		Commit:  m.Commit,
		Branch:  m.Branch,
		Time:    m.BuildTime,
	})
	m.RegisterDiagnosticsClient("runtime", &goRuntime{})
	m.RegisterDiagnosticsClient("network", &network{})
	m.RegisterDiagnosticsClient("system", &system{})

	m.mu.Lock()
	m.done = make(chan struct{})
	m.mu.Unlock()

	// If enabled, record stats in a InfluxDB system.
	if m.storeEnabled {
		// Start periodic writes to system.
		m.wg.Add(1)
		go m.storeStatistics()
	}

	return nil
}

// Close closes the monitor system.
func (m *Monitor) Close() error {
	if !m.open() {
		m.Logger.Println("Monitor is already closed.")
		return nil
	}

	m.Logger.Println("shutting down monitor system")
	m.mu.Lock()
	close(m.done)
	m.mu.Unlock()

	m.wg.Wait()

	m.mu.Lock()
	m.done = nil
	m.mu.Unlock()

	m.DeregisterDiagnosticsClient("build")
	m.DeregisterDiagnosticsClient("runtime")
	m.DeregisterDiagnosticsClient("network")
	m.DeregisterDiagnosticsClient("system")
	return nil
}

// SetGlobalTag can be used to set tags that will appear on all points
// written by the Monitor.
func (m *Monitor) SetGlobalTag(key string, value interface{}) {
	m.mu.Lock()
	m.globalTags[key] = fmt.Sprintf("%v", value)
	m.mu.Unlock()
}

// RemoteWriterConfig represents the configuration of a remote writer
type RemoteWriterConfig struct {
	RemoteAddr string
	NodeID     string
	Username   string
	Password   string
	ClusterID  uint64
}

// SetPointsWriter can be used to set a writer for the monitoring points.
func (m *Monitor) SetPointsWriter(pw PointsWriter) error {
	if !m.storeEnabled {
		// not enabled, nothing to do
		return nil
	}
	m.mu.Lock()
	m.PointsWriter = pw
	m.mu.Unlock()

	// Subsequent calls to an already open Monitor are just a no-op.
	return m.Open()
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (m *Monitor) SetLogOutput(w io.Writer) {
	m.Logger = log.New(w, "[monitor] ", log.LstdFlags)
}

// RegisterDiagnosticsClient registers a diagnostics client with the given name and tags.
func (m *Monitor) RegisterDiagnosticsClient(name string, client diagnostics.Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.diagRegistrations[name] = client
	m.Logger.Printf(`'%s' registered for diagnostics monitoring`, name)
}

// DeregisterDiagnosticsClient deregisters a diagnostics client by name.
func (m *Monitor) DeregisterDiagnosticsClient(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.diagRegistrations, name)
}

// Statistics returns the combined statistics for all expvar data. The given
// tags are added to each of the returned statistics.
func (m *Monitor) Statistics(tags map[string]string) ([]*Statistic, error) {
	var statistics []*Statistic

	expvar.Do(func(kv expvar.KeyValue) {
		// Skip built-in expvar stats.
		if kv.Key == "memstats" || kv.Key == "cmdline" {
			return
		}

		statistic := &Statistic{
			Tags:   make(map[string]string),
			Values: make(map[string]interface{}),
		}

		// Add any supplied tags.
		for k, v := range tags {
			statistic.Tags[k] = v
		}

		// Every other top-level expvar value is a map.
		m := kv.Value.(*expvar.Map)

		m.Do(func(subKV expvar.KeyValue) {
			switch subKV.Key {
			case "name":
				// straight to string name.
				u, err := strconv.Unquote(subKV.Value.String())
				if err != nil {
					return
				}
				statistic.Name = u
			case "tags":
				// string-string tags map.
				n := subKV.Value.(*expvar.Map)
				n.Do(func(t expvar.KeyValue) {
					u, err := strconv.Unquote(t.Value.String())
					if err != nil {
						return
					}
					statistic.Tags[t.Key] = u
				})
			case "values":
				// string-interface map.
				n := subKV.Value.(*expvar.Map)
				n.Do(func(kv expvar.KeyValue) {
					var f interface{}
					var err error
					switch v := kv.Value.(type) {
					case *expvar.Float:
						f, err = strconv.ParseFloat(v.String(), 64)
						if err != nil {
							return
						}
					case *expvar.Int:
						f, err = strconv.ParseInt(v.String(), 10, 64)
						if err != nil {
							return
						}
					default:
						return
					}
					statistic.Values[kv.Key] = f
				})
			}
		})

		// If a registered client has no field data, don't include it in the results
		if len(statistic.Values) == 0 {
			return
		}

		statistics = append(statistics, statistic)
	})

	// Add Go memstats.
	statistic := &Statistic{
		Name:   "runtime",
		Tags:   make(map[string]string),
		Values: make(map[string]interface{}),
	}

	// Add any supplied tags to Go memstats
	for k, v := range tags {
		statistic.Tags[k] = v
	}

	var rt runtime.MemStats
	runtime.ReadMemStats(&rt)
	statistic.Values = map[string]interface{}{
		"Alloc":        int64(rt.Alloc),
		"TotalAlloc":   int64(rt.TotalAlloc),
		"Sys":          int64(rt.Sys),
		"Lookups":      int64(rt.Lookups),
		"Mallocs":      int64(rt.Mallocs),
		"Frees":        int64(rt.Frees),
		"HeapAlloc":    int64(rt.HeapAlloc),
		"HeapSys":      int64(rt.HeapSys),
		"HeapIdle":     int64(rt.HeapIdle),
		"HeapInUse":    int64(rt.HeapInuse),
		"HeapReleased": int64(rt.HeapReleased),
		"HeapObjects":  int64(rt.HeapObjects),
		"PauseTotalNs": int64(rt.PauseTotalNs),
		"NumGC":        int64(rt.NumGC),
		"NumGoroutine": int64(runtime.NumGoroutine()),
	}
	statistics = append(statistics, statistic)

	return statistics, nil
}

// Diagnostics fetches diagnostic information for each registered
// diagnostic client. It skips any clients that return an error when
// retrieving their diagnostics.
func (m *Monitor) Diagnostics() (map[string]*diagnostics.Diagnostics, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	diags := make(map[string]*diagnostics.Diagnostics, len(m.diagRegistrations))
	for k, v := range m.diagRegistrations {
		d, err := v.Diagnostics()
		if err != nil {
			continue
		}
		diags[k] = d
	}
	return diags, nil
}

// createInternalStorage ensures the internal storage has been created.
func (m *Monitor) createInternalStorage() {
	if m.storeCreated {
		return
	}

	if di := m.MetaClient.Database(m.storeDatabase); di == nil {
		rpi := meta.NewRetentionPolicyInfo(MonitorRetentionPolicy)
		rpi.Duration = MonitorRetentionPolicyDuration
		rpi.ReplicaN = 1

		if _, err := m.MetaClient.CreateDatabaseWithRetentionPolicy(m.storeDatabase, rpi); err != nil {
			m.Logger.Printf("failed to create database '%s', failed to create storage: %s",
				m.storeDatabase, err.Error())
			return
		}
	}

	// Mark storage creation complete.
	m.storeCreated = true
}

// storeStatistics writes the statistics to an InfluxDB system.
func (m *Monitor) storeStatistics() {
	defer m.wg.Done()
	m.Logger.Printf("Storing statistics in database '%s' retention policy '%s', at interval %s",
		m.storeDatabase, m.storeRetentionPolicy, m.storeInterval)

	hostname, _ := os.Hostname()
	m.SetGlobalTag("hostname", hostname)

	m.mu.Lock()
	tick := time.NewTicker(m.storeInterval)
	m.mu.Unlock()

	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			func() {
				m.mu.Lock()
				defer m.mu.Unlock()

				m.createInternalStorage()

				stats, err := m.Statistics(m.globalTags)
				if err != nil {
					m.Logger.Printf("failed to retrieve registered statistics: %s", err)
					return
				}

				points := make(models.Points, 0, len(stats))
				for _, s := range stats {
					pt, err := models.NewPoint(s.Name, s.Tags, s.Values, time.Now().Truncate(time.Second))
					if err != nil {
						m.Logger.Printf("Dropping point %v: %v", s.Name, err)
						return
					}
					points = append(points, pt)
				}

				if err := m.PointsWriter.WritePoints(m.storeDatabase, m.storeRetentionPolicy, points); err != nil {
					m.Logger.Printf("failed to store statistics: %s", err)
				}
			}()
		case <-m.done:
			m.Logger.Printf("terminating storage of statistics")
			return
		}
	}
}

// Statistic represents the information returned by a single monitor client.
type Statistic struct {
	Name   string                 `json:"name"`
	Tags   map[string]string      `json:"tags"`
	Values map[string]interface{} `json:"values"`
}

// valueNames returns a sorted list of the value names, if any.
func (s *Statistic) ValueNames() []string {
	a := make([]string, 0, len(s.Values))
	for k := range s.Values {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// DiagnosticsFromMap returns a Diagnostics from a map.
func DiagnosticsFromMap(m map[string]interface{}) *diagnostics.Diagnostics {
	// Display columns in deterministic order.
	sortedKeys := make([]string, 0, len(m))
	for k := range m {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	d := diagnostics.NewDiagnostics(sortedKeys)
	row := make([]interface{}, len(sortedKeys))
	for i, k := range sortedKeys {
		row[i] = m[k]
	}
	d.AddRow(row)

	return d
}
