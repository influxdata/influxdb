// Package monitor provides a service and associated functionality
// for InfluxDB to self-monitor internal statistics and diagnostics.
package monitor // import "github.com/influxdata/influxdb/monitor"

import (
	"errors"
	"expvar"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Policy constants.
const (
	// Name of the retention policy used by the monitor service.
	MonitorRetentionPolicy = "monitor"

	// Duration of the monitor retention policy.
	MonitorRetentionPolicyDuration = 7 * 24 * time.Hour

	// Default replication factor to set on the monitor retention policy.
	MonitorRetentionPolicyReplicaN = 1
)

// Monitor represents an instance of the monitor system.
type Monitor struct {
	// Build information for diagnostics.
	Version   string
	Commit    string
	Branch    string
	BuildTime string

	wg sync.WaitGroup

	mu                sync.RWMutex
	globalTags        map[string]string
	diagRegistrations map[string]diagnostics.Client
	reporter          Reporter
	done              chan struct{}
	storeCreated      bool
	storeEnabled      bool

	storeDatabase        string
	storeRetentionPolicy string
	storeInterval        time.Duration

	MetaClient interface {
		CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
		Database(name string) *meta.DatabaseInfo
	}

	// Writer for pushing stats back into the database.
	PointsWriter PointsWriter

	Logger *zap.Logger
}

// PointsWriter is a simplified interface for writing the points the monitor gathers.
type PointsWriter interface {
	WritePoints(database, retentionPolicy string, points models.Points) error
}

// New returns a new instance of the monitor system.
func New(r Reporter, c Config) *Monitor {
	return &Monitor{
		globalTags:           make(map[string]string),
		diagRegistrations:    make(map[string]diagnostics.Client),
		reporter:             r,
		storeEnabled:         c.StoreEnabled,
		storeDatabase:        c.StoreDatabase,
		storeInterval:        time.Duration(c.StoreInterval),
		storeRetentionPolicy: MonitorRetentionPolicy,
		Logger:               zap.NewNop(),
	}
}

// open returns whether the monitor service is open.
func (m *Monitor) open() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done != nil
}

// Open opens the monitoring system, using the given clusterID, node ID, and hostname
// for identification purpose.
func (m *Monitor) Open() error {
	if m.open() {
		m.Logger.Info("Monitor is already open")
		return nil
	}

	m.Logger.Info("Starting monitor service")

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
		hostname, _ := os.Hostname()
		m.SetGlobalTag("hostname", hostname)

		// Start periodic writes to system.
		m.wg.Add(1)
		go m.storeStatistics()
	}

	return nil
}

func (m *Monitor) Enabled() bool { return m.storeEnabled }

func (m *Monitor) WritePoints(p models.Points) error {
	if !m.storeEnabled {
		return nil
	}

	if len(m.globalTags) > 0 {
		for _, pp := range p {
			pp.SetTags(pp.Tags().Merge(m.globalTags))
		}
	}

	return m.writePoints(p)
}

func (m *Monitor) writePoints(p models.Points) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err := m.PointsWriter.WritePoints(m.storeDatabase, m.storeRetentionPolicy, p); err != nil {
		m.Logger.Info("failed to store statistics", zap.Error(err))
	}
	return nil
}

// Close closes the monitor system.
func (m *Monitor) Close() error {
	if !m.open() {
		m.Logger.Info("Monitor is already closed")
		return nil
	}

	m.Logger.Info("Shutting down monitor service")
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

// RemoteWriterConfig represents the configuration of a remote writer.
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

// WithLogger sets the logger for the Monitor.
func (m *Monitor) WithLogger(log *zap.Logger) {
	m.Logger = log.With(zap.String("service", "monitor"))
}

// RegisterDiagnosticsClient registers a diagnostics client with the given name and tags.
func (m *Monitor) RegisterDiagnosticsClient(name string, client diagnostics.Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.diagRegistrations[name] = client
	m.Logger.Info("Registered diagnostics client", zap.String("name", name))
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
			Statistic: models.NewStatistic(""),
		}

		// Add any supplied tags.
		for k, v := range tags {
			statistic.Tags[k] = v
		}

		// Every other top-level expvar value should be a map.
		m, ok := kv.Value.(*expvar.Map)
		if !ok {
			return
		}

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
		Statistic: models.NewStatistic("runtime"),
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

	statistics = m.gatherStatistics(statistics, tags)
	return statistics, nil
}

func (m *Monitor) gatherStatistics(statistics []*Statistic, tags map[string]string) []*Statistic {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.reporter != nil {
		for _, s := range m.reporter.Statistics(tags) {
			statistics = append(statistics, &Statistic{Statistic: s})
		}
	}
	return statistics
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
		duration := MonitorRetentionPolicyDuration
		replicaN := MonitorRetentionPolicyReplicaN
		spec := meta.RetentionPolicySpec{
			Name:     MonitorRetentionPolicy,
			Duration: &duration,
			ReplicaN: &replicaN,
		}

		if _, err := m.MetaClient.CreateDatabaseWithRetentionPolicy(m.storeDatabase, &spec); err != nil {
			m.Logger.Info("Failed to create storage", logger.Database(m.storeDatabase), zap.Error(err))
			return
		}
	}

	// Mark storage creation complete.
	m.storeCreated = true
}

// waitUntilInterval waits until we are on an even interval for the duration.
func (m *Monitor) waitUntilInterval(d time.Duration) error {
	now := time.Now()
	until := now.Truncate(d).Add(d)
	timer := time.NewTimer(until.Sub(now))
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-m.done:
		return errors.New("interrupted")
	}
}

// storeStatistics writes the statistics to an InfluxDB system.
func (m *Monitor) storeStatistics() {
	defer m.wg.Done()
	m.Logger.Info("Storing statistics", logger.Database(m.storeDatabase), logger.RetentionPolicy(m.storeRetentionPolicy), logger.DurationLiteral("interval", m.storeInterval))

	// Wait until an even interval to start recording monitor statistics.
	// If we are interrupted before the interval for some reason, exit early.
	if err := m.waitUntilInterval(m.storeInterval); err != nil {
		return
	}

	tick := time.NewTicker(m.storeInterval)
	defer tick.Stop()

	for {
		select {
		case now := <-tick.C:
			now = now.Truncate(m.storeInterval)
			func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				m.createInternalStorage()
			}()

			stats, err := m.Statistics(m.globalTags)
			if err != nil {
				m.Logger.Info("Failed to retrieve registered statistics", zap.Error(err))
				return
			}

			// Write all stats in batches
			batch := make(models.Points, 0, 5000)
			for _, s := range stats {
				pt, err := models.NewPoint(s.Name, models.NewTags(s.Tags), s.Values, now)
				if err != nil {
					m.Logger.Info("Dropping point", zap.String("name", s.Name), zap.Error(err))
					return
				}
				batch = append(batch, pt)
				if len(batch) == cap(batch) {
					m.writePoints(batch)
					batch = batch[:0]

				}
			}

			// Write the last batch
			if len(batch) > 0 {
				m.writePoints(batch)
			}
		case <-m.done:
			m.Logger.Info("Terminating storage of statistics")
			return
		}
	}
}

// Statistic represents the information returned by a single monitor client.
type Statistic struct {
	models.Statistic
}

// ValueNames returns a sorted list of the value names, if any.
func (s *Statistic) ValueNames() []string {
	a := make([]string, 0, len(s.Values))
	for k := range s.Values {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// Statistics is a slice of sortable statistics.
type Statistics []*Statistic

// Len implements sort.Interface.
func (a Statistics) Len() int { return len(a) }

// Less implements sort.Interface.
func (a Statistics) Less(i, j int) bool {
	return a[i].Name < a[j].Name
}

// Swap implements sort.Interface.
func (a Statistics) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
