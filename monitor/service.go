package monitor

import (
	"expvar"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

const leaderWaitTimeout = 30 * time.Second

// DiagsClient is the interface modules implement if they register diags with monitor.
type DiagsClient interface {
	Diagnostics() (*Diagnostic, error)
}

// The DiagsClientFunc type is an adapter to allow the use of
// ordinary functions as Diagnostis clients.
type DiagsClientFunc func() (*Diagnostic, error)

// Diagnostics calls f().
func (f DiagsClientFunc) Diagnostics() (*Diagnostic, error) {
	return f()
}

// Diagnostic represents a table of diagnostic information. The first value
// is the name of the columns, the second is a slice of interface slices containing
// the values for each column, by row. This information is never written to an InfluxDB
// system and is display-only. An example showing, say, connections follows:
//
//     source_ip    source_port       dest_ip     dest_port
//     182.1.0.2    2890              127.0.0.1   38901
//     174.33.1.2   2924              127.0.0.1   38902
type Diagnostic struct {
	Columns []string
	Rows    [][]interface{}
}

func NewDiagnostic(columns []string) *Diagnostic {
	return &Diagnostic{
		Columns: columns,
		Rows:    make([][]interface{}, 0),
	}
}

func (d *Diagnostic) AddRow(r []interface{}) {
	d.Rows = append(d.Rows, r)
}

// Monitor represents an instance of the monitor system.
type Monitor struct {
	wg   sync.WaitGroup
	done chan struct{}
	mu   sync.Mutex

	diagRegistrations map[string]DiagsClient

	storeEnabled  bool
	storeDatabase string
	storeAddress  string
	storeInterval time.Duration

	MetaStore interface {
		ClusterID() (uint64, error)
		NodeID() uint64
		WaitForLeader(d time.Duration) error
		CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error)
	}

	PointsWriter interface {
		WritePoints(p *cluster.WritePointsRequest) error
	}

	Logger *log.Logger
}

// New returns a new instance of the monitor system.
func New(c Config) *Monitor {
	return &Monitor{
		done:              make(chan struct{}),
		diagRegistrations: make(map[string]DiagsClient),
		storeEnabled:      c.StoreEnabled,
		storeDatabase:     c.StoreDatabase,
		storeInterval:     time.Duration(c.StoreInterval),
		Logger:            log.New(os.Stderr, "[monitor] ", log.LstdFlags),
	}
}

// Open opens the monitoring system, using the given clusterID, node ID, and hostname
// for identification purposem.
func (m *Monitor) Open() error {
	m.Logger.Printf("Starting monitor system")

	// Self-register various stats and diagnostics.
	m.RegisterDiagnosticsClient("runtime", &goRuntime{})
	m.RegisterDiagnosticsClient("network", &network{})
	m.RegisterDiagnosticsClient("system", &system{})

	// If enabled, record stats in a InfluxDB system.
	if m.storeEnabled {

		// Start periodic writes to system.
		m.wg.Add(1)
		go m.storeStatistics()
	}

	return nil
}

// Close closes the monitor system.
func (m *Monitor) Close() {
	m.Logger.Println("shutting down monitor system")
	close(m.done)
	m.wg.Wait()
	m.done = nil
}

// SetLogger sets the internal logger to the logger passed in.
func (m *Monitor) SetLogger(l *log.Logger) {
	m.Logger = l
}

// RegisterDiagnosticsClient registers a diagnostics client with the given name and tags.
func (m *Monitor) RegisterDiagnosticsClient(name string, client DiagsClient) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.diagRegistrations[name] = client
	m.Logger.Printf(`'%s' registered for diagnostics monitoring`, name)
	return nil
}

// statistics returns the combined statistics for all expvar data.
func (m *Monitor) Statistics() ([]*statistic, error) {
	statistics := make([]*statistic, 0)

	expvar.Do(func(kv expvar.KeyValue) {
		// Skip built-in expvar stats.
		if kv.Key == "memstats" || kv.Key == "cmdline" {
			return
		}

		statistic := &statistic{
			Tags:   make(map[string]string),
			Values: make(map[string]interface{}),
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
						f, err = strconv.ParseUint(v.String(), 10, 64)
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

	return statistics, nil
}

func (m *Monitor) Diagnostics() (map[string]*Diagnostic, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	diags := make(map[string]*Diagnostic, len(m.diagRegistrations))
	for k, v := range m.diagRegistrations {
		d, err := v.Diagnostics()
		if err != nil {
			continue
		}
		diags[k] = d
	}
	return diags, nil
}

// storeStatistics writes the statistics to an InfluxDB system.
func (m *Monitor) storeStatistics() {
	defer m.wg.Done()
	m.Logger.Printf("storing statistics in database '%s', interval %s",
		m.storeDatabase, m.storeInterval)

	if err := m.MetaStore.WaitForLeader(leaderWaitTimeout); err != nil {
		m.Logger.Printf("failed to detect a cluster leader, terminating storage: %s", err.Error())
		return
	}

	if _, err := m.MetaStore.CreateDatabaseIfNotExists(m.storeDatabase); err != nil {
		m.Logger.Printf("failed to create database '%s', terminating storage: %s",
			m.storeDatabase, err.Error())
		return
	}

	tick := time.NewTicker(m.storeInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			stats, err := m.Statistics()
			if err != nil {
				m.Logger.Printf("failed to retrieve registered statistics: %s", err)
				continue
			}

			points := make(tsdb.Points, 0, len(stats))
			for _, s := range stats {
				points = append(points, tsdb.NewPoint(s.Name, s.Tags, s.Values, time.Now()))
			}

			err = m.PointsWriter.WritePoints(&cluster.WritePointsRequest{
				Database:         m.storeDatabase,
				RetentionPolicy:  "",
				ConsistencyLevel: cluster.ConsistencyLevelOne,
				Points:           points,
			})
			if err != nil {
				m.Logger.Printf("failed to store statistics: %s", err)
			}
		case <-m.done:
			m.Logger.Printf("terminating storage of statistics")
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

// newStatistic returns a new statistic object.
func newStatistic(name string, tags map[string]string, values map[string]interface{}) *statistic {
	return &statistic{
		Name:   name,
		Tags:   tags,
		Values: values,
	}
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

// DiagnosticFromMap returns a Diagnostic from a map.
func DiagnosticFromMap(m map[string]interface{}) *Diagnostic {
	// Display columns in deterministic order.
	sortedKeys := make([]string, 0, len(m))
	for k, _ := range m {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	d := NewDiagnostic(sortedKeys)
	row := make([]interface{}, len(sortedKeys))
	for i, k := range sortedKeys {
		row[i] = m[k]
	}
	d.AddRow(row)

	return d
}
