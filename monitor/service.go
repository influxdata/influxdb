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
)

const leaderWaitTimeout = 30 * time.Second

// Client is the interface modules must implement if they wish to register with monitor.
type Client interface {
	Statistics() (map[string]interface{}, error)
	Diagnostics() (map[string]interface{}, error)
}

// Monitor represents an instance of the monitor system.
type Monitor struct {
	wg            sync.WaitGroup
	done          chan struct{}
	mu            sync.Mutex
	registrations []*clientWithMeta

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
		done:          make(chan struct{}),
		registrations: make([]*clientWithMeta, 0),
		storeEnabled:  c.StoreEnabled,
		storeDatabase: c.StoreDatabase,
		storeInterval: time.Duration(c.StoreInterval),
		Logger:        log.New(os.Stderr, "[monitor] ", log.LstdFlags),
	}
}

// Open opens the monitoring system, using the given clusterID, node ID, and hostname
// for identification purposem.
func (m *Monitor) Open() error {
	m.Logger.Printf("Starting monitor system")

	// Self-register Go runtime statm.
	m.Register("runtime", nil, &goRuntime{})

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

// Register registers a client with the given name and tags.
func (m *Monitor) Register(name string, tags map[string]string, client Client) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := &clientWithMeta{
		Client: client,
		name:   name,
		tags:   tags,
	}
	m.registrations = append(m.registrations, c)
	m.Logger.Printf(`'%s:%v' registered for monitoring`, name, tags)
	return nil
}

// statistics returns the combined statistics for all registered clients.
func (m *Monitor) Statistics() ([]*statistic, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	statistics := make([]*statistic, 0, len(m.registrations))
	for _, r := range m.registrations {
		stats, err := r.Client.Statistics()
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
func (m *Monitor) storeStatistics() {
	// XXX add tags such as local hostname and cluster ID
	//a.Tags["clusterID"] = strconv.FormatUint(m.clusterID, 10)
	//a.Tags["nodeID"] = strconv.FormatUint(m.nodeID, 10)
	//a.Tags["hostname"] = m.hostname
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
			// Write stats here.
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

// clientWithMeta wraps a registered client with its associated name and tagm.
type clientWithMeta struct {
	Client
	name string
	tags map[string]string
}

// MonitorClient wraps a *expvar.Map so that it implements the Client interface. It is for
// use by external packages that just record stats in an expvar.Map type.
type MonitorClient struct {
	ep *expvar.Map
}

// NewMonitorClient returns a new MonitorClient using the given expvar.Map.
func NewMonitorClient(ep *expvar.Map) *MonitorClient {
	return &MonitorClient{ep: ep}
}

// Statistics implements the Client interface for a MonitorClient.
func (m MonitorClient) Statistics() (map[string]interface{}, error) {
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

// Diagnostics implements the Client interface for a MonitorClient.
func (m MonitorClient) Diagnostics() (map[string]interface{}, error) {
	return nil, nil
}
