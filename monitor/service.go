package monitor

import (
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Client is the interface modules must implement if they wish to register with monitor.
type Client interface {
	Statistics() (*expvar.Map, error)
	Diagnostics() (map[string]interface{}, error)
}

type Statistic struct {
	Name   string
	Tags   map[string]string
	Values map[string]float64
}

func newStatistic(name string, tags map[string]string, values map[string]float64) *Statistic {
	var a map[string]string

	if tags == nil {
		a = make(map[string]string)
	} else {
		a = tags
	}

	return &Statistic{
		Name:   name,
		Tags:   a,
		Values: values,
	}
}

type clientWithMeta struct {
	Client
	name string
	tags map[string]string
}

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

	expvarAddress string

	Logger *log.Logger
}

func NewService(c Config) *Service {
	return &Service{
		registrations: make([]*clientWithMeta, 0),
		storeEnabled:  c.StoreEnabled,
		storeDatabase: c.StoreDatabase,
		storeAddress:  c.StoreAddress,
		storeInterval: time.Duration(c.StoreInterval),
		expvarAddress: c.ExpvarAddress,
		Logger:        log.New(os.Stderr, "[monitor] ", log.LstdFlags),
	}
}

// Open opens the monitoring service, using the given clusterID, node ID, and hostname
// for identification purposes.
func (s *Service) Open(clusterID, nodeID uint64, hostname string) error {
	s.Logger.Printf("starting monitor service for cluster %s, host %s", clusterID, hostname)
	s.clusterID = clusterID
	s.nodeID = nodeID
	s.hostname = hostname

	// If enabled, record stats in a InfluxDB system.
	if s.storeEnabled {
		s.Logger.Printf("storing in %s, database '%s', interval %s",
			s.storeAddress, s.storeDatabase, s.storeInterval)
		// Ensure database exists.
		values := url.Values{}
		values.Set("q", fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %d", s.storeDatabase))
		resp, err := http.Get(s.storeAddress + "/query?" + values.Encode())
		if err != nil {
			return fmt.Errorf("failed to create monitoring database on %s:", s.storeAddress, err.Error())
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to create monitoring database on %s, received code: %d",
				s.storeAddress, resp.StatusCode)
		}
		s.Logger.Printf("succesfully created database %s on %s", s.storeDatabase, s.storeAddress)

		// XXX Self-register Go runtime stuff from expvar

		// Start periodic writes to system.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			tick := time.NewTicker(s.storeInterval)
			for {
				select {
				case <-tick.C:
					if err := s.storeStatistics(); err != nil {
						s.Logger.Printf("failed to write statistics to %s: %s", s.storeAddress, err.Error())
					}
				case <-s.done:
					return
				}

			}
		}()
	}

	// If enabled, expose all expvar data over HTTP.
	if s.expvarAddress != "" {
		listener, err := net.Listen("tcp", s.expvarAddress)
		if err != nil {
			return err
		}

		go func() {
			http.Serve(listener, nil)
		}()
		s.Logger.Println("expvar information available on %s", s.expvarAddress)
	}
	return nil
}

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

// Register registers a client with the given name and tags.
func (s *Service) Register(name string, tags map[string]string, client Client) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := &clientWithMeta{
		Client: client,
		name:   name,
		tags:   tags,
	}
	s.registrations = append(s.registrations, c)
	s.Logger.Printf(`'%s:%v' registered for monitoring`, name, tags)
	return nil
}

func (s *Service) ExecuteStatement(stmt influxql.Statement) *influxql.Result {
	switch stmt := stmt.(type) {
	case *influxql.ShowStatsStatement:
		return s.executeShowStatistics(stmt)
	default:
		panic(fmt.Sprintf("unsupported statement type: %T", stmt))
	}
}

func (s *Service) executeShowStatistics(q *influxql.ShowStatsStatement) *influxql.Result {
	stats, _ := s.statistics()
	rows := make([]*influxql.Row, len(stats))

	for n, stat := range stats {
		row := &influxql.Row{}
		for k, v := range stat.Tags {
			row.Columns = append(row.Columns, k)
			row.Values = append(row.Values, []interface{}{v})
		}
		for k, v := range stat.Values {
			row.Columns = append(row.Columns, k)
			row.Values = append(row.Values, []interface{}{v})
		}
		rows[n] = row
	}
	return &influxql.Result{Series: rows}
}

func (s *Service) statistics() ([]*Statistic, error) {
	statistics := make([]*Statistic, len(s.registrations))
	for i, r := range s.registrations {
		values := make(map[string]float64, 0)
		ep, err := r.Client.Statistics()
		if err != nil {
			continue
		}

		ep.Do(func(kv expvar.KeyValue) {
			f, ok := kv.Value.(*expvar.Float)
			if !ok {
				return
			}
			v, err := strconv.ParseFloat(f.String(), 64)
			if err != nil {
				return
			}
			values[kv.Key] = v
		})

		a := newStatistic(r.name, r.tags, values)
		a.Tags["clusterID"] = strconv.FormatUint(s.clusterID, 10)
		a.Tags["nodeID"] = strconv.FormatUint(s.nodeID, 10)
		a.Tags["hostname"] = s.hostname

		statistics[i] = a
	}
	return statistics, nil
}

func (s *Service) storeStatistics() error {
	// XXX add tags such as local hostname and cluster ID
	return nil
}
