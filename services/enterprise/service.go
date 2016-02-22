package enterprise

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/influxdata/enterprise-client/v2"
	"github.com/influxdata/influxdb/monitor"
)

// Service represents the enterprise service.
type Service struct {
	MetaStore interface {
		ClusterID() uint64
		NodeID() uint64
	}
	Monitor interface {
		Statistics(tags map[string]string) ([]*monitor.Statistic, error)
		RegisterDiagnosticsClient(name string, client monitor.DiagsClient)
	}

	enabled       bool
	hosts         []*client.Host
	statsInterval time.Duration
	version       string

	mu          sync.Mutex
	lastContact time.Time

	adminPort string

	wg   sync.WaitGroup
	done chan struct{}

	logger *log.Logger
}

// NewService returns a configured enterprise service.
func NewService(c Config, version string) (*Service, error) {
	return &Service{
		enabled:       c.Enabled,
		hosts:         c.Hosts,
		statsInterval: time.Duration(c.StatsInterval),
		version:       version,
		done:          make(chan struct{}),
		logger:        log.New(os.Stderr, "[enterprise] ", log.LstdFlags),
		adminPort:     fmt.Sprintf(":%d", c.AdminPort),
	}, nil
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	if !s.enabled {
		return nil
	}

	s.logger.Println("Starting enterprise service")
	if err := s.registerServer(); err != nil {
		return err
	}

	// Register diagnostics if a Monitor service is available.
	if s.Monitor != nil {
		s.Monitor.RegisterDiagnosticsClient("enterprise", s)
	}

	s.wg.Add(2)
	go s.reportStats()

	return nil
}

// Close stops stats reporting to enterprise
func (s *Service) Close() error {
	s.logger.Println("enterprise service terminating")
	close(s.done)
	s.wg.Wait()
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.logger = l
}

// Diagnostics returns diagnostics information.
func (s *Service) Diagnostics() (*monitor.Diagnostic, error) {
	diagnostics := map[string]interface{}{
		"hosts":        s.hosts,
		"last_contact": s.lastContact(),
	}

	return monitor.DiagnosticFromMap(diagnostics), nil
}

// registerServer registers the server.
func (s *Service) registerServer() error {
	if !s.enabled {
		return nil
	}

	cl, err := client.New(s.hosts)
	if err != nil {
		s.logger.Printf("Unable to contact one or more enterprise hosts: %s", err.Error())
		return err
	}

	clusterID := s.MetaStore.ClusterID()
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	adminURL := "http://" + hostname + s.adminPort
	_, err = url.Parse(adminURL)
	if err != nil {
		return err
	}

	product := client.Product{
		ClusterID: fmt.Sprintf("%d", clusterID),
		ProductID: fmt.Sprintf("%d", s.MetaStore.NodeID()),
		Host:      hostname,
		Name:      "influxdb",
		Version:   s.version,
		AdminURL:  adminURL,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		resp, err := cl.Register(&product)

		if err != nil {
			s.logger.Printf("Failed to register InfluxDB with %s: received code %s, error: %s", resp.Response.Request.URL.String(), resp.Status, err)
			return
		}
		s.hosts = cl.Hosts
		s.updateLastContact(time.Now().UTC())
	}()
	return nil
}

func (s *Service) reportStats() {
	defer s.wg.Done()

	cl, err := client.New(s.hosts)
	if err != nil {
		s.logger.Printf("Unable to contact one or more enterprise hosts: %s", err.Error())
	}

	clusterID := s.MetaStore.ClusterID()

	t := time.NewTicker(s.statsInterval)
	for {
		select {
		case <-t.C:
			stats, err := s.Monitor.Statistics(nil)
			if err != nil {
				s.logger.Printf("Failed to retrieve statistics: %s", err.Error())
				continue
			}

			st := client.Stats{
				ProductName: "influxdb",
				ClusterID:   fmt.Sprintf("%d", clusterID),
				ProductID:   fmt.Sprintf("%d", s.MetaStore.NodeID()),
			}
			data := make([]client.StatsData, len(stats))
			for i, x := range stats {
				data[i] = client.StatsData{
					Name:   x.Name,
					Tags:   x.Tags,
					Values: x.Values,
				}
			}
			st.Data = data

			resp, err := cl.Save(st)
			if err != nil {
				s.logger.Printf("Failed to post statistics to Enterprise: repsonse code: %d: error: %s", resp.StatusCode, err)
				continue
			}
			s.updateLastContact(time.Now().UTC())

		case <-s.done:
			return
		}
	}
}

func (s *Service) updateLastContact(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastContact = t
}

func (s *Service) lastContact() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastContact.String()
}
