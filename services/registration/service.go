package registration

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/influxdb/enterprise-client/v1"
	"github.com/influxdb/influxdb/monitor"
)

// Service represents the registration service.
type Service struct {
	MetaStore interface {
		ClusterID() (uint64, error)
		NodeID() uint64
	}
	Monitor interface {
		Statistics(tags map[string]string) ([]*monitor.Statistic, error)
		RegisterDiagnosticsClient(name string, client monitor.DiagsClient)
	}

	enabled       bool
	url           *url.URL
	token         string
	statsInterval time.Duration
	version       string
	mu            sync.Mutex
	lastContact   time.Time

	wg   sync.WaitGroup
	done chan struct{}

	logger *log.Logger
}

// NewService returns a configured registration service.
func NewService(c Config, version string) (*Service, error) {
	url, err := url.Parse(c.URL)
	if err != nil {
		return nil, err
	}

	return &Service{
		enabled:       c.Enabled,
		url:           url,
		token:         c.Token,
		statsInterval: time.Duration(c.StatsInterval),
		version:       version,
		done:          make(chan struct{}),
		logger:        log.New(os.Stderr, "[registration] ", log.LstdFlags),
	}, nil
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	if !s.enabled {
		return nil
	}

	s.logger.Println("Starting registration service")
	if err := s.registerServer(); err != nil {
		return err
	}

	// Register diagnostics if a Monitor service is available.
	if s.Monitor != nil {
		s.Monitor.RegisterDiagnosticsClient("registration", s)
	}

	s.wg.Add(1)
	go s.reportStats()

	return nil
}

// Close stops retention policy enforcement.
func (s *Service) Close() error {
	s.logger.Println("registration service terminating")
	close(s.done)
	s.wg.Wait()
	return nil
}

func (s *Service) Diagnostics() (*monitor.Diagnostic, error) {
	diagnostics := map[string]interface{}{
		"URL":          s.url.String(),
		"token":        s.token,
		"last_contact": s.getLastContact().String(),
	}

	return monitor.DiagnosticFromMap(diagnostics), nil
}

// registerServer registers the server.
func (s *Service) registerServer() error {
	if !s.enabled || s.token == "" {
		return nil
	}

	cl := client.New(s.token)
	cl.URL = s.url.String()

	clusterID, err := s.MetaStore.ClusterID()
	if err != nil {
		s.logger.Printf("failed to retrieve cluster ID for registration: %s", err.Error())
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	server := client.Server{
		ClusterID: fmt.Sprintf("%d", clusterID),
		ServerID:  fmt.Sprintf("%d", s.MetaStore.NodeID()),
		Host:      hostname,
		Product:   "influxdb",
		Version:   s.version,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		resp, err := cl.Save(server)

		if err != nil {
			s.logger.Printf("failed to register server with %s: received code %s, error: %s", s.url.String(), resp.Status, err)
			return
		}
		s.updateLastContact(time.Now().UTC())
	}()
	return nil
}

func (s *Service) reportStats() {
	defer s.wg.Done()
	if s.token == "" {
		// No reporting, for now, without token.
		return
	}

	cl := client.New(s.token)
	cl.URL = s.url.String()

	clusterID, err := s.MetaStore.ClusterID()
	if err != nil {
		s.logger.Printf("failed to retrieve cluster ID for registration -- aborting stats upload: %s", err.Error())
		return
	}

	t := time.NewTicker(s.statsInterval)
	for {
		select {
		case <-t.C:
			stats, err := s.Monitor.Statistics(nil)
			if err != nil {
				s.logger.Printf("failed to retrieve statistics: %s", err.Error())
				continue
			}

			st := client.Stats{
				Product:   "influxdb",
				ClusterID: fmt.Sprintf("%d", clusterID),
				ServerID:  fmt.Sprintf("%d", s.MetaStore.NodeID()),
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
				s.logger.Printf("failed to post statistics to Enterprise: repsonse code: %d: error: %s", resp.StatusCode, err)
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

func (s *Service) getLastContact() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastContact
}
