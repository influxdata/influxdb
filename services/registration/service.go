package registration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

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
	clusterID, err := s.MetaStore.ClusterID()
	if err != nil {
		s.logger.Printf("failed to retrieve cluster ID for registration: %s", err.Error())
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	j := map[string]interface{}{
		"cluster_id": fmt.Sprintf("%d", clusterID),
		"server_id":  fmt.Sprintf("%d", s.MetaStore.NodeID()),
		"host":       hostname,
		"product":    "influxdb",
		"version":    s.version,
	}
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/api/v1/servers?token=%s", s.url.String(), s.token)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		client := http.Client{Timeout: time.Duration(5 * time.Second)}
		resp, err := client.Post(url, "application/json", bytes.NewBuffer(b))
		if err != nil {
			s.logger.Printf("failed to register server with %s: %s", s.url.String(), err.Error())
			return
		}
		s.updateLastContact(time.Now().UTC())

		defer resp.Body.Close()
		if resp.StatusCode == http.StatusCreated {
			return
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			s.logger.Printf("failed to read response from registration server: %s", err.Error())
			return
		}
		s.logger.Printf("failed to register server with %s: received code %s, body: %s", s.url.String(), resp.Status, string(body))
	}()
	return nil
}

func (s *Service) reportStats() {
	defer s.wg.Done()
	if s.token == "" {
		// No reporting, for now, without token.
		return
	}
	statsURL := fmt.Sprintf("%s/api/v1/stats/influxdb?token=%s", s.url.String(), s.token)

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

			o := map[string]interface{}{
				"cluster_id": fmt.Sprintf("%d", clusterID),
				"server_id":  fmt.Sprintf("%d", s.MetaStore.NodeID()),
				"stats":      stats,
			}
			b, err := json.Marshal(o)
			if err != nil {
				s.logger.Printf("failed to JSON-encode stats: %s", err.Error())
				continue
			}

			client := http.Client{Timeout: time.Duration(5 * time.Second)}
			resp, err := client.Post(statsURL, "application/json", bytes.NewBuffer(b))
			if err != nil {
				s.logger.Printf("failed to post statistics to %s: %s", statsURL, err.Error())
				continue
			}
			s.updateLastContact(time.Now().UTC())

			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				s.logger.Printf("failed to post statistics to %s: repsonse code: %d", statsURL, resp.StatusCode)
				continue
			}
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
