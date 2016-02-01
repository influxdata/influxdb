package registration

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/influxdata/enterprise-client/v2"
	"github.com/influxdata/enterprise-client/v2/admin"
	"github.com/influxdb/influxdb/monitor"
)

// Service represents the registration service.
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
	mu            sync.Mutex
	lastContact   time.Time
	adminPort     string

	token     string
	secretKey string

	wg   sync.WaitGroup
	done chan struct{}

	logger *log.Logger
}

// NewService returns a configured registration service.
func NewService(c Config, version string) (*Service, error) {
	return &Service{
		enabled:       c.Enabled,
		hosts:         c.Hosts,
		statsInterval: time.Duration(c.StatsInterval),
		version:       version,
		done:          make(chan struct{}),
		logger:        log.New(os.Stderr, "[registration] ", log.LstdFlags),
		adminPort:     fmt.Sprintf(":%d", c.AdminPort),
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

	s.wg.Add(2)
	go s.reportStats()
	go s.launchAdminInterface()

	return nil
}

// Close stops stats reporting to enterprise
func (s *Service) Close() error {
	s.logger.Println("registration service terminating")
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
		"Hosts":        s.hosts,
		"last_contact": s.getLastContact().String(),
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

	product := client.Product{
		ClusterID: fmt.Sprintf("%d", clusterID),
		ProductID: fmt.Sprintf("%d", s.MetaStore.NodeID()),
		Host:      hostname,
		Name:      "influxdb",
		Version:   s.version,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		resp, err := cl.Register(&product)

		if err != nil {
			s.logger.Printf("failed to register InfluxDB with %s: received code %s, error: %s", resp.Response.Request.URL.String(), resp.Status, err)
			return
		}
		for _, host := range cl.Hosts {
			if host.Primary {
				s.token = host.Token
				s.secretKey = host.SecretKey
			}
		}
		s.hosts = cl.Hosts
		s.updateLastContact(time.Now().UTC())
	}()
	return nil
}

func (s *Service) reportStats() {
	defer s.wg.Done()

	for _, host := range s.hosts {
		s.logger.Printf("Reporting stats to %#v", host)
	}

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
				s.logger.Printf("failed to retrieve statistics: %s", err.Error())
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
				s.logger.Printf("failed to post statistics to Enterprise: repsonse code: %d: error: %s", resp.StatusCode, err)
				continue
			}
			s.updateLastContact(time.Now().UTC())

		case <-s.done:
			return
		}
	}
}

func (s *Service) launchAdminInterface() {
	defer s.wg.Done()

	srv := &http.Server{
		Addr:         s.adminPort,
		Handler:      admin.App(s.token, []byte(s.secretKey)),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	l, err := net.Listen("tcp", s.adminPort)
	if err != nil {
		s.logger.Printf("Unable to bind enterprise admin interface to port %s\n", s.adminPort)
		return
	}

	s.logger.Printf("Starting enterprise admin interface on port %s\n", s.adminPort)
	go srv.Serve(l)
	select {
	case <-s.done:
		s.logger.Println("Shutting down enterprise admin interface...")
		l.Close()
		return
		break
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
