package monitor

import (
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// Client is the interface modules must implement if they wish to register with monitor.
type Client interface {
	Statistics() (*expvar.Map, error)
	Diagnostics() (map[string]interface{}, error)
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
		expvarAddress: c.ExpvarAddress,
		Logger:        log.New(os.Stderr, "[monitor] ", log.LstdFlags),
	}
}

// Open opens the monitoring service.
func (s *Service) Open() error {
	s.Logger.Println("starting monitor service")

	// If enabled, record stats in a InfluxDB system.
	if s.storeEnabled {
		// Ensure database exists.
		values := url.Values{}
		values.Set("q", fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %d", s.storeDatabase))
		resp, err := http.Get(s.storeAddress + "/query?" + values.Encode())
		if err != nil {
			return fmt.Errorf("failed to create monitoring database on %s:", s.storeAddress, err.Error())
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to create monitoring database on %s, received code: %d", s.storeAddress, resp.StatusCode)
		}
		s.Logger.Println("succesfully created database %s on %s", s.storeDatabase, s.storeAddress)

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

func (s *Service) storeStatistics() error {
	return nil
}
