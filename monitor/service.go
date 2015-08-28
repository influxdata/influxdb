package monitor

import (
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
)

// Client is the interface modules must implement if they wish to register with monitor.
type Client interface {
	Statistics() (*expvar.Map, error)
	Diagnostics() (map[string]interface{}, error)
}

type clientWithTags struct {
	Client
	tags map[string]string
}

type Service struct {
	mu            sync.Mutex
	registrations map[string]clientWithTags

	expvarAddress string

	Logger *log.Logger
}

func NewService(c Config) *Service {
	return &Service{
		registrations: make(map[string]clientWithTags, 0),
		expvarAddress: c.ExpvarAddress,
		Logger:        log.New(os.Stderr, "[monitor] ", log.LstdFlags),
	}
}

// Open opens the monitoring service.
func (s *Service) Open() error {
	s.Logger.Println("starting monitor service")

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

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// Register registers a client with the given name. It is an error to register a client with
// an already registered key.
func (s *Service) Register(name string, tags map[string]string, client Client) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.registrations[name]; ok {
		return fmt.Errorf("existing client registered with name %s", name)
	}

	s.registrations[name] = clientWithTags{tags: tags, Client: client}
	s.Logger.Printf(`'%s:%v' registered for monitoring`, name, tags)
	return nil
}

// Deregister deregisters any client previously registered with the existing name.
func (s *Service) Deregister(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.registrations, name)
}
