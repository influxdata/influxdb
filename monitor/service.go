package monitor

import (
	"expvar"
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

type clientWithMeta struct {
	Client
	name string
	tags map[string]string
}

type Service struct {
	mu            sync.Mutex
	registrations []*clientWithMeta

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
