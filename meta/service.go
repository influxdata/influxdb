package meta

import (
	"io/ioutil"
	"net/http"

	"github.com/hashicorp/raft"
	//"github.com/hashicorp/raft-boltdb"
)

// Service represents
type Service struct {
	raft *raft.Raft
}

// NewService returns a new instance of Service.
func NewService(cache *Cache) *Service {
	return &Service{}
}

func (s *Service) Open() error
func (s *Service) Close() error
func (s *Service) Exec([]byte) error

// ServiceHandler represents an HTTP interface to the service.
type ServiceHandler struct {
	Service interface {
		Exec([]byte) error
	}
}

// ServeHTTP serves HTTP requests for the handler.
func (h *ServiceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/commands":
		if r.Method == "POST" {
			h.postCommand(w, r)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

// postCommand reads a command from the body and executes it against the service.
func (h *ServiceHandler) postCommand(w http.ResponseWriter, r *http.Request) {
	// Read request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute command.
	if err := h.Service.Exec(body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
