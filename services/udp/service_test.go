package udp_test

import (
	"io/ioutil"
	"testing"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/udp"
)

func TestService_OpenClose(t *testing.T) {
	service := NewService(nil)

	// Closing a closed service is fine.
	if err := service.UDPService.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing a closed service again is fine.
	if err := service.UDPService.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.UDPService.Open(); err != nil {
		t.Fatal(err)
	}

	// Opening an already open service is fine.
	if err := service.UDPService.Open(); err != nil {
		t.Fatal(err)
	}

	// Reopening a previously opened service is fine.
	if err := service.UDPService.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.UDPService.Open(); err != nil {
		t.Fatal(err)
	}

	// Tidy up.
	if err := service.UDPService.Close(); err != nil {
		t.Fatal(err)
	}
}

type Service struct {
	UDPService *udp.Service
	MetaClient *internal.MetaClientMock
}

func NewService(c *udp.Config) *Service {
	if c == nil {
		defaultC := udp.NewConfig()
		c = &defaultC
	}

	service := &Service{
		UDPService: udp.NewService(*c),
		MetaClient: &internal.MetaClientMock{},
	}

	service.MetaClient.CreateDatabaseFn = func(string) (*meta.DatabaseInfo, error) { return nil, nil }

	// Set the Meta Client
	service.UDPService.MetaClient = service.MetaClient

	if !testing.Verbose() {
		service.UDPService.SetLogOutput(ioutil.Discard)
	}

	return service
}
