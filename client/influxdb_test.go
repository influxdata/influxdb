package client_test

import (
	"testing"

	"github.com/influxdb/influxdb/client"
)

func TestNewClient(t *testing.T) {
	config := client.Config{}
	_, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %s, got %s", nil, err)
	}
}

func TestClient_Query(t *testing.T) {
	config := client.Config{}
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %s, got %s", nil, err)
	}

	query := client.Query{}
	_, err = c.Query(query)
	if err != nil {
		t.Fatalf("unexpected error.  expected %s, got %s", nil, err)
	}
}

func TestClient_Write(t *testing.T) {
	config := client.Config{}
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %s, got %s", nil, err)
	}

	write := client.Write{}
	_, err = c.Write(write)
	if err != nil {
		t.Fatalf("unexpected error.  expected %s, got %s", nil, err)
	}
}
