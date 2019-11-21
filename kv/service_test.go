package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	"go.uber.org/zap"
)

type mockStore struct {
}

func (s mockStore) View(context.Context, func(kv.Tx) error) error {
	return nil
}

func (s mockStore) Update(context.Context, func(kv.Tx) error) error {
	return nil
}

func TestNewService(t *testing.T) {
	s := kv.NewService(zap.NewNop(), mockStore{})

	if s.Config.SessionLength != influxdb.DefaultSessionLength {
		t.Errorf("Service session length should use default length when not set")
	}

	config := kv.ServiceConfig{
		SessionLength: time.Duration(time.Hour * 4),
	}

	s = kv.NewService(zap.NewNop(), mockStore{}, config)

	if s.Config != config {
		t.Errorf("Service config not set by constructor")
	}
}
