package prometheus_test

import (
	"context"
	"errors"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/prometheus"
	"go.uber.org/zap"
)

// authzSvc is a test helper that returns its Err from every method on the AuthorizationService interface.
type authzSvc struct {
	Err error
}

var _ platform.AuthorizationService = (*authzSvc)(nil)

func (a *authzSvc) FindAuthorizationByID(context.Context, platform2.ID) (*platform.Authorization, error) {
	return nil, a.Err
}

func (a *authzSvc) FindAuthorizationByToken(context.Context, string) (*platform.Authorization, error) {
	return nil, a.Err
}

func (a *authzSvc) FindAuthorizations(context.Context, platform.AuthorizationFilter, ...platform.FindOptions) ([]*platform.Authorization, int, error) {
	return nil, 0, a.Err
}

func (a *authzSvc) CreateAuthorization(context.Context, *platform.Authorization) error {
	return a.Err
}

func (a *authzSvc) DeleteAuthorization(context.Context, platform2.ID) error {
	return a.Err
}

func (a *authzSvc) UpdateAuthorization(context.Context, platform2.ID, *platform.AuthorizationUpdate) (*platform.Authorization, error) {
	return nil, a.Err
}

func TestAuthorizationService_Metrics(t *testing.T) {
	a := new(authzSvc)

	svc := prometheus.NewAuthorizationService()
	svc.AuthorizationService = a
	reg := prom.NewRegistry(zap.NewNop())
	reg.MustRegister(svc.PrometheusCollectors()...)

	ctx := context.Background()
	id := platform2.ID(1)

	if _, err := svc.FindAuthorizationByID(ctx, id); err != nil {
		t.Fatal(err)
	}
	mfs := promtest.MustGather(t, reg)
	m := promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "FindAuthorizationByID", "error": "false"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	if _, err := svc.FindAuthorizationByToken(ctx, ""); err != nil {
		t.Fatal(err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "FindAuthorizationByToken", "error": "false"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	if _, _, err := svc.FindAuthorizations(ctx, platform.AuthorizationFilter{}); err != nil {
		t.Fatal(err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "FindAuthorizations", "error": "false"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	if err := svc.CreateAuthorization(ctx, nil); err != nil {
		t.Fatal(err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "CreateAuthorization", "error": "false"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	var tempID platform2.ID
	if err := svc.DeleteAuthorization(ctx, tempID); err != nil {
		t.Fatal(err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "DeleteAuthorization", "error": "false"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	forced := errors.New("forced error")
	a.Err = forced

	if _, err := svc.FindAuthorizationByID(ctx, id); err != forced {
		t.Fatalf("expected forced error, got %v", err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "FindAuthorizationByID", "error": "true"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	if _, err := svc.FindAuthorizationByToken(ctx, ""); err != forced {
		t.Fatalf("expected forced error, got %v", err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "FindAuthorizationByToken", "error": "true"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	if _, _, err := svc.FindAuthorizations(ctx, platform.AuthorizationFilter{}); err != forced {
		t.Fatalf("expected forced error, got %v", err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "FindAuthorizations", "error": "true"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	if err := svc.CreateAuthorization(ctx, nil); err != forced {
		t.Fatalf("expected forced error, got %v", err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "CreateAuthorization", "error": "true"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}

	if err := svc.DeleteAuthorization(ctx, tempID); err != forced {
		t.Fatalf("expected forced error, got %v", err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "auth_prometheus_requests_total", map[string]string{"method": "DeleteAuthorization", "error": "true"})
	if got := m.GetCounter().GetValue(); got != 1 {
		t.Fatalf("exp 1 request, got %v", got)
	}
}
