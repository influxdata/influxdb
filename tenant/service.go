package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type contextKey string

const (
	ctxInternal contextKey = "influx/tenant/internal"
)

func internalCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxInternal, true)
}

func isInternal(ctx context.Context) bool {
	_, ok := ctx.Value(ctxInternal).(bool)
	return ok
}

type Service struct {
	store *Store
	influxdb.UserService
	influxdb.PasswordsService
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
	influxdb.BucketService
}

func (s *Service) RLock() {
	s.store.RLock()
}

func (s *Service) RUnlock() {
	s.store.RUnlock()
}

// NewService creates a new base tenant service.
func NewService(st *Store) *Service {
	svc := &Service{store: st}
	userSvc := NewUserSvc(st, svc)
	svc.UserService = userSvc
	svc.PasswordsService = userSvc
	svc.UserResourceMappingService = NewUserResourceMappingSvc(st, svc)
	svc.OrganizationService = NewOrganizationSvc(st, svc)
	svc.BucketService = NewBucketSvc(st, svc)

	return svc
}

// creates a new Service with logging and metrics middleware wrappers.
func NewSystem(store *Store, log *zap.Logger, reg prometheus.Registerer, metricOpts ...metric.ClientOptFn) *Service {
	ts := NewService(store)
	ts.UserService = NewUserLogger(log, NewUserMetrics(reg, ts.UserService, metricOpts...))
	ts.PasswordsService = NewPasswordLogger(log, NewPasswordMetrics(reg, ts.PasswordsService, metricOpts...))
	ts.UserResourceMappingService = NewURMLogger(log, NewUrmMetrics(reg, ts.UserResourceMappingService, metricOpts...))
	ts.OrganizationService = NewOrgLogger(log, NewOrgMetrics(reg, ts.OrganizationService, metricOpts...))
	ts.BucketService = NewBucketLogger(log, NewBucketMetrics(reg, ts.BucketService, metricOpts...))

	return ts
}

func (ts *Service) NewOrgHTTPHandler(log *zap.Logger, secretSvc influxdb.SecretService) *OrgHandler {
	secretHandler := secret.NewHandler(log, "id", secret.NewAuthedService(secretSvc))
	urmHandler := NewURMHandler(log.With(zap.String("handler", "urm")), influxdb.OrgsResourceType, "id", ts.UserService, NewAuthedURMService(ts.OrganizationService, ts.UserResourceMappingService))
	return NewHTTPOrgHandler(log.With(zap.String("handler", "org")), NewAuthedOrgService(ts.OrganizationService), urmHandler, secretHandler)
}

func (ts *Service) NewBucketHTTPHandler(log *zap.Logger, labelSvc influxdb.LabelService) *BucketHandler {
	urmHandler := NewURMHandler(log.With(zap.String("handler", "urm")), influxdb.BucketsResourceType, "id", ts.UserService, NewAuthedURMService(ts.OrganizationService, ts.UserResourceMappingService))
	labelHandler := label.NewHTTPEmbeddedHandler(log.With(zap.String("handler", "label")), influxdb.BucketsResourceType, labelSvc)
	return NewHTTPBucketHandler(log.With(zap.String("handler", "bucket")), NewAuthedBucketService(ts.BucketService), labelSvc, urmHandler, labelHandler)
}

func (ts *Service) NewUserHTTPHandler(log *zap.Logger) *UserHandler {
	return NewHTTPUserHandler(log.With(zap.String("handler", "user")), NewAuthedUserService(ts.UserService), NewAuthedPasswordService(ts.PasswordsService))
}
