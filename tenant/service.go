package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
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
	// The raw version (not the interface), retained so tests can reach the
	// concrete type and so UndecoratedUserService can hand out the service
	// without the middleware NewSystem layers over the embedded UserService.
	userSvc *UserSvc
	influxdb.UserService
	influxdb.PasswordsService
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
	influxdb.BucketService
	taskmodel.TaskService
}

func (s *Service) RLock() {
	s.store.RLock()
}

func (s *Service) RUnlock() {
	s.store.RUnlock()
}

// NewService creates a new base tenant service.
func NewService(st *Store, UserSvcOptFns ...func(svc *UserSvc)) *Service {
	svc := &Service{store: st}
	svc.userSvc = NewUserSvc(st, svc, UserSvcOptFns...)
	svc.UserService = svc.userSvc
	svc.PasswordsService = svc.userSvc
	svc.UserResourceMappingService = NewUserResourceMappingSvc(st, svc)
	svc.OrganizationService = NewOrganizationSvc(st, svc)
	svc.BucketService = NewBucketSvc(st, svc)

	return svc
}

// UndecoratedUserService returns the user service without the logging and
// metrics middleware NewSystem wraps around the embedded UserService. It reads
// the same store and honors the same options — SetUserOptions reaches it too —
// so it differs from UserService only in what it reports about itself. On a
// Service from NewService, which has no middleware, the two are the same.
//
// Almost no caller wants this: a user lookup that goes unrecorded is a lookup
// missing from the metrics an operator uses to understand load. Use it only
// where the lookup is not user activity and reporting it as such would be a
// lie. The one caller today is the /health and /ready credential resolver,
// which identifies the caller behind every credentialed probe: through the
// decorated service, a monitor polling every ten seconds becomes a permanent
// stream of find_user_by_id in service_user_new_call_total and a log line per
// probe, indistinguishable from a user actually doing something.
func (s *Service) UndecoratedUserService() influxdb.UserService {
	return s.userSvc
}

func (s *Service) SetUserOptions(opts ...func(*UserSvc)) {
	s.userSvc.SetOptions(opts...)
}

type ServiceOption func(*Service)

func WithTaskService(ts taskmodel.TaskService) ServiceOption {
	return func(s *Service) { s.TaskService = ts }
}

func (s *Service) Apply(opts ...ServiceOption) {
	for _, opt := range opts {
		opt(s)
	}
}

// creates a new Service with logging and metrics middleware wrappers.
func NewSystem(store *Store, log *zap.Logger, reg prometheus.Registerer, strongPasswords bool, metricOpts ...metric.ClientOptFn) *Service {
	ts := NewService(store, WithPasswordChecking(strongPasswords))
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

func (ts *Service) NewMeHTTPHandler(log *zap.Logger) *MeHandler {
	return NewHTTPMeHandler(log.With(zap.String("handler", "user")), NewAuthedUserService(ts.UserService), NewAuthedPasswordService(ts.PasswordsService))
}
