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
	influxdb.UserService
	influxdb.PasswordsService
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
	influxdb.BucketService
}

func NewService(st *Store) *Service {
	svc := &Service{}
	userSvc := NewUserSvc(st, svc)
	svc.UserService = userSvc
	svc.PasswordsService = userSvc
	svc.UserResourceMappingService = NewUserResourceMappingSvc(st, svc)
	svc.OrganizationService = NewOrganizationSvc(st, svc)
	svc.BucketService = NewBucketSvc(st, svc)

	return svc
}

type TenantSystem struct {
	UserSvc     influxdb.UserService
	PasswordSvc influxdb.PasswordsService
	UrmSvc      influxdb.UserResourceMappingService
	OrgSvc      influxdb.OrganizationService
	BucketSvc   influxdb.BucketService
}

func NewSystem(store *Store, log *zap.Logger, reg prometheus.Registerer, metricOpts ...metric.ClientOptFn) *TenantSystem {
	ts := NewService(store)
	return &TenantSystem{
		UserSvc:     NewUserLogger(log, NewUserMetrics(reg, ts, metricOpts...)),
		PasswordSvc: NewPasswordLogger(log, NewPasswordMetrics(reg, ts, metricOpts...)),
		UrmSvc:      NewURMLogger(log, NewUrmMetrics(reg, ts, metricOpts...)),
		OrgSvc:      NewOrgLogger(log, NewOrgMetrics(reg, ts, metricOpts...)),
		BucketSvc:   NewBucketLogger(log, NewBucketMetrics(reg, ts, metricOpts...)),
	}
}

func (ts *TenantSystem) NewOrgHTTPHandler(log *zap.Logger, secretSvc influxdb.SecretService) *OrgHandler {
	secretHandler := secret.NewHandler(log, "id", secret.NewAuthedService(secretSvc))
	urmHandler := NewURMHandler(log.With(zap.String("handler", "urm")), influxdb.OrgsResourceType, "id", ts.UserSvc, NewAuthedURMService(ts.OrgSvc, ts.UrmSvc))
	return NewHTTPOrgHandler(log.With(zap.String("handler", "org")), NewAuthedOrgService(ts.OrgSvc), urmHandler, secretHandler)
}

func (ts *TenantSystem) NewBucketHTTPHandler(log *zap.Logger, labelSvc influxdb.LabelService) *BucketHandler {
	urmHandler := NewURMHandler(log.With(zap.String("handler", "urm")), influxdb.OrgsResourceType, "id", ts.UserSvc, NewAuthedURMService(ts.OrgSvc, ts.UrmSvc))
	labelHandler := label.NewHTTPEmbeddedHandler(log.With(zap.String("handler", "label")), influxdb.BucketsResourceType, labelSvc)
	return NewHTTPBucketHandler(log.With(zap.String("handler", "bucket")), NewAuthedBucketService(ts.BucketSvc), labelSvc, urmHandler, labelHandler)
}

func (ts *TenantSystem) NewUserHTTPHandler(log *zap.Logger) *UserHandler {
	return NewHTTPUserHandler(log.With(zap.String("handler", "user")), NewAuthedUserService(ts.UserSvc), NewAuthedPasswordService(ts.PasswordSvc))
}
