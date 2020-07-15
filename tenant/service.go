package tenant

import (
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type Service struct {
	store *Store
}

func NewService(st *Store) influxdb.TenantService {
	return &Service{
		store: st,
	}
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

func (ts *TenantSystem) NewOrgHTTPHandler(log *zap.Logger, labelSvc influxdb.LabelService, secretSvc influxdb.SecretService) *OrgHandler {
	secretHandler := secret.NewHandler(log, "id", secret.NewAuthedService(secretSvc))
	urmHandler := NewURMHandler(log.With(zap.String("handler", "urm")), influxdb.OrgsResourceType, "id", ts.UserSvc, NewAuthedURMService(ts.OrgSvc, ts.UrmSvc))
	labelHandler := label.NewHTTPEmbeddedHandler(log.With(zap.String("handler", "label")), influxdb.OrgsResourceType, labelSvc)
	return NewHTTPOrgHandler(log.With(zap.String("handler", "org")), NewAuthedOrgService(ts.OrgSvc), urmHandler, labelHandler, secretHandler)
}

func (ts *TenantSystem) NewBucketHTTPHandler(log *zap.Logger, labelSvc influxdb.LabelService) *BucketHandler {
	urmHandler := NewURMHandler(log.With(zap.String("handler", "urm")), influxdb.OrgsResourceType, "id", ts.UserSvc, NewAuthedURMService(ts.OrgSvc, ts.UrmSvc))
	labelHandler := label.NewHTTPEmbeddedHandler(log.With(zap.String("handler", "label")), influxdb.BucketsResourceType, labelSvc)
	return NewHTTPBucketHandler(log.With(zap.String("handler", "bucket")), NewAuthedBucketService(ts.BucketSvc), labelSvc, urmHandler, labelHandler)
}

func (ts *TenantSystem) NewUserHTTPHandler(log *zap.Logger) *UserHandler {
	return NewHTTPUserHandler(log.With(zap.String("handler", "user")), NewAuthedUserService(ts.UserSvc), NewAuthedPasswordService(ts.PasswordSvc))
}
