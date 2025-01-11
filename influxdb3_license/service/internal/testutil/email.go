package testutil

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/email"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store/postgres"
	"go.uber.org/zap"
)

type MockMailer struct {
	SendErr error
}

func (m MockMailer) SendMail(ctx context.Context, email *store.Email) (resp string, id string, err error) {
	if m.SendErr != nil {
		return "", "", m.SendErr
	}
	return "Queued. Thank you.", uuid.New().String(), nil
}

type TestService struct {
	Db     *sql.DB
	Ctx    context.Context
	Store  store.Store
	Mailer email.Mailer
	Svc    *email.Service
}

func NewTestService(t *testing.T, tscfg *email.Config) *TestService {
	t.Helper()

	testDB := NewTestDB(t)
	ctx := context.Background()
	testDB.Setup(ctx)
	t.Cleanup(func() { testDB.Cleanup() })

	mailer := &MockMailer{}
	stor := postgres.NewStore(testDB.DB)
	logger := zap.NewNop()

	cfg := email.DefaultConfig(mailer, stor, logger)

	// Apply default config overrides
	if tscfg != nil {
		if tscfg.Domain != "" {
			cfg.Domain = tscfg.Domain
		}
		if tscfg.EmailTemplateName != "" {
			cfg.EmailTemplateName = tscfg.EmailTemplateName
		}
		if tscfg.MaxEmailsPerUserPerLicense > -1 {
			cfg.MaxEmailsPerUserPerLicense = tscfg.MaxEmailsPerUserPerLicense
		}
		if tscfg.RateLimit > -1.0 {
			cfg.RateLimit = tscfg.RateLimit
		}
		if tscfg.RateLimitBurst > -1 {
			cfg.RateLimitBurst = tscfg.RateLimitBurst
		}
		if tscfg.UserRateLimit > -1.0 {
			cfg.UserRateLimit = tscfg.UserRateLimit
		}
		if tscfg.UserRateLimitBurst > -1 {
			cfg.UserRateLimitBurst = tscfg.UserRateLimitBurst
		}
		if tscfg.IPRateLimit > -1.0 {
			cfg.IPRateLimit = tscfg.IPRateLimit
		}
		if tscfg.IPRateLimitBurst > -1 {
			cfg.IPRateLimitBurst = tscfg.IPRateLimitBurst
		}
		if tscfg.Mailer != nil {
			cfg.Mailer = tscfg.Mailer
		}
		if tscfg.Store != nil {
			cfg.Store = tscfg.Store
		}
		if tscfg.Logger != nil {
			cfg.Logger = tscfg.Logger
		}
	}

	return &TestService{
		Db:     testDB.DB,
		Ctx:    ctx,
		Store:  postgres.NewStore(testDB.DB),
		Mailer: cfg.Mailer,
		Svc:    email.NewService(cfg),
	}}
