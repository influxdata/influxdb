package email

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/internal/testutil"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store/postgres"
	"go.uber.org/zap/zaptest"
)

type mockMailer struct {
	SendErr error
}

func (m mockMailer) SendMail(ctx context.Context, email *store.Email) (resp string, id string, err error) {
	if m.SendErr != nil {
		return "", "", m.SendErr
	}
	return "Queued. Thank you.", uuid.New().String(), nil
}

func newServiceConfig() *Config {
	return &Config{
		MaxEmailsPerUserPerLicense: -1,
		RateLimit:                  rate.Limit(-1.0),
		RateLimitBurst:             -1,
		UserRateLimit:              rate.Limit(-1.0),
		UserRateLimitBurst:         -1,
		IPRateLimit:                rate.Limit(-1.0),
		IPRateLimitBurst:           -1,
	}
}

type testService struct {
	db     *sql.DB
	ctx    context.Context
	store  store.Store
	mailer Mailer
	svc    *Service
}

func newTestService(t *testing.T, tscfg *Config) *testService {
	t.Helper()

	testDB := testutil.NewTestDB(t)
	ctx := context.Background()
	testDB.Setup(ctx)
	t.Cleanup(func() { testDB.Cleanup() })

	mailer := &mockMailer{}
	stor := postgres.NewStore(testDB.DB)
	logger := zap.NewNop()

	cfg := DefaultConfig(mailer, stor, logger)

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

	return &testService{
		db:     testDB.DB,
		ctx:    ctx,
		store:  postgres.NewStore(testDB.DB),
		mailer: cfg.Mailer,
		svc:    NewService(cfg),
	}
}

func deleteAllEmails(t *testing.T, s store.Store) {
	t.Helper()

	ctx := context.Background()

	tx, err := s.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Fatal(err)
		}
	}()

	if err := s.DeleteAllEmails(ctx, tx); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestService_StartStop(t *testing.T) {
	tstSvc := newTestService(t, nil)
	pollInterval := time.Second
	tstSvc.svc.Start(pollInterval)
	tstSvc.svc.Stop()
}

func TestService_SendEmail(t *testing.T) {
	// Create the service under test
	ts := newTestService(t, nil)

	// We'll tell the service under test to poll the DB frequently for
	// emails in the queue to send so the test runs quickly. In prod,
	// this would likely be every 2 or 3 seconds at the fastest.
	pollInterval := 100 * time.Millisecond
	ts.svc.Start(pollInterval)
	t.Cleanup(func() { ts.svc.Stop() })

	// Create a db transaction
	tx, err := ts.store.BeginTx(ts.ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Create a test user
	user := &store.User{
		Email: "test@example.com",
	}

	if err := ts.store.CreateUser(ts.ctx, tx, user); err != nil {
		t.Fatal(err)
	}

	// Create the user's IP address record
	uip := &store.UserIP{
		UserID: user.ID,
		IPAddr: net.ParseIP("127.0.0.1"),
	}

	if err := ts.store.CreateUserIP(ts.ctx, tx, uip); err != nil {
		t.Fatal(err)
	}

	// Create a license
	now := time.Now()

	license := &store.License{
		Email:      user.Email,
		HostID:     "host_id",
		InstanceID: uuid.NewString(),
		LicenseKey: "license_key",
		ValidFrom:  now,
		ValidUntil: now.Add(time.Hour),
	}

	if err := ts.store.CreateLicense(ts.ctx, tx, license); err != nil {
		t.Fatal(err)
	}

	// Commit the db transaction
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Create a test email
	email := &store.Email{
		UserID:            user.ID,
		UserIP:            uip.IPAddr,
		VerificationToken: uuid.NewString(),
		LicenseID:         license.ID,
		TemplateName:      "template_name",
		ToEmail:           user.Email,
		Subject:           "subject",
		Body:              "body",
		// This value of State should get overwritten with the DB
		// default value of store.EmailStateScheduled
		State: store.EmailStateSent,
	}

	// Send the email the first time
	if err := ts.svc.QueueEmail(email); err != nil {
		t.Fatal(err)
	}

	// Check that the state of the email
	if email.State != store.EmailStateScheduled {
		t.Fatalf("expected email state to be Scheduled, got %s", email.State)
	}

	// Loop waiting for the send to complete and email to update
	// in the DB
	tx, err = ts.store.BeginTx(ts.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func(tx store.Tx) {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Fatal(err)
		}
	}(tx)

	passed := false
	for n := 0; n < 10; n++ {
		email, err = ts.store.GetEmailByID(ts.ctx, tx, email.ID)
		if err != nil {
			t.Fatal(err)
		}

		if email.SendCnt == 0 {
			// Hasn't updated so wait a little bit and try again
			time.Sleep(100 * time.Millisecond)
			continue
		} else if email.SendCnt == 1 {
			// Updated as expected
			passed = true
			break
		} else if email.SendCnt > 1 || email.SendCnt < 0 {
			t.Fatalf("email send count should be 1, got %d", email.SendCnt)
		}
	}

	if !passed {
		t.Fatalf("email send count should be 1, got %d", email.SendCnt)
	}
}

func TestService_EmailRateLimits(t *testing.T) {
	// Create a config that overrides the default rate limits with lower
	// limits that will be easier to trigger in these tests.
	tscfg := newServiceConfig()
	tscfg.UserRateLimit = rate.Every(time.Second)
	tscfg.UserRateLimitBurst = 3
	tscfg.IPRateLimit = rate.Every(time.Second)
	tscfg.IPRateLimitBurst = 3
	tscfg.RateLimit = rate.Every(time.Second)
	tscfg.RateLimitBurst = tscfg.UserRateLimitBurst + tscfg.IPRateLimitBurst + 3

	if testing.Verbose() {
		tscfg.Logger = zaptest.NewLogger(t)
	} else {
		tscfg.Logger = zap.NewNop()
	}

	ts := newTestService(t, tscfg)

	// We'll tell the service under test to poll the DB frequently
	//pollInterval := 100 * time.Millisecond
	//ts.svc.Start(pollInterval)
	//t.Cleanup(func() { ts.svc.Stop() })

	// Helper function to create a test user with IP and license
	createTestUserWithIP := func(ctx context.Context, email, ipAddr string) (*store.User, *store.UserIP, *store.License, error) {
		tx, err := ts.store.BeginTx(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		defer func() { _ = tx.Rollback() }()

		// Create user
		user := &store.User{
			Email: email,
		}
		if err := ts.store.CreateUser(ctx, tx, user); err != nil {
			return nil, nil, nil, err
		}

		// Create IP record
		uip := &store.UserIP{
			UserID: user.ID,
			IPAddr: net.ParseIP(ipAddr),
		}
		if err := ts.store.CreateUserIP(ctx, tx, uip); err != nil {
			return nil, nil, nil, err
		}

		// Create license
		now := time.Now()
		license := &store.License{
			Email:      user.Email,
			HostID:     "host_id_" + email,
			InstanceID: uuid.NewString(),
			LicenseKey: "license_key_" + email,
			ValidFrom:  now,
			ValidUntil: now.Add(time.Hour),
		}
		if err := ts.store.CreateLicense(ctx, tx, license); err != nil {
			return nil, nil, nil, err
		}

		if err := tx.Commit(); err != nil {
			return nil, nil, nil, err
		}

		return user, uip, license, nil
	}

	// Helper to create an email for a user
	createTestEmail := func(user *store.User, uip *store.UserIP, license *store.License) *store.Email {
		return &store.Email{
			UserID:            user.ID,
			UserIP:            uip.IPAddr,
			VerificationToken: uuid.NewString(),
			LicenseID:         license.ID,
			TemplateName:      "test_template",
			ToEmail:           user.Email,
			Subject:           "Test Subject",
			Body:              "Test Body",
		}
	}

	ctx := context.Background()

	t.Run("UserRateLimit", func(t *testing.T) {
		// Create a user that we'll use to trigger the rate limit
		user, uip, license, err := createTestUserWithIP(ctx, "user_limit@test.com", "192.168.1.1")
		if err != nil {
			t.Fatal(err)
		}

		// Try to send more emails than the user burst limit
		for i := 0; i < ts.svc.cfg.UserRateLimitBurst+1; i++ {
			email := createTestEmail(user, uip, license)
			err := ts.svc.QueueEmail(email)
			if i < ts.svc.cfg.UserRateLimitBurst {
				if err != nil {
					t.Errorf("expected email %d to be accepted, got error: %v", i+1, err)
				}
			} else {
				if err == nil || !errors.Is(err, ErrUserRateLimitExceeded) {
					t.Errorf("expected user rate limit error, got: %v", err)
				}
			}
		}
	})

	t.Run("IPRateLimit", func(t *testing.T) {
		// Create multiple users with the same IP to trigger IP-based rate limit
		sharedIP := "192.168.1.2"
		var users []*store.User
		var uips []*store.UserIP
		var licenses []*store.License

		// Create enough users sharing an IP to exceed the IP rate limit
		for i := 0; i < ts.svc.cfg.IPRateLimitBurst+1; i++ {
			user, uip, license, err := createTestUserWithIP(ctx,
				fmt.Sprintf("ip_limit_%d@test.com", i),
				sharedIP)
			if err != nil {
				t.Fatal(err)
			}
			users = append(users, user)
			uips = append(uips, uip)
			licenses = append(licenses, license)
		}

		// Try to send emails from different users but same IP
		for i, user := range users {
			email := createTestEmail(user, uips[i], licenses[i])
			err := ts.svc.QueueEmail(email)
			if i < ts.svc.cfg.IPRateLimitBurst {
				if err != nil {
					t.Errorf("expected email %d to be accepted, got error: %v", i+1, err)
				}
			} else {
				if err == nil || !errors.Is(err, ErrIPRateLimitExceeded) {
					t.Errorf("expected IP rate limit error, got: %v", err)
				}
			}
		}
	})

	t.Run("OverallRateLimit", func(t *testing.T) {
		// Create many users with different IPs to trigger overall rate limit
		var emails []*store.Email

		// Calculate the number of emails we need to send to trigger the
		// overall rate limit. Since we've already sent the burst limits
		// for both user ID and IP address, we need to account for those.
		n := tscfg.RateLimitBurst - tscfg.IPRateLimitBurst - tscfg.UserRateLimitBurst + 1

		// Create enough users and associated emails to exceed the overall
		// rate limit
		for i := 0; i < n; i++ {
			user, uip, license, err := createTestUserWithIP(ctx,
				fmt.Sprintf("overall_limit_%d@test.com", i),
				fmt.Sprintf("192.168.1.%d", i+10)) // Different IP for each user
			if err != nil {
				t.Fatal(err)
			}
			emails = append(emails, createTestEmail(user, uip, license))
		}

		// Try to send all emails rapidly to trigger overall rate limit
		gotExpErr := false
		for i, email := range emails {
			err := ts.svc.QueueEmail(email)
			if i < len(emails)-1 {
				if err != nil {
					t.Fatalf("expected email %d to be accepted, got error: %v", i+1, err)
				}
			} else {
				if err == nil || !errors.Is(err, ErrOverallRateLimitExceeded) {
					t.Fatalf("expected overall rate limit error, got: %v", err)
				} else {
					gotExpErr = true
					break
				}
			}
		}

		if !gotExpErr {
			t.Fatal("expected overall rate limit error")
		}
	})

	t.Run("ErrOutboundEmailQueueFull", func(t *testing.T) {
		// To make this test simpler to reason about, clear the emails table
		// and reset the global rate limiter.
		deleteAllEmails(t, ts.store)
		ts.svc.limiter = rate.NewLimiter(tscfg.RateLimit, tscfg.RateLimitBurst)

		// Create enough users and associated emails to exceed the overall
		// rate limit
		var emails []*store.Email
		maxQueueLen := ts.svc.cfg.MaxQueueLength

		for i := 0; i <= maxQueueLen+1; i++ {
			user, uip, license, err := createTestUserWithIP(ctx,
				fmt.Sprintf("queue_full_%d@test.com", i),
				fmt.Sprintf("192.168.2.%d", i+10)) // Different IP for each user
			if err != nil {
				t.Fatal(err)
			}
			emails = append(emails, createTestEmail(user, uip, license))
		}

		gotExpErr := false

		// Try to send all emails rapidly to trigger queue full error
		for i, email := range emails {
			err := ts.svc.QueueEmail(email)
			switch err {
			case nil:
				// nothing to do
			case ErrOverallRateLimitExceeded:
				// Even though the overall rate limit was exceeded, the email
				// was still added to the queue to send when it can.
				if i > maxQueueLen {
					t.Fatalf("expeced email %d to be rejected", i)
				}
			case ErrOutboundEmailQueueFull:
				gotExpErr = true
				if i <= maxQueueLen {
					t.Fatalf("expected email %d to be added to queue", i)
				}
			default:
				t.Fatalf("unexpected error while queueing email %d: %v", i, err)
			}
		}

		if !gotExpErr {
			t.Fatal("expected an ErrOutboundEmailQueueFull error")
		}
	})
}
