package email_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/google/uuid"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/email"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/internal/testutil"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"go.uber.org/zap/zaptest"
)

func newServiceConfig() *email.Config {
	return &email.Config{
		MaxEmailsPerUserPerLicense: -1,
		RateLimit:                  rate.Limit(-1.0),
		RateLimitBurst:             -1,
		UserRateLimit:              rate.Limit(-1.0),
		UserRateLimitBurst:         -1,
		IPRateLimit:                rate.Limit(-1.0),
		IPRateLimitBurst:           -1,
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
	tstSvc := testutil.NewTestService(t, nil)
	pollInterval := time.Second
	tstSvc.Svc.Start(pollInterval)
	tstSvc.Svc.Stop()
}

func TestService_SendEmail(t *testing.T) {
	// Create the service under test
	ts := testutil.NewTestService(t, nil)

	// We'll tell the service under test to poll the DB frequently for
	// emails in the queue to send so the test runs quickly. In prod,
	// this would likely be every 2 or 3 seconds at the fastest.
	pollInterval := 100 * time.Millisecond
	ts.Svc.Start(pollInterval)
	t.Cleanup(func() { ts.Svc.Stop() })

	// Create a db transaction
	tx, err := ts.Store.BeginTx(ts.Ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Create a test user
	user := &store.User{
		Email: "test@example.com",
	}

	if err := ts.Store.CreateUser(ts.Ctx, tx, user); err != nil {
		t.Fatal(err)
	}

	// Create the user's IP address record
	uip := &store.UserIP{
		UserID: user.ID,
		IPAddr: net.ParseIP("127.0.0.1"),
	}

	if err := ts.Store.CreateUserIP(ts.Ctx, tx, uip); err != nil {
		t.Fatal(err)
	}

	// Create a license
	now := time.Now()

	license := &store.License{
		Email:      user.Email,
		WriterID:   "writer_id",
		InstanceID: uuid.NewString(),
		LicenseKey: "license_key",
		ValidFrom:  now,
		ValidUntil: now.Add(time.Hour),
	}

	if err := ts.Store.CreateLicense(ts.Ctx, tx, license); err != nil {
		t.Fatal(err)
	}

	// Commit the db transaction
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Create a test emailInst
	emailInst := &store.Email{
		UserID:            user.ID,
		UserIP:            uip.IPAddr,
		VerificationToken: uuid.NewString(),
		LicenseID:         license.ID,
		TemplateName:      "template_name",
		To:                user.Email,
		Subject:           "subject",
		Body:              "body",
		// This value of State should get overwritten with the DB
		// default value of store.EmailStateScheduled
		State: store.EmailStateSent,
	}

	tx, err = ts.Store.BeginTx(ts.Ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Send the email the first time
	if err := ts.Svc.QueueEmail(ts.Ctx, tx, emailInst); err != nil {
		t.Fatal(err)
	}

	// Check that the state of the email
	if emailInst.State != store.EmailStateScheduled {
		t.Fatalf("expected email state to be Scheduled, got %s", emailInst.State)
	}

	// Commit the db transaction
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Loop waiting for the send to complete and email to update
	// in the DB
	tx, err = ts.Store.BeginTx(ts.Ctx)
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
		emailInst, err = ts.Store.GetEmailByID(ts.Ctx, tx, emailInst.ID)
		if err != nil {
			t.Fatal(err)
		} else if emailInst == nil {
			t.Fatalf("email not found")
		}

		if emailInst.SendCnt == 0 {
			// Hasn't updated so wait a little bit and try again
			time.Sleep(100 * time.Millisecond)
			continue
		} else if emailInst.SendCnt == 1 {
			// Updated as expected
			passed = true
			break
		} else if emailInst.SendCnt > 1 || emailInst.SendCnt < 0 {
			t.Fatalf("email send count should be 1, got %d", emailInst.SendCnt)
		}
	}

	if !passed {
		t.Fatalf("email send count should be 1, got %d", emailInst.SendCnt)
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

	ts := testutil.NewTestService(t, tscfg)

	// We'll tell the service under test to poll the DB frequently
	//pollInterval := 100 * time.Millisecond
	//ts.svc.Start(pollInterval)
	//t.Cleanup(func() { ts.svc.Stop() })

	// Helper function to create a test user with IP and license
	createTestUserWithIP := func(ctx context.Context, email, ipAddr string) (*store.User, *store.UserIP, *store.License, error) {
		tx, err := ts.Store.BeginTx(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		defer func() { _ = tx.Rollback() }()

		// Create user
		user := &store.User{
			Email: email,
		}
		if err := ts.Store.CreateUser(ctx, tx, user); err != nil {
			return nil, nil, nil, err
		}

		// Create IP record
		uip := &store.UserIP{
			UserID: user.ID,
			IPAddr: net.ParseIP(ipAddr),
		}
		if err := ts.Store.CreateUserIP(ctx, tx, uip); err != nil {
			return nil, nil, nil, err
		}

		// Create license
		now := time.Now()
		license := &store.License{
			Email:      user.Email,
			WriterID:   "writer_id_" + email,
			InstanceID: uuid.NewString(),
			LicenseKey: "license_key_" + email,
			ValidFrom:  now,
			ValidUntil: now.Add(time.Hour),
		}
		if err := ts.Store.CreateLicense(ctx, tx, license); err != nil {
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
			To:                user.Email,
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
		for i := 0; i < ts.Svc.Cfg().UserRateLimitBurst+1; i++ {
			emailInst := createTestEmail(user, uip, license)
			tx, err := ts.Store.BeginTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()

			err = ts.Svc.QueueEmail(ts.Ctx, tx, emailInst)
			if i < ts.Svc.Cfg().UserRateLimitBurst {
				if err != nil {
					t.Errorf("expected email %d to be accepted, got error: %v", i+1, err)
				}
			} else {
				if err == nil || !errors.Is(err, email.ErrUserRateLimitExceeded) {
					t.Errorf("expected user rate limit error, got: %v", err)
				}
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
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
		for i := 0; i < ts.Svc.Cfg().IPRateLimitBurst+1; i++ {
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
			emailInst := createTestEmail(user, uips[i], licenses[i])
			tx, err := ts.Store.BeginTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()

			err = ts.Svc.QueueEmail(ts.Ctx, tx, emailInst)
			if i < ts.Svc.Cfg().IPRateLimitBurst {
				if err != nil {
					t.Errorf("expected email %d to be accepted, got error: %v", i+1, err)
				}
			} else {
				if err == nil || !errors.Is(err, email.ErrIPRateLimitExceeded) {
					t.Errorf("expected IP rate limit error, got: %v", err)
				}
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
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
		for i, emailInst := range emails {
			tx, err := ts.Store.BeginTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()

			err = ts.Svc.QueueEmail(ts.Ctx, tx, emailInst)
			if i < len(emails)-1 {
				if err != nil {
					t.Fatalf("expected email %d to be accepted, got error: %v", i+1, err)
				}
			} else {
				if err == nil || !errors.Is(err, email.ErrOverallRateLimitExceeded) {
					t.Fatalf("expected overall rate limit error, got: %v", err)
				} else {
					gotExpErr = true
					break
				}
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		if !gotExpErr {
			t.Fatal("expected overall rate limit error")
		}
	})

	t.Run("ErrOutboundEmailQueueFull", func(t *testing.T) {
		// To make this test simpler to reason about, clear the emails table
		// and reset the global rate limiter.
		deleteAllEmails(t, ts.Store)
		ts.Svc.SetLimiter(rate.NewLimiter(tscfg.RateLimit, tscfg.RateLimitBurst))

		// Create enough users and associated emails to exceed the overall
		// rate limit
		var emails []*store.Email
		maxQueueLen := ts.Svc.Cfg().MaxQueueLength

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
		for i, emailInst := range emails {
			tx, err := ts.Store.BeginTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()

			err = ts.Svc.QueueEmail(ts.Ctx, tx, emailInst)
			switch err {
			case nil:
				// nothing to do
			case email.ErrOverallRateLimitExceeded:
				// Even though the overall rate limit was exceeded, the email
				// was still added to the queue to send when it can.
				if i > maxQueueLen {
					t.Fatalf("expeced email %d to be rejected", i)
				}
			case email.ErrOutboundEmailQueueFull:
				gotExpErr = true
				if i <= maxQueueLen {
					t.Fatalf("expected email %d to be added to queue", i)
				}
			default:
				t.Fatalf("unexpected error while queueing email %d: %v", i, err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		if !gotExpErr {
			t.Fatal("expected an ErrOutboundEmailQueueFull error")
		}
	})
}
