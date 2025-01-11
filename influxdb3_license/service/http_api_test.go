package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/time/rate"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/config"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/email"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/internal/testutil"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license/signer"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store/postgres"
)

func testHandler(t *testing.T) *HTTPHandler {
	t.Helper()

	testDB := testutil.NewTestDB(t)
	ctx := context.Background()
	testDB.Setup(ctx)
	t.Cleanup(testDB.Cleanup)
	store := postgres.NewStore(testDB.DB)

	privKey := "self-managed_test_private-key.pem"
	pubKey := "self-managed_test_public-key.pem"

	signMethod, err := signer.NewLocalSigningMethod(privKey, pubKey)
	if err != nil {
		t.Fatalf("Error creating local signer: %v", err)
	}
	lic, err := license.NewCreator(signMethod, privKey, pubKey)

	cfg := config.Config{
		TrialEndDate: time.Now().Add(time.Second * 120),
	}
	logger := zaptest.NewLogger(t)
	logLimiter := &rate.Sometimes{First: 10, Interval: 10 * time.Second}

	mailer := &testutil.MockMailer{}
	emailCfg := email.DefaultConfig(mailer, store, logger)
	emailSvc := testutil.NewTestService(t, emailCfg)

	return &HTTPHandler{
		cfg:        &cfg,
		logger:     logger,
		store:      store,
		emailSvc:   emailSvc.Svc,
		lic:        lic,
		logLimiter: logLimiter,
	}
}

type testFixtures struct {
	store store.Store
	srv   *httptest.Server
}

func getTestFixtures(t *testing.T) testFixtures {
	handler := testHandler(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return testFixtures{
		srv:   ts,
		store: handler.store,
	}
}

func TestLicenseCreate(t *testing.T) {
	testFixtures := getTestFixtures(t)

	tests := []struct {
		name       string
		email      string
		hostID     string
		doActivate bool

		expectedPostResponseStatus  int
		expectedPostResponseHeaders map[string]string
		expectedGetResponseStatus   int
		expectedGetResponseHeaders  map[string]string
	}{
		{
			name:       "email username must be url-encoded",
			email:      "invalid+email@influxdata.com",
			hostID:     "meow",
			doActivate: true,
			// TODO: this might should probably be a bug but it works
			expectedPostResponseStatus:  201,
			expectedPostResponseHeaders: map[string]string{},
			expectedGetResponseStatus:   200,
			expectedGetResponseHeaders:  map[string]string{},
		},
		{
			name:                        "404 response for unverified email address",
			email:                       "fake1@influxdata.com",
			hostID:                      "meow",
			doActivate:                  false,
			expectedPostResponseStatus:  201,
			expectedPostResponseHeaders: map[string]string{},
			expectedGetResponseStatus:   404,
			expectedGetResponseHeaders:  map[string]string{},
		},
		{
			name:                        "only user name needs to be url-encoded",
			email:                       "valid_1%2Bemail@influxdata.com",
			hostID:                      "meow",
			doActivate:                  true,
			expectedPostResponseStatus:  201,
			expectedPostResponseHeaders: map[string]string{},
			expectedGetResponseStatus:   200,
			expectedGetResponseHeaders:  map[string]string{},
		},
		{
			name:                        "at-sign can also be url-encoded",
			email:                       "valid_2%2Bemail%40influxdata.com",
			hostID:                      "meow",
			doActivate:                  true,
			expectedPostResponseStatus:  201,
			expectedPostResponseHeaders: map[string]string{},
			expectedGetResponseStatus:   200,
			expectedGetResponseHeaders:  map[string]string{},
		},
	}

	client := &http.Client{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instanceID, _ := uuid.NewV4()
			url := fmt.Sprintf("%s/licenses?email=%s&host-id=%s&instance-id=%s", testFixtures.srv.URL, tt.email, tt.hostID, instanceID)
			res, err := http.Post(url, "", nil)
			if err != nil {
				t.Fatalf("must be able to issue http POST request to test server: %s", err.Error())
			}

			require.Equal(t, tt.expectedPostResponseStatus, res.StatusCode, "actual response must match expected")
			location := res.Header.Get("Location")
			require.True(t, url != "", "must return a 'Location' header")
			url = fmt.Sprintf("%s%s", testFixtures.srv.URL, location)

			if tt.doActivate {
				ctx := context.Background()
				tx, err := testFixtures.store.BeginTx(ctx)
				require.NoError(t, err)

				license, err := testFixtures.store.GetLicenseByInstanceID(ctx, tx, instanceID.String())
				require.NoError(t, err)

				err = testFixtures.store.SetLicenseState(ctx, tx, license.ID, store.LicenseStateActive)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			}

			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				t.Fatalf("must be able to create http GET request: %s", err.Error())
			}

			t.Logf("request: method %s url %s", req.Method, req.URL)

			res, err = client.Do(req)
			if err != nil {
				t.Fatalf("must be able to issue http GET request to test server: %s", err.Error())
			}

			require.Equal(t, tt.expectedGetResponseStatus, res.StatusCode, "actual response must match expected")

		})
	}

}
