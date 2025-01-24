package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	require.NoError(t, err)
	lic, err := license.NewCreator(signMethod)
	require.NoError(t, err)

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
	tests := []struct {
		name              string
		email             string
		nodeID            string
		doActivateLicense bool

		expectedPostResponseStatus  int
		expectedPostResponseHeaders map[string]string
		expectedGetResponseStatus   int
		expectedGetResponseHeaders  map[string]string
	}{
		{
			name:                        "email username must be url-encoded",
			email:                       "invalid+email@influxdata.com",
			nodeID:                      "meow",
			doActivateLicense:           true,
			expectedPostResponseStatus:  201,
			expectedPostResponseHeaders: map[string]string{},
			expectedGetResponseStatus:   200,
			expectedGetResponseHeaders:  map[string]string{},
		},
		{
			name:                        "404 response for unverified email address",
			email:                       "fake1@influxdata.com",
			nodeID:                      "meow",
			doActivateLicense:           false,
			expectedPostResponseStatus:  201,
			expectedPostResponseHeaders: map[string]string{},
			expectedGetResponseStatus:   404,
			expectedGetResponseHeaders:  map[string]string{},
		},
		{
			name:                        "only user name needs to be url-encoded",
			email:                       "valid_1%2Bemail@influxdata.com",
			nodeID:                      "meow",
			doActivateLicense:           true,
			expectedPostResponseStatus:  201,
			expectedPostResponseHeaders: map[string]string{},
			expectedGetResponseStatus:   200,
			expectedGetResponseHeaders:  map[string]string{},
		},
		{
			name:                        "at-sign can also be url-encoded",
			email:                       "valid_2%2Bemail%40influxdata.com",
			nodeID:                      "meow",
			doActivateLicense:           true,
			expectedPostResponseStatus:  201,
			expectedPostResponseHeaders: map[string]string{},
			expectedGetResponseStatus:   200,
			expectedGetResponseHeaders:  map[string]string{},
		},
	}

	client := &http.Client{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// need to initialize test fixures on a per-test-case basis since to
			// avoid cross-test interference
			testFixtures := getTestFixtures(t)

			instanceID, _ := uuid.NewV4()
			url := fmt.Sprintf("%s/licenses?email=%s&node-id=%s&instance-id=%s", testFixtures.srv.URL, tt.email, tt.nodeID, instanceID)
			res, err := http.Post(url, "", nil)
			if err != nil {
				t.Fatalf("must be able to issue http POST request to test server: %s", err.Error())
			}

			require.Equal(t, tt.expectedPostResponseStatus, res.StatusCode, "actual response must match expected")
			location := res.Header.Get("Location")
			require.True(t, url != "", "must return a 'Location' header")
			url = fmt.Sprintf("%s%s", testFixtures.srv.URL, location)

			if tt.doActivateLicense {
				ctx := context.Background()
				tx, err := testFixtures.store.BeginTx(ctx)
				require.NoError(t, err)

				license, err := testFixtures.store.GetLicenseByInstanceID(ctx, tx, instanceID.String())
				require.NoError(t, err)
				require.NotNil(t, license)

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

// users may prematurely exit the database process or it may time out waiting
// for license verification; because of this, the license server needs to
// return 2xx response rather than a 4xx
func TestLicenseCreateContinuation(t *testing.T) {
	testFixtures := getTestFixtures(t)

	instanceID, _ := uuid.NewV4()
	email := "fake2@influxdata.com"
	nodeID := "meow"

	pathAndQuery := fmt.Sprintf("/licenses?email=%s&node-id=%s&instance-id=%s", email, nodeID, instanceID)
	targetUrl := fmt.Sprintf("%s%s", testFixtures.srv.URL, pathAndQuery)
	expectedLocationHeader := []string{fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(email), instanceID)}

	// first attempt leads to license creation in database and a '201 Created' response
	res, err := http.Post(targetUrl, "", nil)
	require.NoError(t, err)
	require.Equal(t, 201, res.StatusCode)
	header, ok := res.Header["Location"]
	require.True(t, ok, "must have Location header in every 2xx POST response")
	require.Equal(t, expectedLocationHeader, header)

	// second should also result in a '202 Accepted' response
	res, err = http.Post(targetUrl, "", nil)
	require.NoError(t, err)
	require.Equal(t, 202, res.StatusCode)

	header, ok = res.Header["Location"]
	require.True(t, ok, "must have Location header in every 2xx POST response")
	require.Equal(t, expectedLocationHeader, header)

	ctx := context.Background()
	tx, err := testFixtures.store.BeginTx(ctx)
	require.NoError(t, err)
	ls, err := testFixtures.store.GetLicensesByEmail(ctx, tx, email)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// verify there is only one license for this email address/instance combo
	// even though we POST'd twice
	require.Equal(t, 1, len(ls))
}

// this test covers the case where the same user (ie same email address)
// attempts to create a license for two different instances at the same time
// (before email verification)
func TestLicenseCreateSameEmailDifferentInstances(t *testing.T) {
	testFixtures := getTestFixtures(t)

	email := "fake2@influxdata.com"
	nodeID := "meow"

	instanceID, _ := uuid.NewV4()
	pathAndQuery := fmt.Sprintf("/licenses?email=%s&node-id=%s&instance-id=%s", email, nodeID, instanceID)
	targetUrl := fmt.Sprintf("%s%s", testFixtures.srv.URL, pathAndQuery)
	expectedLocationHeader := []string{fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(email), instanceID)}

	// first attempt leads to license creation in database and a '201 Created' response
	res, err := http.Post(targetUrl, "", nil)
	require.NoError(t, err)
	require.Equal(t, 201, res.StatusCode)
	header, ok := res.Header["Location"]
	require.True(t, ok, "must have Location header in every 2xx POST response")
	require.Equal(t, expectedLocationHeader, header)

	instanceID, _ = uuid.NewV4()
	pathAndQuery = fmt.Sprintf("/licenses?email=%s&node-id=%s&instance-id=%s", email, nodeID, instanceID)
	targetUrl = fmt.Sprintf("%s%s", testFixtures.srv.URL, pathAndQuery)
	expectedLocationHeader = []string{fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(email), instanceID)}

	// second should also result in a '201 Created' response
	res, err = http.Post(targetUrl, "", nil)
	require.NoError(t, err)
	require.Equal(t, 201, res.StatusCode)
	header, ok = res.Header["Location"]
	require.True(t, ok, "must have Location header in every 2xx POST response")
	require.Equal(t, expectedLocationHeader, header)
}

// this test covers the case where the same user (ie same email address)
// attempts to create a license for two different instances at the same time
// (verification after first attempt but before second)
func TestLicenseCreateSameEmailDifferentInstancesWithVerification(t *testing.T) {
	testFixtures := getTestFixtures(t)

	email := "fake2@influxdata.com"
	nodeID := "meow"

	instanceID, _ := uuid.NewV4()
	pathAndQuery := fmt.Sprintf("/licenses?email=%s&node-id=%s&instance-id=%s", email, nodeID, instanceID)
	targetUrl := fmt.Sprintf("%s%s", testFixtures.srv.URL, pathAndQuery)
	expectedLocationHeader := []string{fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(email), instanceID)}

	// first attempt leads to license creation in database and a '201 Created' response
	res, err := http.Post(targetUrl, "", nil)
	require.NoError(t, err)
	require.Equal(t, 201, res.StatusCode)
	header, ok := res.Header["Location"]
	require.True(t, ok, "must have Location header in every 2xx POST response")
	require.Equal(t, expectedLocationHeader, header)

	t.Logf("verifying user")
	ctx := context.Background()
	tx, err := testFixtures.store.BeginTx(ctx)
	require.NoError(t, err)

	dbEmail, err := url.QueryUnescape(email)
	require.NoError(t, err)

	user, err := testFixtures.store.GetUserByEmailTx(ctx, tx, dbEmail)
	require.NoError(t, err)

	err = testFixtures.store.MarkUserAsVerified(ctx, tx, user.ID)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	instanceID, _ = uuid.NewV4()
	pathAndQuery = fmt.Sprintf("/licenses?email=%s&node-id=%s&instance-id=%s", email, nodeID, instanceID)
	targetUrl = fmt.Sprintf("%s%s", testFixtures.srv.URL, pathAndQuery)
	expectedLocationHeader = []string{fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(email), instanceID)}

	// second should result in a '201 Created' response
	res, err = http.Post(targetUrl, "", nil)
	require.NoError(t, err)
	require.Equal(t, 201, res.StatusCode)
	header, ok = res.Header["Location"]
	require.True(t, ok, "must have Location header in every 2xx POST response")
	require.Equal(t, expectedLocationHeader, header)
}
