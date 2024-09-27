package httpd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/golang/snappy"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/mock"
	"github.com/influxdata/influxdb/flux/client"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/prometheus/prometheus/prompb"
)

// Ensure the handler returns results from a query (including nil results).
func TestHandler_Query(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		if stmt.String() != `SELECT * FROM bar` {
			t.Fatalf("unexpected query: %s", stmt.String())
		} else if ctx.Database != `foo` {
			t.Fatalf("unexpected db: %s", ctx.Database)
		}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})}
		ctx.Results <- &query.Result{StatementID: 2, Series: models.Rows([]*models.Row{{Name: "series1"}})}
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series0"}]},{"statement_id":2,"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler returns results from a query passed as a file.
func TestHandler_Query_File(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		if stmt.String() != `SELECT * FROM bar` {
			t.Fatalf("unexpected query: %s", stmt.String())
		} else if ctx.Database != `foo` {
			t.Fatalf("unexpected db: %s", ctx.Database)
		}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})}
		ctx.Results <- &query.Result{StatementID: 2, Series: models.Rows([]*models.Row{{Name: "series1"}})}
		return nil
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("q", "")
	if err != nil {
		t.Fatal(err)
	}
	io.WriteString(part, "SELECT * FROM bar")

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	r := MustNewJSONRequest("POST", "/query?db=foo", &body)
	r.Header.Set("Content-Type", writer.FormDataContentType())

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series0"}]},{"statement_id":2,"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Test query with user authentication.
func TestHandler_Query_Auth(t *testing.T) {
	// Create the handler to be tested.
	h := NewHandler(true)

	// Set mock meta client functions for the handler to use.
	h.MetaClient.AdminUserExistsFn = func() bool { return true }

	h.MetaClient.UserFn = func(username string) (meta.User, error) {
		if username != "user1" {
			return nil, meta.ErrUserNotFound
		}
		return &meta.UserInfo{
			Name:  "user1",
			Hash:  "abcd",
			Admin: true,
		}, nil
	}

	h.MetaClient.AuthenticateFn = func(u, p string) (meta.User, error) {
		if u != "user1" {
			return nil, fmt.Errorf("unexpected user: exp: user1, got: %s", u)
		} else if p != "abcd" {
			return nil, fmt.Errorf("unexpected password: exp: abcd, got: %s", p)
		}
		return h.MetaClient.User(u)
	}

	// Set mock query authorizer for handler to use.
	h.QueryAuthorizer.AuthorizeQueryFn = func(u meta.User, query *influxql.Query, database string) error {
		return nil
	}

	// Set mock statement executor for handler to use.
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		if stmt.String() != `SELECT * FROM bar` {
			t.Fatalf("unexpected query: %s", stmt.String())
		} else if ctx.Database != `foo` {
			t.Fatalf("unexpected db: %s", ctx.Database)
		}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})}
		ctx.Results <- &query.Result{StatementID: 2, Series: models.Rows([]*models.Row{{Name: "series1"}})}
		return nil
	}

	// Test the handler with valid user and password in the URL parameters.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?u=user1&p=abcd&db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series0"}]},{"statement_id":2,"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}

	// Test the handler with valid user and password using basic auth.
	w = httptest.NewRecorder()
	r := MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil)
	r.SetBasicAuth("user1", "abcd")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series0"}]},{"statement_id":2,"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}

	// Test the handler with valid JWT bearer token.
	req := MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil)
	// Create a signed JWT token string and add it to the request header.
	_, signedToken := MustJWTToken("user1", h.Config.SharedSecret, false)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", signedToken))

	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series0"}]},{"statement_id":2,"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}

	// Test the handler with JWT token signed with invalid key.
	req = MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil)
	// Create a signed JWT token string and add it to the request header.
	_, signedToken = MustJWTToken("user1", "invalid key", false)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", signedToken))

	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"error":"signature is invalid"}` {
		t.Fatalf("unexpected body: %s", body)
	}

	// Test handler with valid JWT token carrying non-existent user.
	_, signedToken = MustJWTToken("bad_user", h.Config.SharedSecret, false)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", signedToken))

	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"error":"user not found"}` {
		t.Fatalf("unexpected body: %s", body)
	}

	// Test handler with expired JWT token.
	_, signedToken = MustJWTToken("user1", h.Config.SharedSecret, true)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", signedToken))

	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if !strings.Contains(w.Body.String(), `{"error":"Token is expired`) {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}

	// Test handler with JWT token that has no expiration set.
	token, _ := MustJWTToken("user1", h.Config.SharedSecret, false)
	delete(token.Claims.(jwt.MapClaims), "exp")
	signedToken, err := token.SignedString([]byte(h.Config.SharedSecret))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", signedToken))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"error":"token expiration required"}` {
		t.Fatalf("unexpected body: %s", body)
	}

	// Test that auth fails if shared secret is blank.
	origSecret := h.Config.SharedSecret
	h.Config.SharedSecret = ""
	token, _ = MustJWTToken("user1", h.Config.SharedSecret, false)
	signedToken, err = token.SignedString([]byte(h.Config.SharedSecret))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", signedToken))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"error":"bearer auth disabled"}` {
		t.Fatalf("unexpected body: %s", body)
	}
	h.Config.SharedSecret = origSecret

	// Test the handler with valid user and password in the url and invalid in
	// basic auth (prioritize url).
	w = httptest.NewRecorder()
	r = MustNewJSONRequest("GET", "/query?u=user1&p=abcd&db=foo&q=SELECT+*+FROM+bar", nil)
	r.SetBasicAuth("user1", "efgh")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", w.Code, w.Body.String())
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series0"}]},{"statement_id":2,"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler returns results from a query (including nil results).
func TestHandler_QueryRegex(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		if stmt.String() != `SELECT * FROM test WHERE url =~ /http\:\/\/www.akamai\.com/` {
			t.Fatalf("unexpected query: %s", stmt.String())
		} else if ctx.Database != `test` {
			t.Fatalf("unexpected db: %s", ctx.Database)
		}
		ctx.Results <- nil
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("GET", "/query?db=test&q=SELECT%20%2A%20FROM%20test%20WHERE%20url%20%3D~%20%2Fhttp%5C%3A%5C%2F%5C%2Fwww.akamai%5C.com%2F", nil))
}

// Ensure the handler merges results from the same statement.
func TestHandler_Query_MergeResults(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series1"}})}
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series0"},{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler merges results from the same statement.
func TestHandler_Query_MergeEmptyResults(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows{}}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series1"}})}
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler merges series from the same result.
func TestHandler_Query_MergeSeries(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{
			{
				Name: "series0",
				Values: [][]interface{}{
					{float64(2.0)},
				},
				Partial: true,
			},
		})}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{
			{
				Name: "series0",
				Values: [][]interface{}{
					{float64(3.0)},
				},
			},
		})}
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":1,"series":[{"name":"series0","values":[[2],[3]]}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can parse chunked and chunk size query parameters.
func TestHandler_Query_Chunked(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		if ctx.ChunkSize != 2 {
			t.Fatalf("unexpected chunk size: %d", ctx.ChunkSize)
		}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series1"}})}
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar&chunked=true&chunk_size=2", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"statement_id":1,"series":[{"name":"series0"}]}]}
{"results":[{"statement_id":1,"series":[{"name":"series1"}]}]}
` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler can accept an async query.
func TestHandler_Query_Async(t *testing.T) {
	done := make(chan struct{})
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		if stmt.String() != `SELECT * FROM bar` {
			t.Fatalf("unexpected query: %s", stmt.String())
		} else if ctx.Database != `foo` {
			t.Fatalf("unexpected db: %s", ctx.Database)
		}
		ctx.Results <- &query.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})}
		ctx.Results <- &query.Result{StatementID: 2, Series: models.Rows([]*models.Row{{Name: "series1"}})}
		close(done)
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar&async=true", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `` {
		t.Fatalf("unexpected body: %s", body)
	}

	// Wait to make sure the async query runs and completes.
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
		t.Fatal("timeout while waiting for async query to complete")
	case <-done:
	}
}

// Ensure the handler returns a status 400 if the query is not passed in.
func TestHandler_Query_ErrQueryRequired(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"error":"missing required parameter \"q\""}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler returns a status 400 if the query cannot be parsed.
func TestHandler_Query_ErrInvalidQuery(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?q=SELECT", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"error":"error parsing query: found EOF, expected identifier, string, number, bool at line 1, char 8"}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler returns an appropriate 401 or 403 status when authentication or authorization fails.
func TestHandler_Query_ErrAuthorize(t *testing.T) {
	h := NewHandler(true)
	h.QueryAuthorizer.AuthorizeQueryFn = func(u meta.User, q *influxql.Query, db string) error {
		return errors.New("marker")
	}
	h.MetaClient.AdminUserExistsFn = func() bool { return true }
	h.MetaClient.AuthenticateFn = func(u, p string) (meta.User, error) {

		users := []meta.UserInfo{
			{
				Name:  "admin",
				Hash:  "admin",
				Admin: true,
			},
			{
				Name: "user1",
				Hash: "abcd",
				Privileges: map[string]influxql.Privilege{
					"db0": influxql.ReadPrivilege,
				},
			},
		}

		for _, user := range users {
			if u == user.Name {
				if p == user.Hash {
					return &user, nil
				}
				return nil, meta.ErrAuthenticate
			}
		}
		return nil, meta.ErrUserNotFound
	}

	for i, tt := range []struct {
		user     string
		password string
		query    string
		code     int
	}{
		{
			query: "/query?q=SHOW+DATABASES",
			code:  http.StatusUnauthorized,
		},
		{
			user:     "user1",
			password: "abcd",
			query:    "/query?q=SHOW+DATABASES",
			code:     http.StatusForbidden,
		},
		{
			user:     "user2",
			password: "abcd",
			query:    "/query?q=SHOW+DATABASES",
			code:     http.StatusUnauthorized,
		},
	} {
		w := httptest.NewRecorder()
		r := MustNewJSONRequest("GET", tt.query, nil)
		params := r.URL.Query()
		if tt.user != "" {
			params.Set("u", tt.user)
		}
		if tt.password != "" {
			params.Set("p", tt.password)
		}
		r.URL.RawQuery = params.Encode()

		h.ServeHTTP(w, r)
		if w.Code != tt.code {
			t.Errorf("%d. unexpected status: got=%d exp=%d\noutput: %s", i, w.Code, tt.code, w.Body.String())
		}
	}
}

// Ensure the handler returns a status 200 if an error is returned in the result.
func TestHandler_Query_ErrResult(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		return errors.New("measurement not found")
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SHOW+SERIES+from+bin", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if body := strings.TrimSpace(w.Body.String()); body != `{"results":[{"statement_id":0,"error":"measurement not found"}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure that closing the HTTP connection causes the query to be interrupted.
func TestHandler_Query_CloseNotify(t *testing.T) {
	// Avoid leaking a goroutine when this fails.
	done := make(chan struct{})
	defer close(done)

	interrupted := make(chan struct{})
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		select {
		case <-ctx.Done():
		case <-done:
		}
		close(interrupted)
		return nil
	}

	s := httptest.NewServer(h)
	defer s.Close()

	// Parse the URL and generate a query request.
	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/query"

	values := url.Values{}
	values.Set("q", "SELECT * FROM cpu")
	values.Set("db", "db0")
	values.Set("rp", "rp0")
	values.Set("chunked", "true")
	u.RawQuery = values.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Perform the request and retrieve the response.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	// Validate that the interrupted channel has NOT been closed yet.
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-interrupted:
		timer.Stop()
		t.Fatal("query interrupted unexpectedly")
	case <-timer.C:
	}

	// Close the response body which should abort the query in the handler.
	resp.Body.Close()

	// The query should abort within 100 milliseconds.
	timer.Reset(100 * time.Millisecond)
	select {
	case <-interrupted:
		timer.Stop()
	case <-timer.C:
		t.Fatal("timeout while waiting for query to abort")
	}
}

// Ensure the handler returns an appropriate 401 status when authentication
// fails on ping endpoints.
func TestHandler_Ping_ErrAuthorize(t *testing.T) {
	h := NewHandlerWithConfig(NewHandlerConfig(WithAuthentication(), WithPingAuthEnabled()))
	h.MetaClient.AdminUserExistsFn = func() bool { return true }
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}
	h.MetaClient.AuthenticateFn = func(u, p string) (meta.User, error) {
		users := []meta.UserInfo{
			{
				Name:  "admin",
				Hash:  "admin",
				Admin: true,
			},
			{
				Name: "user1",
				Hash: "abcd",
				Privileges: map[string]influxql.Privilege{
					"db0": influxql.ReadPrivilege,
				},
			},
		}

		for _, user := range users {
			if u == user.Name {
				if p == user.Hash {
					return &user, nil
				}
				return nil, meta.ErrAuthenticate
			}
		}
		return nil, meta.ErrUserNotFound
	}

	for i, tt := range []struct {
		user     string
		password string
		query    string
		code     int
	}{
		{
			query: "/ping",
			code:  http.StatusUnauthorized,
		},
		{
			user:     "user1",
			password: "abcd",
			query:    "/ping",
			code:     http.StatusNoContent,
		},
		{
			user:     "user2",
			password: "abcd",
			query:    "/ping",
			code:     http.StatusUnauthorized,
		},
	} {
		w := httptest.NewRecorder()
		r := MustNewJSONRequest("GET", tt.query, nil)
		params := r.URL.Query()
		if tt.user != "" {
			params.Set("u", tt.user)
		}
		if tt.password != "" {
			params.Set("p", tt.password)
		}
		r.URL.RawQuery = params.Encode()

		h.ServeHTTP(w, r)
		if w.Code != tt.code {
			t.Errorf("%d. unexpected status: got=%d exp=%d\noutput: %s", i, w.Code, tt.code, w.Body.String())
		}
	}
}

// Ensure the handler returns an appropriate 403 status when authentication or
// authorization fails on debug endpoints.
func TestHandler_Debug_ErrAuthorize(t *testing.T) {
	h := NewHandlerWithConfig(NewHandlerConfig(WithAuthentication(), WithPprofAuthEnabled()))
	h.MetaClient.AdminUserExistsFn = func() bool { return true }
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}
	h.MetaClient.AuthenticateFn = func(u, p string) (meta.User, error) {
		users := []meta.UserInfo{
			{
				Name:  "admin",
				Hash:  "admin",
				Admin: true,
			},
			{
				Name: "user1",
				Hash: "abcd",
				Privileges: map[string]influxql.Privilege{
					"db0": influxql.ReadPrivilege,
				},
			},
		}

		for _, user := range users {
			if u == user.Name {
				if p == user.Hash {
					return &user, nil
				}
				return nil, meta.ErrAuthenticate
			}
		}
		return nil, meta.ErrUserNotFound
	}

	for i, tt := range []struct {
		user     string
		password string
		query    string
		code     int
	}{
		{
			query: "/debug/vars",
			code:  http.StatusUnauthorized,
		},
		{
			user:     "user1",
			password: "abcd",
			query:    "/debug/vars",
			code:     http.StatusForbidden,
		},
		{
			user:     "user2",
			password: "abcd",
			query:    "/debug/vars",
			code:     http.StatusUnauthorized,
		},
	} {
		w := httptest.NewRecorder()
		r := MustNewJSONRequest("GET", tt.query, nil)
		params := r.URL.Query()
		if tt.user != "" {
			params.Set("u", tt.user)
		}
		if tt.password != "" {
			params.Set("p", tt.password)
		}
		r.URL.RawQuery = params.Encode()

		h.ServeHTTP(w, r)
		if w.Code != tt.code {
			t.Errorf("%d. unexpected status: got=%d exp=%d\noutput: %s", i, w.Code, tt.code, w.Body.String())
		}
	}
}

// Ensure the prometheus remote write works with valid values.
func TestHandler_PromWrite(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "host", Value: "a"},
					{Name: "region", Value: "west"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 1.2},
					{Timestamp: 3, Value: 14.5},
					{Timestamp: 6, Value: 222.99},
				},
			},
		},
	}

	data, err := req.Marshal()
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)

	b := bytes.NewReader(compressed)
	h := NewHandler(false)
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}

	var called bool
	h.PointsWriter.WritePointsFn = func(db, rp string, _ models.ConsistencyLevel, _ meta.User, points []models.Point) error {
		called = true

		if got, exp := len(points), 3; got != exp {
			t.Fatalf("got %d points, expected %d\n\npoints:\n%v", got, exp, points)
		}

		expFields := []models.Fields{
			models.Fields{"value": req.Timeseries[0].Samples[0].Value},
			models.Fields{"value": req.Timeseries[0].Samples[1].Value},
			models.Fields{"value": req.Timeseries[0].Samples[2].Value},
		}

		expTS := []int64{
			req.Timeseries[0].Samples[0].Timestamp * int64(time.Millisecond),
			req.Timeseries[0].Samples[1].Timestamp * int64(time.Millisecond),
			req.Timeseries[0].Samples[2].Timestamp * int64(time.Millisecond),
		}

		for i, point := range points {
			if got, exp := point.UnixNano(), expTS[i]; got != exp {
				t.Fatalf("got time %d, expected %d\npoint:\n%v", got, exp, point)
			}

			exp := models.Tags{models.Tag{Key: []byte("host"), Value: []byte("a")}, models.Tag{Key: []byte("region"), Value: []byte("west")}}
			if got := point.Tags(); !reflect.DeepEqual(got, exp) {
				t.Fatalf("got tags: %v, expected: %v\npoint:\n%v", got, exp, point)
			}

			gotFields, err := point.Fields()
			if err != nil {
				t.Fatal(err.Error())
			}

			if got, exp := gotFields, expFields[i]; !reflect.DeepEqual(got, exp) {
				t.Fatalf("got fields %v, expected %v\npoint:\n%v", got, exp, point)
			}
		}
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("POST", "/api/v1/prom/write?db=foo", b))
	if !called {
		t.Fatal("WritePoints: expected call")
	}

	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

// Ensure the prometheus remote write works with invalid values.
func TestHandler_PromWrite_Dropped(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "host", Value: "a"},
					{Name: "region", Value: "west"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 1.2},
					{Timestamp: 2, Value: math.NaN()},
					{Timestamp: 3, Value: 14.5},
					{Timestamp: 4, Value: math.Inf(-1)},
					{Timestamp: 5, Value: math.Inf(1)},
					{Timestamp: 6, Value: 222.99},
					{Timestamp: 7, Value: math.Inf(-1)},
					{Timestamp: 8, Value: math.Inf(1)},
					{Timestamp: 9, Value: math.Inf(1)},
				},
			},
		},
	}

	data, err := req.Marshal()
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)

	b := bytes.NewReader(compressed)
	h := NewHandler(false)
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}

	var called bool
	h.PointsWriter.WritePointsFn = func(db, rp string, _ models.ConsistencyLevel, _ meta.User, points []models.Point) error {
		called = true

		if got, exp := len(points), 3; got != exp {
			t.Fatalf("got %d points, expected %d\n\npoints:\n%v", got, exp, points)
		}

		expFields := []models.Fields{
			models.Fields{"value": req.Timeseries[0].Samples[0].Value},
			models.Fields{"value": req.Timeseries[0].Samples[2].Value},
			models.Fields{"value": req.Timeseries[0].Samples[5].Value},
		}

		expTS := []int64{
			req.Timeseries[0].Samples[0].Timestamp * int64(time.Millisecond),
			req.Timeseries[0].Samples[2].Timestamp * int64(time.Millisecond),
			req.Timeseries[0].Samples[5].Timestamp * int64(time.Millisecond),
		}

		for i, point := range points {
			if got, exp := point.UnixNano(), expTS[i]; got != exp {
				t.Fatalf("got time %d, expected %d\npoint:\n%v", got, exp, point)
			}

			exp := models.Tags{models.Tag{Key: []byte("host"), Value: []byte("a")}, models.Tag{Key: []byte("region"), Value: []byte("west")}}
			if got := point.Tags(); !reflect.DeepEqual(got, exp) {
				t.Fatalf("got tags: %v, expected: %v\npoint:\n%v", got, exp, point)
			}

			gotFields, err := point.Fields()
			if err != nil {
				t.Fatal(err.Error())
			}

			if got, exp := gotFields, expFields[i]; !reflect.DeepEqual(got, exp) {
				t.Fatalf("got fields %v, expected %v\npoint:\n%v", got, exp, point)
			}
		}
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("POST", "/api/v1/prom/write?db=foo", b))
	if !called {
		t.Fatal("WritePoints: expected call")
	}

	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

func mustMakeBigString(sz int) string {
	a := make([]byte, 0, sz)
	for i := 0; i < cap(a); i++ {
		a = append(a, 'a')
	}
	return string(a)
}

func TestHandler_PromWrite_Error(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				// Invalid tag key
				Labels:  []prompb.Label{{Name: mustMakeBigString(models.MaxKeyLength), Value: "a"}},
				Samples: []prompb.Sample{{Timestamp: 1, Value: 1.2}},
			},
		},
	}

	data, err := req.Marshal()
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)

	b := bytes.NewReader(compressed)
	h := NewHandler(false)
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}

	var called bool
	h.PointsWriter.WritePointsFn = func(db, rp string, _ models.ConsistencyLevel, _ meta.User, points []models.Point) error {
		called = true
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("POST", "/api/v1/prom/write?db=foo", b))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	if got, exp := strings.TrimSpace(w.Body.String()), `{"error":"max key length exceeded: 65572 \u003e 65535"}`; got != exp {
		t.Fatalf("got error %q, expected %q", got, exp)
	}

	if called {
		t.Fatal("WritePoints called but should not be")
	}
}

// Ensure Prometheus remote read requests are converted to the correct InfluxQL query and
// data is returned
func TestHandler_PromRead(t *testing.T) {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "value",
				},
			},
			StartTimestampMs: 1,
			EndTimestampMs:   2,
		}},
	}
	data, err := req.Marshal()
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)
	h := NewHandler(false)
	w := httptest.NewRecorder()

	// Number of results in the result set
	var i int64
	h.Store.ResultSet.NextFn = func() bool {
		i++
		return i <= 2
	}

	// data for each cursor.
	h.Store.ResultSet.CursorFn = func() tsdb.Cursor {
		cursor := internal.NewFloatArrayCursorMock()

		var i int64
		cursor.NextFn = func() *tsdb.FloatArray {
			i++
			ts := []int64{22000000 * i, 10000000000 * i}
			vs := []float64{2.3, 2992.33}
			if i > 2 {
				ts, vs = nil, nil
			}
			return &tsdb.FloatArray{Timestamps: ts, Values: vs}
		}

		return cursor
	}

	// Tags for each cursor.
	h.Store.ResultSet.TagsFn = func() models.Tags {
		return models.NewTags(map[string]string{
			"host":         fmt.Sprintf("server-%d", i),
			"_measurement": "mem",
		})
	}

	h.ServeHTTP(w, MustNewRequest("POST", "/api/v1/prom/read?db=foo&rp=bar", b))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	reqBuf, err := snappy.Decode(nil, w.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	var resp prompb.ReadResponse
	if err := resp.Unmarshal(reqBuf); err != nil {
		t.Fatal(err)
	}

	expResults := []*prompb.QueryResult{
		{
			Timeseries: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "host", Value: "server-1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 22, Value: 2.3},
						{Timestamp: 10000, Value: 2992.33},
						{Timestamp: 44, Value: 2.3},
						{Timestamp: 20000, Value: 2992.33},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "host", Value: "server-2"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 22, Value: 2.3},
						{Timestamp: 10000, Value: 2992.33},
						{Timestamp: 44, Value: 2.3},
						{Timestamp: 20000, Value: 2992.33},
					},
				},
			},
		},
	}

	if !reflect.DeepEqual(resp.Results, expResults) {
		t.Fatalf("Results differ:\n%v", cmp.Diff(resp.Results, expResults))
	}
}

func TestHandler_PromRead_NoResults(t *testing.T) {
	req := &prompb.ReadRequest{Queries: []*prompb.Query{&prompb.Query{
		Matchers: []*prompb.LabelMatcher{
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "value",
			},
		},
		StartTimestampMs: 0,
		EndTimestampMs:   models.MaxNanoTime / int64(time.Millisecond),
	}}}
	data, err := req.Marshal()
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)
	h := NewHandler(false)
	w := httptest.NewRecorder()

	b := bytes.NewReader(compressed)
	h.ServeHTTP(w, MustNewJSONRequest("POST", "/api/v1/prom/read?db=foo", b))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	reqBuf, err := snappy.Decode(nil, w.Body.Bytes())
	if err != nil {
		t.Fatal(err.Error())
	}

	var resp prompb.ReadResponse
	if err := resp.Unmarshal(reqBuf); err != nil {
		t.Fatal(err.Error())
	}
}

func TestHandler_PromRead_UnsupportedCursors(t *testing.T) {
	req := &prompb.ReadRequest{Queries: []*prompb.Query{&prompb.Query{
		Matchers: []*prompb.LabelMatcher{
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "value",
			},
		},
		StartTimestampMs: 0,
		EndTimestampMs:   models.MaxNanoTime / int64(time.Millisecond),
	}}}
	data, err := req.Marshal()
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)

	unsupported := []tsdb.Cursor{
		internal.NewIntegerArrayCursorMock(),
		internal.NewBooleanArrayCursorMock(),
		internal.NewUnsignedArrayCursorMock(),
		internal.NewStringArrayCursorMock(),
	}

	for _, cursor := range unsupported {
		h := NewHandler(false)
		w := httptest.NewRecorder()
		var lb bytes.Buffer
		h.Logger = logger.New(&lb)

		more := true
		h.Store.ResultSet.NextFn = func() bool { defer func() { more = false }(); return more }

		// Set the cursor type that will be returned while iterating over
		// the mock store.
		h.Store.ResultSet.CursorFn = func() tsdb.Cursor {
			return cursor
		}

		b := bytes.NewReader(compressed)
		h.ServeHTTP(w, MustNewJSONRequest("POST", "/api/v1/prom/read?db=foo", b))
		if w.Code != http.StatusOK {
			t.Fatalf("unexpected status: %d", w.Code)
		}
		reqBuf, err := snappy.Decode(nil, w.Body.Bytes())
		if err != nil {
			t.Fatal(err.Error())
		}

		var resp prompb.ReadResponse
		if err := resp.Unmarshal(reqBuf); err != nil {
			t.Fatal(err.Error())
		}

		if !strings.Contains(lb.String(), "cursor_type=") {
			t.Fatalf("got log message %q, expected to contain \"cursor_type\"", lb.String())
		}
	}
}

func TestHandler_Flux_DisabledByDefault(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()

	body := bytes.NewBufferString(`from(bucket:"db/rp") |> range(start:-1h) |> last()`)
	h.ServeHTTP(w, MustNewRequest("POST", "/api/v2/query", body))
	if got := w.Code; !cmp.Equal(got, http.StatusForbidden) {
		t.Fatalf("unexpected status: %d", got)
	}

	exp := `{"error":"Flux query service disabled. Verify flux-enabled=true in the [http] section of the InfluxDB config."}` + "\n"
	if got := w.Body.String(); got != exp {
		t.Fatalf("unexpected body -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestHandler_PromRead_NilResultSet(t *testing.T) {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "value",
				},
			},
			StartTimestampMs: 1,
			EndTimestampMs:   2,
		}},
	}
	data, err := req.Marshal()
	if err != nil {
		log.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)

	h := NewHandler(false)

	// Mocks the case when Store.Read() returns nil, nil
	h.Handler.Store.(*internal.StorageStoreMock).ReadFilterFn = func(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
		return nil, nil
	}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, MustNewRequest("POST", "/api/v1/prom/read?db=foo&rp=bar", b))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	if w.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("Got unexpected \"Content-Type\" header value:\n%v", cmp.Diff("application/x-protobuf", w.Header().Get("Content-Type")))
	}
	if w.Header().Get("Content-Encoding") != "snappy" {
		t.Fatalf("Got unexpected \"Content-Encoding\" header value:\n%v", cmp.Diff("snappy", w.Header().Get("Content-Encoding")))
	}

	decompressed, err := snappy.Decode(nil, w.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	resp := new(prompb.ReadResponse)
	err = resp.Unmarshal(decompressed)
	if err != nil {
		t.Fatal(err)
	}

	expected := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{{}},
	}
	if !reflect.DeepEqual(resp, expected) {
		t.Fatalf("Results differ:\n%v", cmp.Diff(expected, resp))
	}
}

func TestHandler_Flux_QueryJSON(t *testing.T) {
	h := NewHandlerWithConfig(NewHandlerConfig(WithFlux(), WithNoLog()))
	called := false
	qry := "foo"
	h.Controller.QueryFn = func(ctx context.Context, compiler flux.Compiler) (i flux.Query, e error) {
		if exp := flux.CompilerType(lang.FluxCompilerType); compiler.CompilerType() != exp {
			t.Fatalf("unexpected compiler type -got/+exp\n%s", cmp.Diff(compiler.CompilerType(), exp))
		}
		if c, ok := compiler.(lang.FluxCompiler); !ok {
			t.Fatal("expected lang.FluxCompiler")
		} else if exp := qry; c.Query != exp {
			t.Fatalf("unexpected query -got/+exp\n%s", cmp.Diff(c.Query, exp))
		}
		called = true

		p := &mock.Program{}
		return p.Start(ctx, nil)
	}

	q := client.QueryRequest{Query: qry}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(q); err != nil {
		t.Fatalf("unexpected JSON encoding error: %q", err.Error())
	}

	req := MustNewRequest("POST", "/api/v2/query", &body)
	req.Header.Add("content-type", "application/json")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if got := w.Code; !cmp.Equal(got, http.StatusOK) {
		t.Fatalf("unexpected status: %d", got)
	}

	if !called {
		t.Fatalf("expected QueryFn to be called")
	}
}

func TestHandler_Flux_QueryText(t *testing.T) {
	h := NewHandlerWithConfig(NewHandlerConfig(WithFlux(), WithNoLog()))
	called := false
	qry := "bar"
	h.Controller.QueryFn = func(ctx context.Context, compiler flux.Compiler) (i flux.Query, e error) {
		if exp := flux.CompilerType(lang.FluxCompilerType); compiler.CompilerType() != exp {
			t.Fatalf("unexpected compiler type -got/+exp\n%s", cmp.Diff(compiler.CompilerType(), exp))
		}
		if c, ok := compiler.(lang.FluxCompiler); !ok {
			t.Fatal("expected lang.FluxCompiler")
		} else if exp := qry; c.Query != exp {
			t.Fatalf("unexpected query -got/+exp\n%s", cmp.Diff(c.Query, exp))
		}
		called = true

		p := &mock.Program{}
		return p.Start(ctx, nil)
	}

	req := MustNewRequest("POST", "/api/v2/query", bytes.NewBufferString(qry))
	req.Header.Add("content-type", "application/vnd.flux")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if got := w.Code; !cmp.Equal(got, http.StatusOK) {
		t.Fatalf("unexpected status: %d", got)
	}

	if !called {
		t.Fatalf("expected QueryFn to be called")
	}
}

func TestHandler_Flux(t *testing.T) {

	queryBytes := func(qs string) io.Reader {
		var b bytes.Buffer
		q := &client.QueryRequest{Query: qs}
		if err := json.NewEncoder(&b).Encode(q); err != nil {
			t.Fatalf("unexpected JSON encoding error: %q", err.Error())
		}
		return &b
	}

	tests := []struct {
		name    string
		reqFn   func() *http.Request
		expCode int
		expBody string
	}{
		{
			name: "no media type",
			reqFn: func() *http.Request {
				return MustNewRequest("POST", "/api/v2/query", nil)
			},
			expCode: http.StatusBadRequest,
			expBody: "{\"error\":\"mime: no media type\"}\n",
		},
		{
			name: "200 OK",
			reqFn: func() *http.Request {
				req := MustNewRequest("POST", "/api/v2/query", queryBytes("foo"))
				req.Header.Add("content-type", "application/json")
				return req
			},
			expCode: http.StatusOK,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := NewHandlerWithConfig(NewHandlerConfig(WithFlux(), WithNoLog()))
			h.Controller.QueryFn = func(ctx context.Context, compiler flux.Compiler) (i flux.Query, e error) {
				p := &mock.Program{}
				return p.Start(ctx, nil)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, test.reqFn())
			if got := w.Code; !cmp.Equal(got, test.expCode) {
				t.Fatalf("unexpected status: %d", got)
			}

			if test.expBody != "" {
				if got := w.Body.String(); got != test.expBody {
					t.Fatalf("unexpected body -got/+exp\n%s", cmp.Diff(got, test.expBody))
				}
			}
		})
	}
}

func TestHandler_Flux_Auth(t *testing.T) {
	// Create the handler to be tested.
	h := NewHandlerWithConfig(NewHandlerConfig(WithFlux(), WithNoLog(), WithAuthentication()))
	h.MetaClient.AdminUserExistsFn = func() bool { return true }
	h.MetaClient.UserFn = func(username string) (meta.User, error) {
		if username != "user1" {
			return nil, meta.ErrUserNotFound
		}
		return &meta.UserInfo{
			Name:  "user1",
			Hash:  "abcd",
			Admin: true,
		}, nil
	}
	h.MetaClient.AuthenticateFn = func(u, p string) (meta.User, error) {
		if u != "user1" {
			return nil, fmt.Errorf("unexpected user: exp: user1, got: %s", u)
		} else if p != "abcd" {
			return nil, fmt.Errorf("unexpected password: exp: abcd, got: %s", p)
		}
		return h.MetaClient.User(u)
	}

	h.Controller.QueryFn = func(ctx context.Context, compiler flux.Compiler) (i flux.Query, e error) {
		p := &mock.Program{}
		return p.Start(ctx, nil)
	}

	req := MustNewRequest("POST", "/api/v2/query", bytes.NewBufferString("bar"))
	req.Header.Set("content-type", "application/vnd.flux")
	req.Header.Set("Authorization", "Token user1:abcd")
	// Test the handler with valid user and password in the URL parameters.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if got := w.Code; !cmp.Equal(got, http.StatusOK) {
		t.Fatalf("unexpected status: %d", got)
	}

	req.Header.Set("Authorization", "Token user1:efgh")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if got := w.Code; !cmp.Equal(got, http.StatusUnauthorized) {
		t.Fatalf("unexpected status: %d", got)
	}
}

// Ensure the handler handles ping requests correctly.
func TestHandler_Ping(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("GET", "/ping", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	h.ServeHTTP(w, MustNewRequest("HEAD", "/ping", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

// Ensure the handler handles health requests correctly.
func TestHandler_Health(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("GET", "/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, got["name"], "influxdb", "invalid name")
	assert.Equal(t, got["message"], "ready for queries and writes", "invalid message")
	assert.Equal(t, got["status"], "pass", "invalid status")
	assert.Equal(t, got["version"], "0.0.0", "invalid version")
	if _, present := got["checks"]; !present {
		t.Fatal("missing checks")
	}
}

// Ensure the handler returns the version correctly from the different endpoints.
func TestHandler_Version(t *testing.T) {
	h := NewHandler(false)
	h.StatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement, ctx *query.ExecutionContext) error {
		return nil
	}
	tests := []struct {
		method   string
		endpoint string
		body     io.Reader
	}{
		{
			method:   "GET",
			endpoint: "/ping",
			body:     nil,
		},
		{
			method:   "GET",
			endpoint: "/query?db=foo&q=SELECT+*+FROM+bar",
			body:     nil,
		},
		{
			method:   "POST",
			endpoint: "/write",
			body:     bytes.NewReader(make([]byte, 10)),
		},
		{
			method:   "GET",
			endpoint: "/notfound",
			body:     nil,
		},
	}

	for _, test := range tests {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, MustNewRequest(test.method, test.endpoint, test.body))
		if v := w.HeaderMap["X-Influxdb-Version"]; len(v) > 0 {
			if v[0] != "0.0.0" {
				t.Fatalf("unexpected version: %s", v)
			}
		} else {
			t.Fatalf("Header entry 'X-Influxdb-Version' not present")
		}

		if v := w.HeaderMap["X-Influxdb-Build"]; len(v) > 0 {
			if v[0] != "OSS" {
				t.Fatalf("unexpected BuildType: %s", v)
			}
		} else {
			t.Fatalf("Header entry 'X-Influxdb-Build' not present")
		}
	}
}

// Ensure the handler handles status requests correctly.
func TestHandler_Status(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("GET", "/status", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	h.ServeHTTP(w, MustNewRequest("HEAD", "/status", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

// Ensure write endpoint can handle bad requests
func TestHandler_HandleBadRequestBody(t *testing.T) {
	b := bytes.NewReader(make([]byte, 10))
	h := NewHandler(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("POST", "/write", b))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

func TestHandler_Write_EntityTooLarge_ContentLength(t *testing.T) {
	b := bytes.NewReader(make([]byte, 100))
	h := NewHandler(false)
	h.Config.MaxBodySize = 5
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("POST", "/write?db=foo", b))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

func TestHandler_Write_SuppressLog(t *testing.T) {
	var buf bytes.Buffer
	c := httpd.NewConfig()
	c.SuppressWriteLog = true
	h := NewHandlerWithConfig(c)
	h.CLFLogger = log.New(&buf, "", log.LstdFlags)
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}
	h.PointsWriter.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error {
		return nil
	}

	b := strings.NewReader("cpu,host=server01 value=2\n")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("POST", "/write?db=foo", b))
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	// If the log has anything in it, this failed.
	if buf.Len() > 0 {
		t.Fatalf("expected no bytes to be written to the log, got %d", buf.Len())
	}
}

// onlyReader implements io.Reader only to ensure Request.ContentLength is not set
type onlyReader struct {
	r io.Reader
}

func (o onlyReader) Read(p []byte) (n int, err error) {
	return o.r.Read(p)
}

func TestHandler_Write_EntityTooLarge_NoContentLength(t *testing.T) {
	b := onlyReader{bytes.NewReader(make([]byte, 100))}
	h := NewHandler(false)
	h.Config.MaxBodySize = 5
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("POST", "/write?db=foo", b))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

// TestHandler_Write_NegativeMaxBodySize verifies no error occurs if MaxBodySize is < 0
func TestHandler_Write_NegativeMaxBodySize(t *testing.T) {
	b := bytes.NewReader([]byte(`foo n=1`))
	h := NewHandler(false)
	h.Config.MaxBodySize = -1
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}
	called := false
	h.PointsWriter.WritePointsFn = func(_, _ string, _ models.ConsistencyLevel, _ meta.User, _ []models.Point) error {
		called = true
		return nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("POST", "/write?db=foo", b))
	if !called {
		t.Fatal("WritePoints: expected call")
	}
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

// TestHandler_Write_V1_Precision verifies v1 writes validate precision.
func TestHandler_Write_V1_Precision(t *testing.T) {
	h := NewHandler(false)
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}
	h.PointsWriter.WritePointsFn = func(_, _ string, _ models.ConsistencyLevel, _ meta.User, _ []models.Point) error {
		return nil
	}

	tests := []struct {
		url    string
		status int
	}{
		// Successful requests.
		{"/write?db=foo", http.StatusNoContent},
		{"/write?db=foo&precision=n", http.StatusNoContent},
		{"/write?db=foo&precision=u", http.StatusNoContent},
		{"/write?db=foo&precision=ms", http.StatusNoContent},
		{"/write?db=foo&precision=s", http.StatusNoContent},
		{"/write?db=foo&precision=m", http.StatusNoContent},
		{"/write?db=foo&precision=h", http.StatusNoContent},
		// Invalid requests.
		{"/write?db=foo&precision=us", http.StatusBadRequest},
	}

	runTest := func(url string, status int) {
		b := bytes.NewReader([]byte(`foo n=1`))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, MustNewRequest("POST", url, b))
		if w.Code != status {
			t.Fatalf("unexpected result for: \"%s\"\n\texp: %d, got: %d\n\t%s", url, status, w.Code, w.Body)
		}
	}

	for _, t := range tests {
		runTest(t.url, t.status)
	}
}

// TestHandler_Write_V2_Precision verifies v2 writes validate precision.
func TestHandler_Write_V2_Precision(t *testing.T) {
	h := NewHandler(false)
	h.MetaClient.DatabaseFn = func(name string) *meta.DatabaseInfo {
		return &meta.DatabaseInfo{}
	}
	h.PointsWriter.WritePointsFn = func(_, _ string, _ models.ConsistencyLevel, _ meta.User, _ []models.Point) error {
		return nil
	}

	tests := []struct {
		url    string
		status int
	}{
		// Successful requests.
		{"/api/v2/write?org=bar&bucket=foo", http.StatusNoContent},
		{"/api/v2/write?org=bar&bucket=foo&precision=ns", http.StatusNoContent},
		{"/api/v2/write?org=bar&bucket=foo&precision=us", http.StatusNoContent},
		{"/api/v2/write?org=bar&bucket=foo&precision=ms", http.StatusNoContent},
		{"/api/v2/write?org=bar&bucket=foo&precision=s", http.StatusNoContent},
		// Invalid requests.
		{"/api/v2/write?org=bar&bucket=foo&precision=n", http.StatusBadRequest},
		{"/api/v2/write?org=bar&bucket=foo&precision=u", http.StatusBadRequest},
		{"/api/v2/write?org=bar&bucket=foo&precision=m", http.StatusBadRequest},
		{"/api/v2/write?org=bar&bucket=foo&precision=h", http.StatusBadRequest},
	}

	runTest := func(url string, status int) {
		b := bytes.NewReader([]byte(`foo n=1`))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, MustNewRequest("POST", url, b))
		if w.Code != status {
			t.Fatalf("unexpected result for: \"%s\"\n\texp: %d, got: %d\n\t%s", url, status, w.Code, w.Body)
		}
	}

	for _, t := range tests {
		runTest(t.url, t.status)
	}
}

func TestHandler_Delete_V2(t *testing.T) {
	var errUnexpectedMeasurement = errors.New("unexpected measurement")
	type test struct {
		url    string
		body   httpd.DeleteBody
		status int
		errMsg string
	}
	tests := []*test{
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T22:56:06Z"},
			status: http.StatusOK,
			errMsg: ``,
		},
		&test{
			url:    "/api/v2/delete?/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T22:56:06Z"},
			status: http.StatusNotFound,
			errMsg: `delete - bucket: bucket name "" is missing a slash; not in "database/retention-policy" format`,
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T22:56:06Z", Predicate: "?>!!?>?>;;;"},
			status: http.StatusBadRequest,
			errMsg: `delete - cannot parse predicate "?>!!?>?>;;; AND time >= '2022-03-23T22:56:06Z' AND time < '2022-03-23T20:56:06Z'": found ?, expected identifier, string, number, bool at line 1, char 1`,
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T22:56:06Z", Predicate: "_measurement=\"mymeasure\" AND t1=tagOne"},
			status: http.StatusOK,
			errMsg: ``,
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T22:56:06Z"},
			status: http.StatusOK,
			errMsg: ``,
		},

		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Start: "2022-03-23T20:56:06Z"},
			status: http.StatusBadRequest,
			errMsg: "delete - stop field in RFC3339Nano format required",
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z"},
			status: http.StatusBadRequest,
			errMsg: "delete - start field in RFC3339Nano format required",
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Start: "2022-03-23T20:56:06Z", Stop: "NotAValidTime"},
			status: http.StatusBadRequest,
			errMsg: `delete - invalid format for stop field "NotAValidTime", please use RFC3339Nano: parsing time "NotAValidTime" as "2006-01-02T15:04:05.999999999Z07:00": cannot parse "NotAValidTime" as "2006"`,
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "NotAValidTime"},
			status: http.StatusBadRequest,
			errMsg: `delete - invalid format for start field "NotAValidTime", please use RFC3339Nano: parsing time "NotAValidTime" as "2006-01-02T15:04:05.999999999Z07:00": cannot parse "NotAValidTime" as "2006"`,
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T18:56:06Z", Predicate: `_measurement = "mymeasure" AND "tag0" = "value1"`},
			status: http.StatusOK,
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T18:56:06Z", Predicate: `_measurement = mymeasure AND "tag0" = "value1"`},
			status: http.StatusOK,
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T18:56:06Z", Predicate: `_measurement = "mymeasure" AND "tag0" = "value1" AND tag1 = value3`},
			status: http.StatusOK,
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T18:56:06Z", Predicate: `_measurement = "bad_measurement" AND "tag0" = "value1" AND tag1 = value3`},
			status: http.StatusBadRequest,
			errMsg: "delete - database: \"mydb\", retention policy: \"myrp\", start: \"2022-03-23T18:56:06Z\", stop: \"2022-03-23T20:56:06Z\", predicate: \"_measurement = \\\"bad_measurement\\\" AND \\\"tag0\\\" = \\\"value1\\\" AND tag1 = value3\", error: unexpected measurement",
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T18:56:06Z", Predicate: `_measurement = bad_measurement AND "tag0" = "value1" AND tag1 = value3`},
			status: http.StatusBadRequest,
			errMsg: "delete - database: \"mydb\", retention policy: \"myrp\", start: \"2022-03-23T18:56:06Z\", stop: \"2022-03-23T20:56:06Z\", predicate: \"_measurement = bad_measurement AND \\\"tag0\\\" = \\\"value1\\\" AND tag1 = value3\", error: unexpected measurement",
		},
		&test{
			url:    "/api/v2/delete?org=bar&bucket=mydb/myrp",
			body:   httpd.DeleteBody{Stop: "2022-03-23T20:56:06Z", Start: "2022-03-23T18:56:06Z", Predicate: `_measurement = "mymeasure" AND "tag0" != "value1" AND tag1 = value3`},
			status: http.StatusBadRequest,
			errMsg: `delete - predicate only supports equality operators and conjunctions. database: "mydb", retention policy: "myrp", start: "2022-03-23T18:56:06Z", stop: "2022-03-23T20:56:06Z", predicate: "_measurement = \"mymeasure\" AND \"tag0\" != \"value1\" AND tag1 = value3"`,
		},
	}

	h := NewHandler(false)
	h.Store.DeleteFn = func(database string, sources []influxql.Source, condition influxql.Expr) error {
		if len(sources) > 0 {
			if m, ok := sources[0].(*influxql.Measurement); ok && m.Name != "mymeasure" {
				return errUnexpectedMeasurement
			}
		}
		return nil
	}
	h.MetaClient = &internal.MetaClientMock{
		DatabaseFn: func(name string) *meta.DatabaseInfo {
			if name == "mydb" {
				return &meta.DatabaseInfo{
					Name:              "mydb",
					RetentionPolicies: []meta.RetentionPolicyInfo{meta.RetentionPolicyInfo{Name: "myrp"}},
				}
			} else {
				return nil
			}
		},
	}
	h.Handler.MetaClient = h.MetaClient

	var req *http.Request
	fn := func(ct *test) {
		w := httptest.NewRecorder()
		if body, err := json.Marshal(&ct.body); err != nil {
			t.Fatalf("error marshaling body: %s", err)
		} else {
			req = MustNewJSONRequest("POST", ct.url, bytes.NewReader(body))
		}
		h.ServeHTTP(w, req)
		var errMsg string
		if w.Code != ct.status {
			t.Fatalf("error, expected %d got %d: %s", ct.status, w.Code, errMsg)
		} else if w.Code != http.StatusOK {
			errMsg = w.Header().Get("X-InfluxDB-Error")
			if errMsg != ct.errMsg {
				t.Fatalf("incorrect error message, expected: %q, got: %q", ct.errMsg, errMsg)
			}
		}
	}
	for _, ct := range tests {
		fn(ct)
	}
}

func TestHandler_CreateDeleteBuckets(t *testing.T) {
	const existingDb = "mydb"
	const newDb = "newDb"
	const goodRp = "myrp"
	const postMethod = "POST"
	const deleteMethod = "DELETE"
	const patchMethod = "PATCH"

	type test struct {
		url    string
		method string
		body   httpd.BucketsBody
		status int
		errMsg string
	}

	tests := []*test{
		{
			url:    "/api/v2/buckets",
			method: postMethod,
			body: httpd.BucketsBody{
				BucketUpdate: httpd.BucketUpdate{
					Name: existingDb + "/",
					RetentionRules: []httpd.RetentionRule{
						httpd.RetentionRule{
							EverySeconds:              7200,
							ShardGroupDurationSeconds: 14400,
						},
					},
				},
				Rp:         "",
				SchemaType: "implicit",
			},
			status: http.StatusBadRequest,
			errMsg: `buckets - illegal bucket name: "mydb/"`,
		},
		{
			url:    "/api/v2/buckets",
			method: postMethod,
			body: httpd.BucketsBody{
				BucketUpdate: httpd.BucketUpdate{
					Name: existingDb + "//",
					RetentionRules: []httpd.RetentionRule{
						httpd.RetentionRule{
							EverySeconds:              7200,
							ShardGroupDurationSeconds: 14400,
						},
					},
				},
				Rp:         "",
				SchemaType: "implicit",
			},
			status: http.StatusBadRequest,
			errMsg: `buckets - retention policy "/": invalid name`,
		},
		{
			url:    "/api/v2/buckets/" + existingDb + "%2f" + goodRp,
			method: patchMethod,
			body: httpd.BucketsBody{
				BucketUpdate: httpd.BucketUpdate{
					Name: "newNewRp",
					RetentionRules: []httpd.RetentionRule{
						{
							EverySeconds:              6000,
							ShardGroupDurationSeconds: 18000,
						},
					},
				},
			},
			status: http.StatusOK,
		},
		{
			url:    "/api/v2/buckets/" + existingDb + "/",
			method: deleteMethod,
			status: http.StatusNotFound,
		},
		{
			url:    "/api/v2/buckets/baddb%2f" + goodRp,
			method: deleteMethod,
			status: http.StatusNotFound,
			errMsg: `delete bucket - not found: "baddb/myrp"`,
		},
		{
			url:    "/api/v2/buckets",
			method: postMethod,
			body: httpd.BucketsBody{
				BucketUpdate: httpd.BucketUpdate{
					Name: newDb + "/" + goodRp,
					RetentionRules: []httpd.RetentionRule{
						httpd.RetentionRule{
							EverySeconds:              7200,
							ShardGroupDurationSeconds: 14400,
						},
					},
				},
				Rp:         goodRp,
				SchemaType: "implicit",
			},
			status: http.StatusCreated,
		},
		{
			url:    "/api/v2/buckets/" + existingDb + "%2fbadrp",
			method: deleteMethod,
			status: http.StatusNotFound,
			errMsg: `delete bucket - not found: "mydb/badrp"`,
		},
		{
			url:    "/api/v2/buckets",
			method: postMethod,
			body: httpd.BucketsBody{
				BucketUpdate: httpd.BucketUpdate{
					Name: existingDb + "/" + goodRp,
					RetentionRules: []httpd.RetentionRule{
						httpd.RetentionRule{
							EverySeconds:              7200,
							ShardGroupDurationSeconds: 14400,
						},
					},
				},
				Rp:         goodRp,
				SchemaType: "implicit",
			},
			status: http.StatusCreated,
		},
		{
			url:    "/api/v2/buckets/" + existingDb + "%2f" + goodRp,
			method: deleteMethod,
			status: http.StatusOK,
		},
	}

	createRp := func(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error) {
		return &meta.RetentionPolicyInfo{
			Name:               spec.Name,
			ReplicaN:           *spec.ReplicaN,
			Duration:           *spec.Duration,
			ShardGroupDuration: spec.ShardGroupDuration,
		}, nil
	}

	lookupDb := func(name string) *meta.DatabaseInfo {
		if name == existingDb {
			return &meta.DatabaseInfo{
				Name:                   name,
				DefaultRetentionPolicy: goodRp,
				RetentionPolicies:      []meta.RetentionPolicyInfo{meta.RetentionPolicyInfo{Name: goodRp}},
			}
		} else {
			return nil
		}
	}

	dropDeleteRp := func(database, rp string) error {
		if dbi := lookupDb(database); dbi == nil {
			return fmt.Errorf("database not found: %q", database)
		} else if len(dbi.RetentionPolicies) <= 0 || dbi.RetentionPolicies[0].Name != rp {
			return fmt.Errorf("retention policy in database %q not found: %q", database, rp)
		} else {
			return nil
		}
	}

	updateRp := func(database string, name string, update *meta.RetentionPolicyUpdate, makeDefault bool) error {
		if database == existingDb && name == goodRp {
			return nil
		} else {
			return fmt.Errorf("bucket not found: %q", fmt.Sprintf("%s/%s", database, name))
		}
	}

	h := NewHandler(false)

	h.MetaClient = &internal.MetaClientMock{
		DatabaseFn:              lookupDb,
		CreateRetentionPolicyFn: createRp,
		CreateDatabaseWithRetentionPolicyFn: func(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
			rpi, err := createRp(name, spec, true)
			return &meta.DatabaseInfo{
				Name:                   name,
				DefaultRetentionPolicy: spec.Name,
				RetentionPolicies:      []meta.RetentionPolicyInfo{*rpi},
				ContinuousQueries:      nil,
			}, err
		},
		DropRetentionPolicyFn:   dropDeleteRp,
		UpdateRetentionPolicyFn: updateRp,
	}

	h.Store.DeleteRetentionPolicyFn = dropDeleteRp
	h.Handler.Store = h.Store
	h.Handler.MetaClient = h.MetaClient

	var req *http.Request
	fn := func(ct *test) {
		w := httptest.NewRecorder()
		if body, err := json.Marshal(&ct.body); err != nil {
			t.Fatalf("error marshaling body: %s", err)
		} else {
			req = MustNewJSONRequest(ct.method, ct.url, bytes.NewReader(body))
		}
		h.ServeHTTP(w, req)
		var errMsg string
		if w.Code != ct.status {
			t.Fatalf("error, expected %d got %d: %s", ct.status, w.Code, errMsg)
		} else if w.Code != http.StatusOK {
			errMsg = w.Header().Get("X-InfluxDB-Error")
			if errMsg != ct.errMsg {
				t.Fatalf("incorrect error message, expected: %q, got: %q", ct.errMsg, errMsg)
			}
		}
	}
	for _, ct := range tests {
		fn(ct)
	}
}

var testBuckets = []meta.DatabaseInfo{
	{
		Name:              "dbOne",
		RetentionPolicies: []meta.RetentionPolicyInfo{{Name: "rpOne_1"}, {Name: "rpTwo_1"}, {Name: "rpThree_1"}},
	},
	{
		Name:              "dbTwo",
		RetentionPolicies: []meta.RetentionPolicyInfo{{Name: "rpOne_2"}, {Name: "rpTwo_2"}, {Name: "rpThree_2"}, {Name: "rpFour_2"}},
	},
	{
		Name:              "dbThree",
		RetentionPolicies: []meta.RetentionPolicyInfo{{Name: "rpOne_3"}},
	},
}

func getBuckets(offset, limit int) []string {
	i := 0
	buckets := make([]string, 0, 8)

	for _, dbi := range testBuckets {
		for _, rpi := range dbi.RetentionPolicies {
			if limit <= (i - offset) {
				return buckets
			}
			if i >= offset {
				buckets = append(buckets, fmt.Sprintf("%s/%s", dbi.Name, rpi.Name))
			}
			i++
		}
	}
	return buckets
}

func TestHandler_ListBuckets(t *testing.T) {
	type test struct {
		url    string
		status int
		errMsg string
		skip   int
		limit  int
	}

	tests := []*test{
		{
			url:    "/api/v2/buckets?after=dbOne/rpTwo_1&limit=-1",
			status: http.StatusOK,
			skip:   2,
			limit:  1000000,
		},
		{
			url:    "/api/v2/buckets?offset=200",
			status: http.StatusOK,
			skip:   100,
			limit:  20,
		},
		{
			url:    "/api/v2/buckets?id=dbOne/rpTwo_1&name=NotThere/rpTwo_1",
			status: http.StatusBadRequest,
			skip:   1,
			limit:  1,
			errMsg: "list buckets: name: \"NotThere/rpTwo_1\" and id: \"dbOne/rpTwo_1\" do not match",
		},
		{
			url:    "/api/v2/buckets?after=dbOne/rpTwo_1&limit=4",
			status: http.StatusOK,
			skip:   2,
			limit:  4,
		},
		{
			url:    "/api/v2/buckets?after=dbOne/rpThree_1",
			status: http.StatusOK,
			skip:   3,
			limit:  20,
		},
		{
			url:    "/api/v2/buckets?after=dbTwo/rpTwo_2",
			status: http.StatusOK,
			skip:   5,
			limit:  20,
		},
		{
			url:    "/api/v2/buckets?id=dbOne/rpTwo_1&name=dbOne/rpTwo_1",
			status: http.StatusOK,
			skip:   1,
			limit:  1,
		},
		{
			url:    "/api/v2/buckets?id=dbOne/rpTwo_1",
			status: http.StatusOK,
			skip:   1,
			limit:  1,
		},
		{
			url:    "/api/v2/buckets?name=dbOne/rpTwo_1",
			status: http.StatusOK,
			skip:   1,
			limit:  1,
		},
		{
			url:    "/api/v2/buckets?offset=3&after=dbOne/rpOne_1",
			status: http.StatusBadRequest,
			skip:   0,
			limit:  20,
			errMsg: "list buckets cannot have both \"offset\" and \"after\" arguments",
		},
		{
			url:    "/api/v2/buckets?offset=3&limit=4",
			status: http.StatusOK,
			skip:   3,
			limit:  4,
		},
		{
			url:    "/api/v2/buckets?offset=1&limit=5",
			status: http.StatusOK,
			skip:   1,
			limit:  5,
		},
		{
			url:    "/api/v2/buckets",
			status: http.StatusOK,
			skip:   0,
			limit:  20,
		},
	}

	lookupDbFn := func(name string) *meta.DatabaseInfo {
		for i := 0; i < len(testBuckets); i++ {
			if testBuckets[i].Name == name {
				return &testBuckets[i]
			}
		}
		return nil
	}

	dbsFn := func() []meta.DatabaseInfo {
		return testBuckets
	}

	h := NewHandler(false)

	h.MetaClient = &internal.MetaClientMock{
		DatabaseFn:  lookupDbFn,
		DatabasesFn: dbsFn,
	}

	h.Handler.MetaClient = h.MetaClient

	var req *http.Request
	fn := func(ct *test) {
		w := httptest.NewRecorder()
		req = MustNewJSONRequest("GET", ct.url, nil)
		h.ServeHTTP(w, req)
		var errMsg string
		if w.Code != ct.status {
			t.Fatalf("error, expected %d got %d: %s", ct.status, w.Code, errMsg)
		} else if w.Code != http.StatusOK {
			errMsg = w.Header().Get("X-InfluxDB-Error")
			if errMsg != ct.errMsg {
				t.Fatalf("incorrect error message, expected: %q, got: %q", ct.errMsg, errMsg)
			}
		} else {
			var got httpd.Buckets
			exp := getBuckets(ct.skip, ct.limit)

			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("unmarshaling buckets: %s", err.Error())
			}
			if len(exp) != len(got.Buckets) {
				t.Fatalf("expected %d buckets returned, got %d", len(exp), len(got.Buckets))
			}
			for i := 0; i < len(got.Buckets); i++ {
				if exp[i] != got.Buckets[i].Name {
					t.Fatalf("expected %q, got %q", exp[i], got.Buckets[i].Name)
				}
			}
		}
	}
	for _, ct := range tests {
		fn(ct)
	}
}

func TestHandler_RetrieveBucket(t *testing.T) {
	type test struct {
		url    string
		status int
		errMsg string
		exp    string
	}

	tests := []*test{
		{
			url:    "/api/v2/buckets/dbOne//",
			status: http.StatusNotFound,
		},
		{
			url:    "/api/v2/buckets/%2frpTwo_1",
			status: http.StatusBadRequest,
			errMsg: `bucket "/rpTwo_1": bucket name "/rpTwo_1" is in db/rp form but has an empty database`,
		},
		{
			url:    "/api/v2/buckets/dbOne%2f",
			status: http.StatusBadRequest,
			errMsg: `bucket "dbOne/": illegal bucket id, empty retention policy`,
		},
		{
			url:    "/api/v2/buckets/dbFive%2frpTwo_1",
			status: http.StatusNotFound,
			errMsg: `bucket not found: "dbFive/rpTwo_1"`,
		},
		{
			url:    "/api/v2/buckets/dbOne%2frpTwo_1",
			status: http.StatusOK,
			exp:    "dbOne/rpTwo_1",
		},
		{
			url:    "/api/v2/buckets/dbOne%2frpOne_2",
			status: http.StatusNotFound,
			errMsg: `bucket not found: "dbOne/rpOne_2"`,
		},
	}
	lookupDbFn := func(name string) *meta.DatabaseInfo {
		for i := 0; i < len(testBuckets); i++ {
			if testBuckets[i].Name == name {
				return &testBuckets[i]
			}
		}
		return nil
	}

	h := NewHandler(false)

	h.MetaClient = &internal.MetaClientMock{
		DatabaseFn: lookupDbFn,
	}

	h.Handler.MetaClient = h.MetaClient

	var req *http.Request
	fn := func(ct *test) {
		w := httptest.NewRecorder()
		req = MustNewJSONRequest("GET", ct.url, nil)
		h.ServeHTTP(w, req)
		var errMsg string
		if w.Code != ct.status {
			t.Fatalf("error, test %s: expected %d got %d: %s", ct.url, ct.status, w.Code, errMsg)
		} else if w.Code != http.StatusOK {
			errMsg = w.Header().Get("X-InfluxDB-Error")
			if errMsg != ct.errMsg {
				t.Fatalf("incorrect error message, test %s: expected: %q, got: %q", ct.url, ct.errMsg, errMsg)
			}
		} else {
			var got httpd.Bucket
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("unmarshaling buckets: %s", err.Error())
			}
			if ct.exp != got.Name {
				t.Fatalf("expected %q, got %q", ct.exp, got.Name)
			}
		}
	}
	for _, ct := range tests {
		fn(ct)
	}
}

func TestHandler_UnsupportedV2API(t *testing.T) {
	type test struct {
		method string
		url    string
		status int
		errMsg string
	}
	tests := []*test{
		{
			method: "GET",
			url:    "/api/v2/buckets/mydb%2fmyrp/labels",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket labels not supported in this version"},
		{
			method: "POST",
			url:    "/api/v2/buckets/mydb%2fmyrp/labels",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket labels not supported in this version",
		},
		{
			method: "DELETE",
			url:    "/api/v2/buckets/mydb%2fmyrp/labels/mylabel",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket labels not supported in this version"},
		{
			method: "GET",
			url:    "/api/v2/buckets/mydb%2fmyrp/members",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket members not supported in this version",
		},
		{
			method: "POST",
			url:    "/api/v2/buckets/mydb%2fmyrp/members",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket members not supported in this version",
		},
		{
			method: "DELETE",
			url:    "/api/v2/buckets/mydb%2fmyrp/members/amember",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket members not supported in this version",
		},
		{
			method: "GET",
			url:    "/api/v2/buckets/mydb%2fmyrp/owners",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket owners not supported in this version",
		},
		{
			method: "POST",
			url:    "/api/v2/buckets/mydb%2fmyrp/owners",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket owners not supported in this version",
		},
		{
			method: "DELETE",
			url:    "/api/v2/buckets/mydb%2fmyrp/owners/anowner",
			status: http.StatusMethodNotAllowed,
			errMsg: "bucket owners not supported in this version",
		},
	}
	h := NewHandler(false)

	var req *http.Request
	fn := func(ct *test) {
		w := httptest.NewRecorder()
		req = MustNewJSONRequest(ct.method, ct.url, nil)
		h.ServeHTTP(w, req)
		var errMsg string
		if w.Code != ct.status {
			t.Fatalf("error, expected %d got %d: %s", ct.status, w.Code, errMsg)
		} else if w.Code != http.StatusOK {
			errMsg = w.Header().Get("X-InfluxDB-Error")
			if errMsg != ct.errMsg {
				t.Fatalf("incorrect error message, expected: %q, got: %q", ct.errMsg, errMsg)
			}
		}
	}
	for _, ct := range tests {
		fn(ct)
	}
}

// Ensure X-Forwarded-For header writes the correct log message.
func TestHandler_XForwardedFor(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(false)
	h.CLFLogger = log.New(&buf, "", 0)

	req := MustNewRequest("GET", "/query", nil)
	req.Header.Set("X-Forwarded-For", "192.168.0.1")
	req.RemoteAddr = "127.0.0.1"
	h.ServeHTTP(httptest.NewRecorder(), req)

	parts := strings.Split(buf.String(), " ")
	if parts[0] != "192.168.0.1,127.0.0.1" {
		t.Errorf("unexpected host ip address: %s", parts[0])
	}
}

func TestHandler_XRequestId(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(false)
	h.CLFLogger = log.New(&buf, "", 0)

	cases := []map[string]string{
		{"X-Request-Id": "abc123", "Request-Id": ""},          // X-Request-Id is used.
		{"X-REQUEST-ID": "cde", "Request-Id": ""},             // X-REQUEST-ID is used.
		{"X-Request-Id": "", "Request-Id": "foobarzoo"},       // Request-Id is used.
		{"X-Request-Id": "abc123", "Request-Id": "foobarzoo"}, // X-Request-Id takes precedence.
		{"X-Request-Id": "", "Request-Id": ""},                // v1 UUID generated.
	}

	for _, c := range cases {
		t.Run(fmt.Sprint(c), func(t *testing.T) {
			buf.Reset()
			req := MustNewRequest("GET", "/ping", nil)
			req.RemoteAddr = "127.0.0.1"

			// Set the relevant request ID headers
			var allEmpty = true
			for k, v := range c {
				req.Header.Set(k, v)
				if v != "" {
					allEmpty = false
				}
			}

			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			// Split up the HTTP log line. The request ID is currently located in
			// index 12. If the log line gets changed in the future, this test
			// will likely break and the index will need to be updated.
			parts := strings.Split(buf.String(), " ")
			i := 12

			// If neither header is set then we expect a v1 UUID to be generated.
			if allEmpty {
				if got, exp := len(parts[i]), 36; got != exp {
					t.Fatalf("got ID of length %d, expected one of length %d", got, exp)
				}
			} else if c["X-Request-Id"] != "" {
				if got, exp := parts[i], c["X-Request-Id"]; got != exp {
					t.Fatalf("got ID of %q, expected %q", got, exp)
				}
			} else if c["X-REQUEST-ID"] != "" {
				if got, exp := parts[i], c["X-REQUEST-ID"]; got != exp {
					t.Fatalf("got ID of %q, expected %q", got, exp)
				}
			} else {
				if got, exp := parts[i], c["Request-Id"]; got != exp {
					t.Fatalf("got ID of %q, expected %q", got, exp)
				}
			}

			// Check response headers
			if got, exp := w.Header().Get("Request-Id"), parts[i]; got != exp {
				t.Fatalf("Request-Id header was %s, expected %s", got, exp)
			} else if got, exp := w.Header().Get("X-Request-Id"), parts[i]; got != exp {
				t.Fatalf("X-Request-Id header was %s, expected %s", got, exp)
			}
		})
	}
}

func TestThrottler_Handler(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		throttler := httpd.NewThrottler(2, 98)

		// Send the total number of concurrent requests to the channel.
		var concurrentN int32
		concurrentCh := make(chan int)

		h := throttler.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&concurrentN, 1)
			concurrentCh <- int(atomic.LoadInt32(&concurrentN))
			time.Sleep(1 * time.Millisecond)
			atomic.AddInt32(&concurrentN, -1)
		}))

		// Execute requests concurrently.
		const n = 100
		for i := 0; i < n; i++ {
			go func() { h.ServeHTTP(nil, nil) }()
		}

		// Read the number of concurrent requests for every execution.
		for i := 0; i < n; i++ {
			if v := <-concurrentCh; v > 2 {
				t.Fatalf("concurrent requests exceed maximum: %d", v)
			}
		}
	})

	t.Run("ErrTimeout", func(t *testing.T) {
		throttler := httpd.NewThrottler(2, 1)
		throttler.EnqueueTimeout = 1 * time.Millisecond

		begin, end := make(chan struct{}), make(chan struct{})
		h := throttler.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			begin <- struct{}{}
			end <- struct{}{}
		}))

		// First two requests should execute immediately.
		go func() { h.ServeHTTP(nil, nil) }()
		go func() { h.ServeHTTP(nil, nil) }()

		<-begin
		<-begin

		// Third request should be enqueued but timeout.
		w := httptest.NewRecorder()
		h.ServeHTTP(w, nil)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != "request throttled, exceeds timeout\n" {
			t.Fatalf("unexpected response body: %q", body)
		}

		// Allow 2 existing requests to complete.
		<-end
		<-end
	})

	t.Run("ErrFull", func(t *testing.T) {
		delay := 100 * time.Millisecond
		if os.Getenv("CI") != "" {
			delay = 2 * time.Second
		}

		throttler := httpd.NewThrottler(2, 1)

		resp := make(chan struct{})
		h := throttler.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp <- struct{}{}
		}))

		// First two requests should execute immediately and third should be queued.
		go func() { h.ServeHTTP(nil, nil) }()
		go func() { h.ServeHTTP(nil, nil) }()
		go func() { h.ServeHTTP(nil, nil) }()
		time.Sleep(delay)

		// Fourth request should fail when trying to enqueue.
		w := httptest.NewRecorder()
		h.ServeHTTP(w, nil)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != "request throttled, queue full\n" {
			t.Fatalf("unexpected response body: %q", body)
		}

		// Allow 3 existing requests to complete.
		<-resp
		<-resp
		<-resp
	})
}

func TestHandlerDebugVars(t *testing.T) {
	stats := func(s ...*monitor.Statistic) ([]*monitor.Statistic, error) {
		return s, nil
	}
	stat := func(name string, tags map[string]string, vals map[string]interface{}) *monitor.Statistic {
		return &monitor.Statistic{
			Statistic: models.Statistic{
				Name:   name,
				Tags:   tags,
				Values: vals,
			},
		}
	}
	tags := func(kv ...string) map[string]string {
		if len(kv)%2 != 0 {
			panic("expect even number of key/values")
		}
		res := make(map[string]string, len(kv)/2)
		for i := 0; i < len(kv); i += 2 {
			res[kv[i]] = kv[i+1]
		}
		return res
	}
	vals := func(kv ...interface{}) map[string]interface{} {
		if len(kv)%2 != 0 {
			panic("expect even number of key/values")
		}
		res := make(map[string]interface{}, len(kv)/2)
		for i := 0; i < len(kv); i += 2 {
			if key, ok := kv[i].(string); !ok {
				panic("key must be string")
			} else {
				res[key] = kv[i+1]
			}
		}
		return res
	}

	newDiagFn := func(d map[string]*diagnostics.Diagnostics) func() (map[string]*diagnostics.Diagnostics, error) {
		return func() (map[string]*diagnostics.Diagnostics, error) {
			return d, nil
		}
	}

	var Ignored = []string{"memstats", "cmdline"}
	read := func(t *testing.T, b *bytes.Buffer, del ...string) map[string]interface{} {
		t.Helper()
		res := make(map[string]interface{})
		if err := json.Unmarshal(b.Bytes(), &res); err != nil {
			t.Fatal(err)
		}

		for _, k := range del {
			delete(res, k)
		}

		return res
	}
	keys := func(m map[string]interface{}) []string {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return keys
	}

	// stats tests the results of serializing Monitor.Statistics
	t.Run("stats", func(t *testing.T) {
		t.Run("generates unique keys using known tags", func(t *testing.T) {
			h := NewHandler(false)
			h.Monitor.StatisticsFn = func(_ map[string]string) ([]*monitor.Statistic, error) {
				return stats(
					stat("database", tags("database", "foo"), nil),
					stat("hh", tags("path", "/mnt/foo/bar"), nil),
					stat("httpd", tags("bind", "127.0.0.1:8088", "proto", "https"), nil),
					stat("other", tags("foo", "bar"), nil),
					stat("shard", tags("path", "/mnt/foo", "id", "111"), nil),
				)
			}
			h.Monitor.DiagnosticsFn = newDiagFn(map[string]*diagnostics.Diagnostics{})
			req := MustNewRequest("GET", "/debug/vars", nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			got := keys(read(t, w.Body, Ignored...))
			exp := []string{"crypto", "database:foo", "hh:/mnt/foo/bar", "httpd:https:127.0.0.1:8088", "other", "shard:/mnt/foo:111"}
			if !cmp.Equal(got, exp) {
				t.Errorf("unexpected keys; -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("generates numbered keys for collisions", func(t *testing.T) {
			// This also implicitly tests the case where no `crypto` diagnostics are not set by application.
			h := NewHandler(false)
			h.Monitor.StatisticsFn = func(_ map[string]string) ([]*monitor.Statistic, error) {
				return stats(
					stat("hh_processor", tags("db", "foo", "shardID", "10"), vals("queueSize", 100)),
					stat("hh_processor", tags("db", "foo", "shardID", "15"), vals("queueSize", 500)),
					stat("hh_processor", tags("db", "bar", "shardID", "20"), vals("queueSize", 200)),
					stat("hh_processor", tags("db", "bar", "shardID", "25"), vals("queueSize", 700)),
				)
			}
			req := MustNewRequest("GET", "/debug/vars", nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			got := read(t, w.Body, Ignored...)
			exp := map[string]interface{}{
				"crypto": map[string]interface{}{
					"FIPS":           false,
					"ensureFIPS":     false,
					"passwordHash":   "bcrypt",
					"implementation": "Go",
				},
				"hh_processor": map[string]interface{}{
					"name":   "hh_processor",
					"tags":   map[string]interface{}{"db": "foo", "shardID": "10"},
					"values": map[string]interface{}{"queueSize": float64(100)},
				},
				"hh_processor:1": map[string]interface{}{
					"name":   "hh_processor",
					"tags":   map[string]interface{}{"db": "foo", "shardID": "15"},
					"values": map[string]interface{}{"queueSize": float64(500)},
				},
				"hh_processor:2": map[string]interface{}{
					"name":   "hh_processor",
					"tags":   map[string]interface{}{"db": "bar", "shardID": "20"},
					"values": map[string]interface{}{"queueSize": float64(200)},
				},
				"hh_processor:3": map[string]interface{}{
					"name":   "hh_processor",
					"tags":   map[string]interface{}{"db": "bar", "shardID": "25"},
					"values": map[string]interface{}{"queueSize": float64(700)},
				},
			}
			if !cmp.Equal(got, exp) {
				t.Errorf("unexpected keys; -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	})

	t.Run("checks crypto diagnostic handling", func(t *testing.T) {
		h := NewHandler(false)
		// intentionally leave out "ensureFIPS" to test that code path
		h.Monitor.DiagnosticsFn = newDiagFn(
			map[string]*diagnostics.Diagnostics{
				"crypto": diagnostics.RowFromMap(map[string]interface{}{
					"FIPS":           true,
					"passwordHash":   "pbkdf2-sha256",
					"implementation": "BoringCrypto",
				}),
			})
		req := MustNewRequest("GET", "/debug/vars", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		got := read(t, w.Body, Ignored...)
		exp := map[string]interface{}{
			"crypto": map[string]interface{}{
				"FIPS":           true,
				"ensureFIPS":     nil,
				"passwordHash":   "pbkdf2-sha256",
				"implementation": "BoringCrypto",
			},
		}
		if !cmp.Equal(got, exp) {
			t.Errorf("unexpected keys; -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})

}

// NewHandler represents a test wrapper for httpd.Handler.
type Handler struct {
	*httpd.Handler
	MetaClient        *internal.MetaClientMock
	StatementExecutor HandlerStatementExecutor
	QueryAuthorizer   HandlerQueryAuthorizer
	PointsWriter      HandlerPointsWriter
	Monitor           *HandlerMonitor
	Store             *internal.StorageStoreMock
	Controller        *internal.FluxControllerMock
}

type configOption func(c *httpd.Config)

func WithAuthentication() configOption {
	return func(c *httpd.Config) {
		c.AuthEnabled = true
		c.SharedSecret = "super secret key"
	}
}

func WithPprofAuthEnabled() configOption {
	return func(c *httpd.Config) {
		c.PprofEnabled = true
		c.PprofAuthEnabled = true
	}
}

func WithPingAuthEnabled() configOption {
	return func(c *httpd.Config) {
		c.PingAuthEnabled = true
	}
}

func WithFlux() configOption {
	return func(c *httpd.Config) {
		c.FluxEnabled = true
	}
}

func WithNoLog() configOption {
	return func(c *httpd.Config) {
		c.LogEnabled = false
	}
}

func WithHeaders(h map[string]string) configOption {
	return func(c *httpd.Config) {
		c.HTTPHeaders = h
	}
}

// NewHandlerConfig returns a new instance of httpd.Config with
// authentication configured.
func NewHandlerConfig(opts ...configOption) httpd.Config {
	config := httpd.NewConfig()
	for _, opt := range opts {
		opt(&config)
	}
	return config
}

// NewHandler returns a new instance of Handler.
func NewHandler(requireAuthentication bool) *Handler {
	var opts []configOption
	if requireAuthentication {
		opts = append(opts, WithAuthentication())
	}

	return NewHandlerWithConfig(NewHandlerConfig(opts...))
}

func NewHandlerWithConfig(config httpd.Config) *Handler {
	h := &Handler{
		Handler: httpd.NewHandler(config),
	}

	h.MetaClient = &internal.MetaClientMock{}
	h.Store = internal.NewStorageStoreMock()
	h.Controller = internal.NewFluxControllerMock()
	h.Monitor = newHandlerMonitor()

	h.Handler.MetaClient = h.MetaClient
	h.Handler.Store = h.Store
	h.Handler.QueryExecutor = query.NewExecutor()
	h.Handler.QueryExecutor.StatementExecutor = &h.StatementExecutor
	h.Handler.QueryAuthorizer = &h.QueryAuthorizer
	h.Handler.PointsWriter = &h.PointsWriter
	h.Handler.Monitor = h.Monitor
	h.Handler.Version = "0.0.0"
	h.Handler.BuildType = "OSS"
	h.Handler.Controller = h.Controller

	if testing.Verbose() {
		l := logger.New(os.Stdout)
		h.Handler.Logger = l
	}

	return h
}

// HandlerMonitor is a mock implementation of Handler.Monitor.
type HandlerMonitor struct {
	StatisticsFn  func(tags map[string]string) ([]*monitor.Statistic, error)
	DiagnosticsFn func() (map[string]*diagnostics.Diagnostics, error)
}

// newHandlerMonitor returns a HandlerMonitor with default implementations
// for each function.
func newHandlerMonitor() *HandlerMonitor {
	return &HandlerMonitor{
		StatisticsFn: func(_ map[string]string) ([]*monitor.Statistic, error) {
			return nil, nil
		},
		DiagnosticsFn: func() (map[string]*diagnostics.Diagnostics, error) {
			return make(map[string]*diagnostics.Diagnostics), nil
		},
	}
}

func (m *HandlerMonitor) Statistics(tags map[string]string) ([]*monitor.Statistic, error) {
	return m.StatisticsFn(tags)
}

func (m *HandlerMonitor) Diagnostics() (map[string]*diagnostics.Diagnostics, error) {
	return m.DiagnosticsFn()
}

// HandlerStatementExecutor is a mock implementation of Handler.StatementExecutor.
type HandlerStatementExecutor struct {
	ExecuteStatementFn func(stmt influxql.Statement, ctx *query.ExecutionContext) error
}

func (e *HandlerStatementExecutor) ExecuteStatement(ctx *query.ExecutionContext, stmt influxql.Statement) error {
	return e.ExecuteStatementFn(stmt, ctx)
}

// HandlerQueryAuthorizer is a mock implementation of Handler.QueryAuthorizer.
type HandlerQueryAuthorizer struct {
	AuthorizeQueryFn                 func(u meta.User, query *influxql.Query, database string) error
	AuthorizeCreateDatabaseFn        func(u meta.User) error
	AuthorizeCreateRetentionPolicyFn func(u meta.User, db string)
	AuthorizeDeleteRetentionPolicyFn func(u meta.User, db string) error
}

func (a *HandlerQueryAuthorizer) AuthorizeQuery(u meta.User, q *influxql.Query, database string) (query.FineAuthorizer, error) {
	return query.OpenAuthorizer, a.AuthorizeQueryFn(u, q, database)
}

func (a *HandlerQueryAuthorizer) AuthorizeDatabase(u meta.User, priv influxql.Privilege, database string) error {
	panic("AuthorizeDatabase: not implemented")
}

func (a *HandlerQueryAuthorizer) AuthorizeCreateDatabase(u meta.User) error {
	return a.AuthorizeCreateDatabaseFn(u)
}

func (a *HandlerQueryAuthorizer) AuthorizeCreateRetentionPolicy(u meta.User, db string) error {
	return a.AuthorizeCreateRetentionPolicy(u, db)
}

func (a *HandlerQueryAuthorizer) AuthorizeDeleteRetentionPolicy(u meta.User, db string) error {
	return a.AuthorizeDeleteRetentionPolicyFn(u, db)
}

type HandlerPointsWriter struct {
	WritePointsFn func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
}

func (h *HandlerPointsWriter) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error {
	return h.WritePointsFn(database, retentionPolicy, consistencyLevel, user, points)
}

// MustNewRequest returns a new HTTP request. Panic on error.
func MustNewRequest(method, urlStr string, body io.Reader) *http.Request {
	r, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		panic(err.Error())
	}
	return r
}

// MustNewRequest returns a new HTTP request with the content type set. Panic on error.
func MustNewJSONRequest(method, urlStr string, body io.Reader) *http.Request {
	r := MustNewRequest(method, urlStr, body)
	r.Header.Set("Accept", "application/json")
	return r
}

// MustJWTToken returns a new JWT token and signed string or panics trying.
func MustJWTToken(username, secret string, expired bool) (*jwt.Token, string) {
	token := jwt.New(jwt.GetSigningMethod("HS512"))
	token.Claims.(jwt.MapClaims)["username"] = username
	if expired {
		token.Claims.(jwt.MapClaims)["exp"] = time.Now().Add(-time.Second).Unix()
	} else {
		token.Claims.(jwt.MapClaims)["exp"] = time.Now().Add(time.Minute * 10).Unix()
	}
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		panic(err)
	}
	return token, signed
}

// Ensure that user supplied headers are applied to responses.
func TestHandler_UserSuppliedHeaders(t *testing.T) {

	endpoints := []struct {
		method string
		path   string
	}{
		{method: "GET", path: "/ping"},
		{method: "POST", path: "/api/v2/query"},
		{method: "GET", path: "/query?db=foo&q=SELECT+*+FROM+bar"},
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint.method+endpoint.path, func(t *testing.T) {
			headers := map[string]string{
				"X-Best-Operating-System": "FreeBSD",
				"X-Nana-Nana-Nana-Nana":   "Batheader",
				"X-Powered-By":            "hamster in a wheel",
				"X-Trek":                  "Live long and prosper",
			}
			// build a new handler with our headers as part of its configuration
			h := NewHandlerWithConfig(NewHandlerConfig(WithHeaders(headers)))

			w := httptest.NewRecorder()

			// generate request request
			req, err := http.NewRequest(endpoint.method, endpoint.path, nil)
			if err != nil {
				t.Fatal(err)
			}

			// serve the request
			h.ServeHTTP(w, req)

			response := w.Result()
			// ensure we received the headers we supplied
			for k, v := range headers {
				val, found := response.Header[k]
				if !found {
					t.Fatalf("Could not find header field %q in response", k)
					continue
				}
				if v != val[0] {
					t.Fatalf("value for header %q in http response is %q; expected %q", k, val, v)
				}
			}
		})
	}
}
