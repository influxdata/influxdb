package httpd_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
)

// Ensure the handler returns results from a query (including nil results).
func TestHandler_Query(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, chunkSize int, closing chan struct{}) <-chan *influxql.Result {
		if q.String() != `SELECT * FROM bar` {
			t.Fatalf("unexpected query: %s", q.String())
		} else if db != `foo` {
			t.Fatalf("unexpected db: %s", db)
		}
		return NewResultChan(
			&influxql.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})},
			&influxql.Result{StatementID: 2, Series: models.Rows([]*models.Row{{Name: "series1"}})},
			nil,
		)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"series":[{"name":"series0"}]},{"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler returns results from a query (including nil results).
func TestHandler_QueryRegex(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, chunkSize int, closing chan struct{}) <-chan *influxql.Result {
		if q.String() != `SELECT * FROM test WHERE url =~ /http\:\/\/www.akamai\.com/` {
			t.Fatalf("unexpected query: %s", q.String())
		} else if db != `test` {
			t.Fatalf("unexpected db: %s", db)
		}
		return NewResultChan(nil)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewRequest("GET", "/query?db=test&q=SELECT%20%2A%20FROM%20test%20WHERE%20url%20%3D~%20%2Fhttp%5C%3A%5C%2F%5C%2Fwww.akamai%5C.com%2F", nil))
}

// Ensure the handler merges results from the same statement.
func TestHandler_Query_MergeResults(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, chunkSize int, closing chan struct{}) <-chan *influxql.Result {
		return NewResultChan(
			&influxql.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})},
			&influxql.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series1"}})},
		)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"series":[{"name":"series0"},{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler merges results from the same statement.
func TestHandler_Query_MergeEmptyResults(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, chunkSize int, closing chan struct{}) <-chan *influxql.Result {
		return NewResultChan(
			&influxql.Result{StatementID: 1, Series: models.Rows{}},
			&influxql.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series1"}})},
		)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler can parse chunked and chunk size query parameters.
func TestHandler_Query_Chunked(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, chunkSize int, closing chan struct{}) <-chan *influxql.Result {
		if chunkSize != 2 {
			t.Fatalf("unexpected chunk size: %d", chunkSize)
		}
		return NewResultChan(
			&influxql.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series0"}})},
			&influxql.Result{StatementID: 1, Series: models.Rows([]*models.Row{{Name: "series1"}})},
		)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar&chunked=true&chunk_size=2", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"series":[{"name":"series0"}]}]}
{"results":[{"series":[{"name":"series1"}]}]}
` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler returns a status 400 if the query is not passed in.
func TestHandler_Query_ErrQueryRequired(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"error":"missing required parameter \"q\""}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler returns a status 400 if the query cannot be parsed.
func TestHandler_Query_ErrInvalidQuery(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?q=SELECT", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"error":"error parsing query: found EOF, expected identifier, string, number, bool at line 1, char 8"}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler returns a status 401 if the user is not authorized.
// func TestHandler_Query_ErrUnauthorized(t *testing.T) {
// 	h := NewHandler(false)
// 	h.QueryExecutor.AuthorizeFn = func(u *meta.UserInfo, q *influxql.Query, db string) error {
// 		return errors.New("marker")
// 	}

// 	w := httptest.NewRecorder()
// 	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?u=bar&db=foo&q=SHOW+SERIES+FROM+bar", nil))
// 	if w.Code != http.StatusUnauthorized {
// 		t.Fatalf("unexpected status: %d", w.Code)
// 	}
// }

// Ensure the handler returns a status 200 if an error is returned in the result.
func TestHandler_Query_ErrResult(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, chunkSize int, closing chan struct{}) <-chan *influxql.Result {
		return NewResultChan(&influxql.Result{Err: errors.New("measurement not found")})
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SHOW+SERIES+from+bin", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"error":"measurement not found"}]}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler handles ping requests correctly.
// TODO: This should be expanded to verify the MetaClient check in servePing is working correctly
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

// Ensure the handler returns the version correctly from the different endpoints.
func TestHandler_Version(t *testing.T) {
	h := NewHandler(false)
	w := httptest.NewRecorder()
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
	}

	for _, test := range tests {
		h.ServeHTTP(w, MustNewRequest(test.method, test.endpoint, test.body))
		if v, ok := w.HeaderMap["X-Influxdb-Version"]; ok {
			if v[0] != "0.0.0" {
				t.Fatalf("unexpected version: %s", v)
			}
		} else {
			t.Fatalf("Header entry 'X-Influxdb-Version' not present")
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

func TestMarshalJSON_NoPretty(t *testing.T) {
	if b := httpd.MarshalJSON(struct {
		Name string `json:"name"`
	}{Name: "foo"}, false); string(b) != `{"name":"foo"}` {
		t.Fatalf("unexpected bytes: %s", b)
	}
}

func TestMarshalJSON_Pretty(t *testing.T) {
	if b := httpd.MarshalJSON(struct {
		Name string `json:"name"`
	}{Name: "foo"}, true); string(b) != "{\n    \"name\": \"foo\"\n}" {
		t.Fatalf("unexpected bytes: %q", string(b))
	}
}

func TestMarshalJSON_Error(t *testing.T) {
	if b := httpd.MarshalJSON(&invalidJSON{}, true); string(b) != "json: error calling MarshalJSON for type *httpd_test.invalidJSON: marker" {
		t.Fatalf("unexpected bytes: %q", string(b))
	}
}

type invalidJSON struct{}

func (*invalidJSON) MarshalJSON() ([]byte, error) { return nil, errors.New("marker") }

// NewHandler represents a test wrapper for httpd.Handler.
type Handler struct {
	*httpd.Handler
	MetaClient    HandlerMetaStore
	QueryExecutor HandlerQueryExecutor
}

// NewHandler returns a new instance of Handler.
func NewHandler(requireAuthentication bool) *Handler {
	statMap := influxdb.NewStatistics("httpd", "httpd", nil)
	h := &Handler{
		Handler: httpd.NewHandler(requireAuthentication, true, false, 0, statMap),
	}
	h.Handler.MetaClient = &h.MetaClient
	h.Handler.QueryExecutor = &h.QueryExecutor
	h.Handler.Version = "0.0.0"
	return h
}

// HandlerMetaStore is a mock implementation of Handler.MetaClient.
type HandlerMetaStore struct {
	PingFn         func(d time.Duration) error
	DatabaseFn     func(name string) (*meta.DatabaseInfo, error)
	AuthenticateFn func(username, password string) (ui *meta.UserInfo, err error)
	UsersFn        func() []meta.UserInfo
}

func (s *HandlerMetaStore) Ping(b bool) error {
	if s.PingFn == nil {
		// Default behaviour is to assume there is a leader.
		return nil
	}
	return s.Ping(b)
}

func (s *HandlerMetaStore) Database(name string) (*meta.DatabaseInfo, error) {
	return s.DatabaseFn(name)
}

func (s *HandlerMetaStore) Authenticate(username, password string) (ui *meta.UserInfo, err error) {
	return s.AuthenticateFn(username, password)
}

func (s *HandlerMetaStore) Users() []meta.UserInfo {
	return s.UsersFn()
}

// HandlerQueryExecutor is a mock implementation of Handler.QueryExecutor.
type HandlerQueryExecutor struct {
	AuthorizeFn    func(u *meta.UserInfo, q *influxql.Query, db string) error
	ExecuteQueryFn func(q *influxql.Query, db string, chunkSize int, closing chan struct{}) <-chan *influxql.Result
}

func (e *HandlerQueryExecutor) Authorize(u *meta.UserInfo, q *influxql.Query, db string) error {
	return e.AuthorizeFn(u, q, db)
}

func (e *HandlerQueryExecutor) ExecuteQuery(q *influxql.Query, db string, chunkSize int, closing chan struct{}) <-chan *influxql.Result {
	return e.ExecuteQueryFn(q, db, chunkSize, closing)
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
	r.Header.Set("Content-Type", "application/json")
	return r
}

// matchRegex returns true if a s matches pattern.
func matchRegex(pattern, s string) bool {
	return regexp.MustCompile(pattern).MatchString(s)
}

// NewResultChan returns a channel that sends all results and then closes.
func NewResultChan(results ...*influxql.Result) <-chan *influxql.Result {
	ch := make(chan *influxql.Result, len(results))
	for _, r := range results {
		ch <- r
	}
	close(ch)
	return ch
}
