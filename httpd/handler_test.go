package httpd_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/httpd"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

func TestBatchWrite_UnmarshalEpoch(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name      string
		epoch     int64
		precision string
		expected  time.Time
	}{
		{
			name:      "nanoseconds",
			epoch:     now.UnixNano(),
			precision: "n",
			expected:  now,
		},
		{
			name:      "microseconds",
			epoch:     now.Round(time.Microsecond).UnixNano() / int64(time.Microsecond),
			precision: "u",
			expected:  now.Round(time.Microsecond),
		},
		{
			name:      "milliseconds",
			epoch:     now.Round(time.Millisecond).UnixNano() / int64(time.Millisecond),
			precision: "ms",
			expected:  now.Round(time.Millisecond),
		},
		{
			name:      "seconds",
			epoch:     now.Round(time.Second).UnixNano() / int64(time.Second),
			precision: "s",
			expected:  now.Round(time.Second),
		},
		{
			name:      "minutes",
			epoch:     now.Round(time.Minute).UnixNano() / int64(time.Minute),
			precision: "m",
			expected:  now.Round(time.Minute),
		},
		{
			name:      "hours",
			epoch:     now.Round(time.Hour).UnixNano() / int64(time.Hour),
			precision: "h",
			expected:  now.Round(time.Hour),
		},
		{
			name:      "max int64",
			epoch:     9223372036854775807,
			precision: "n",
			expected:  time.Unix(0, 9223372036854775807),
		},
		{
			name:      "100 years from now",
			epoch:     now.Add(time.Hour * 24 * 365 * 100).UnixNano(),
			precision: "n",
			expected:  now.Add(time.Hour * 24 * 365 * 100),
		},
	}

	for _, test := range tests {
		t.Logf("testing %q\n", test.name)
		data := []byte(fmt.Sprintf(`{"time": %d, "precision":"%s"}`, test.epoch, test.precision))
		t.Logf("json: %s", string(data))
		var bp client.BatchPoints
		err := json.Unmarshal(data, &bp)
		if err != nil {
			t.Fatalf("unexpected error.  expected: %v, actual: %v", nil, err)
		}
		if !bp.Time.Equal(test.expected) {
			t.Fatalf("Unexpected time.  expected: %v, actual: %v", test.expected, bp.Time)
		}
	}
}

func TestBatchWrite_UnmarshalRFC(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		rfc      string
		now      time.Time
		expected time.Time
	}{
		{
			name:     "RFC3339Nano",
			rfc:      time.RFC3339Nano,
			now:      now,
			expected: now,
		},
		{
			name:     "RFC3339",
			rfc:      time.RFC3339,
			now:      now.Round(time.Second),
			expected: now.Round(time.Second),
		},
	}

	for _, test := range tests {
		t.Logf("testing %q\n", test.name)
		ts := test.now.Format(test.rfc)
		data := []byte(fmt.Sprintf(`{"time": %q}`, ts))
		t.Logf("json: %s", string(data))
		var bp client.BatchPoints
		err := json.Unmarshal(data, &bp)
		if err != nil {
			t.Fatalf("unexpected error.  exptected: %v, actual: %v", nil, err)
		}
		if !bp.Time.Equal(test.expected) {
			t.Fatalf("Unexpected time.  expected: %v, actual: %v", test.expected, bp.Time)
		}
	}
}

// Ensure the handler returns results from a query (including nil results).
func TestHandler_Query(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error) {
		if q.String() != `SELECT * FROM bar` {
			t.Fatalf("unexpected query: %s", q.String())
		} else if db != `foo` {
			t.Fatalf("unexpected db: %s", db)
		}
		return NewResultChan(
			&influxql.Result{StatementID: 1, Series: influxql.Rows{{Name: "series0"}}},
			&influxql.Result{StatementID: 2, Series: influxql.Rows{{Name: "series1"}}},
			nil,
		), nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"series":[{"name":"series0"}]},{"series":[{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler merges results from the same statement.
func TestHandler_Query_MergeResults(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error) {
		return NewResultChan(
			&influxql.Result{StatementID: 1, Series: influxql.Rows{{Name: "series0"}}},
			&influxql.Result{StatementID: 1, Series: influxql.Rows{{Name: "series1"}}},
		), nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"series":[{"name":"series0"},{"name":"series1"}]}]}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler can parse chunked and chunk size query parameters.
func TestHandler_Query_Chunked(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error) {
		if chunkSize != 2 {
			t.Fatalf("unexpected chunk size: %d", chunkSize)
		}
		return NewResultChan(
			&influxql.Result{StatementID: 1, Series: influxql.Rows{{Name: "series0"}}},
			&influxql.Result{StatementID: 1, Series: influxql.Rows{{Name: "series1"}}},
		), nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SELECT+*+FROM+bar&chunked=true&chunk_size=2", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"series":[{"name":"series0"}]}]}{"results":[{"series":[{"name":"series1"}]}]}` {
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
func TestHandler_Query_ErrUnauthorized(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error) {
		return nil, meta.NewAuthError("marker")
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SHOW+SERIES+FROM+bar", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

// Ensure the handler returns a status 500 if an error is returned from the query executor.
func TestHandler_Query_ErrExecuteQuery(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SHOW+SERIES+FROM+bar", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

// Ensure the handler returns a status 200 if an error is returned in the result.
func TestHandler_Query_ErrResult(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error) {
		return NewResultChan(&influxql.Result{Err: errors.New("measurement not found")}), nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SHOW+SERIES+from+bin", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"error":"measurement not found"}]}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

// Ensure the handler returns a status 401 if an auth error is returned from the result.
func TestHandler_Query_Result_ErrUnauthorized(t *testing.T) {
	h := NewHandler(false)
	h.QueryExecutor.ExecuteQueryFn = func(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error) {
		return NewResultChan(&influxql.Result{Err: meta.NewAuthError("marker")}), nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewJSONRequest("GET", "/query?db=foo&q=SHOW+SERIES+from+bin", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body.String() != `{"results":[{"error":"marker"}]}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
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

/*

func TestHandler_CreateDatabase(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "CREATE DATABASE foo"}, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDatabase_BadRequest_NoName(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "CREATE DATABASE"}, nil, "")
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_CreateDatabase_Conflict(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "CREATE DATABASE foo"}, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{"error":"database exists"}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DropDatabase(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "DROP DATABASE foo"}, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DropDatabase_NotFound(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "DROP DATABASE bar"}, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if !matchRegex(`database not found: bar.*`, body) {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_RetentionPolicies(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "SHOW RETENTION POLICIES foo"}, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if !strings.Contains(body, `{"results":[{"series":[{"columns":["name","duration","replicaN","default"],"values":[["bar","168h0m0s",1,false],["default","0",1,true]]}]}]}`) {
		t.Fatalf("Missing retention policy: %s", body)
	}
}

func TestHandler_RetentionPolicies_DatabaseNotFound(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "SHOW RETENTION POLICIES foo"}, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if !matchRegex(`database not found: foo.*`, body) {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateRetentionPolicy(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateRetentionPolicyAsDefault(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1 DEFAULT"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{}]}` {
		t.Fatalf("unexpected body: %s", body)
	}

	rp, err := srvr.DefaultRetentionPolicy("foo")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "bar" {
		t.Fatalf("default retention policy mismatch:\n  exp=%s\n  got=%s\n", "bar", rp.Name)
	}
}

func TestHandler_CreateRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_CreateRetentionPolicy_Conflict(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1"}
	MustHTTP("GET", s.URL+`/query`, query, nil, "")

	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_CreateRetentionPolicy_BadRequest(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION ***BAD*** REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_UpdateRetentionPolicy(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "ALTER RETENTION POLICY bar ON foo REPLICATION 42 DURATION 2h DEFAULT"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	// Verify updated policy.
	p, _ := srvr.RetentionPolicy("foo", "bar")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{}]}` {
		t.Fatalf("unexpected body: %s", body)
	} else if p.ReplicaN != 42 {
		t.Fatalf("unexpected replication factor: %d", p.ReplicaN)
	}

	// Make sure retention policy has been set as default.
	if p, err := srvr.DefaultRetentionPolicy("foo"); err != nil {
		t.Fatal(err)
	} else if p == nil {
		t.Fatal("default retention policy not set")
	} else if p.Name != "bar" {
		t.Fatal(`expected default retention policy to be "bar"`)
	}
}

func TestHandler_UpdateRetentionPolicy_BadRequest(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "ALTER RETENTION POLICY bar ON foo DURATION ***BAD*** REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	// Verify response.
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_UpdateRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "ALTER RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	// Verify response.
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_UpdateRetentionPolicy_NotFound(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "ALTER RETENTION POLICY qux ON foo DURATION 1h REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	// Verify response.
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_DeleteRetentionPolicy(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "DROP RETENTION POLICY bar ON foo"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "DROP RETENTION POLICY bar ON qux"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if !matchRegex(`database not found: .*qux.*`, body) {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteRetentionPolicy_NotFound(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "DROP RETENTION POLICY bar ON foo"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{"error":"retention policy not found"}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_GzipEnabled(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	req, err := http.NewRequest("GET", s.URL+`/ping`, bytes.NewBuffer([]byte{}))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if ce := resp.Header.Get("Content-Encoding"); ce != "gzip" {
		t.Fatalf("unexpected Content-Encoding.  expected %q, actual: %q", "gzip", ce)
	}
}

func TestHandler_GzipDisabled(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	req, err := http.NewRequest("GET", s.URL+`/ping`, bytes.NewBuffer([]byte{}))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if ce := resp.Header.Get("Content-Encoding"); ce == "gzip" {
		t.Fatalf("unexpected Content-Encoding.  expected %q, actual: %q", "", ce)
	}
}

func TestHandler_Ping(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("GET", s.URL+`/ping`, nil, nil, "")

	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_PingHead(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("HEAD", s.URL+`/ping`, nil, nil, "")

	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_Users_MultipleUsers(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateUser("jdoe", "1337", false)
	srvr.CreateUser("mclark", "1337", true)
	srvr.CreateUser("csmith", "1337", false)
	s := NewAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "SHOW USERS"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"results":[{"series":[{"columns":["user","admin"],"values":[["csmith",false],["jdoe",false],["mclark",true]]}]}]}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_UpdateUser(t *testing.T) {
	t.Skip()
	srvr := OpenAuthlessServer()
	srvr.CreateUser("jdoe", "1337", false)
	s := NewAPIServer(srvr)
	defer s.Close()

	// Save original password hash.
	hash := srvr.User("jdoe").Hash

	// Update user password.
	status, body := MustHTTP("PUT", s.URL+`/users/jdoe`, nil, nil, `{"password": "7331"}`)
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `` {
		t.Fatalf("unexpected body: %s", body)
	} else if srvr.User("jdoe").Hash == hash {
		t.Fatalf("expected password hash to change")
	}
}

func TestHandler_UpdateUser_PasswordBadRequest(t *testing.T) {
	t.Skip()
	srvr := OpenAuthlessServer()
	srvr.CreateUser("jdoe", "1337", false)
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("PUT", s.URL+`/users/jdoe`, nil, nil, `{"password": 10}`)
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `json: cannot unmarshal number into Go value of type string` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DataNodes(t *testing.T) {
	t.Skip()
	srvr := OpenUninitializedServer()
	srvr.CreateDataNode(MustParseURL("http://localhost:1000"))
	srvr.CreateDataNode(MustParseURL("http://localhost:2000"))
	srvr.CreateDataNode(MustParseURL("http://localhost:3000"))
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/data/data_nodes`, nil, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"id":1,"url":"http://localhost:1000"},{"id":2,"url":"http://localhost:2000"},{"id":3,"url":"http://localhost:3000"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDataNode(t *testing.T) {
	t.Skip()
	srvr := OpenUninitializedServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/data/data_nodes`, nil, nil, `{"url":"http://localhost:1000"}`)
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"id":1,"url":"http://localhost:1000"}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDataNode_BadRequest(t *testing.T) {
	t.Skip()
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/data/data_nodes`, nil, nil, `{"name":`)
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `unexpected EOF` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDataNode_InternalServerError(t *testing.T) {
	t.Skip()
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/data/data_nodes`, nil, nil, `{"url":""}`)
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d, %s", status, body)
	} else if body != `data node url required` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteDataNode(t *testing.T) {
	t.Skip()
	srvr := OpenAuthlessServer()
	srvr.CreateDataNode(MustParseURL("http://localhost:1000"))
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/data/data_nodes/1`, nil, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteUser_DataNodeNotFound(t *testing.T) {
	t.Skip()
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/data/data_nodes/10000`, nil, nil, "")
	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `data node not found` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Perform a subset of endpoint testing, with authentication enabled.

func TestHandler_AuthenticatedCreateAdminUser(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	// Attempting to create a non-admin user should fail.
	query := map[string]string{"q": "CREATE USER maeve WITH PASSWORD 'pass'"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}

	// Creating an admin user, without supplying authentication credentials should fail.
	query = map[string]string{"q": "CREATE USER louise WITH PASSWORD 'pass' WITH ALL PRIVILEGES"}
	status, _ = MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}

}

func TestHandler_AuthenticatedDatabases_AuthorizedQueryParams(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	srvr.CreateUser("lisa", "password", true)
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "SHOW DATABASES", "u": "lisa", "p": "password"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_AuthenticatedDatabases_UnauthorizedQueryParams(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	srvr.CreateUser("lisa", "password", true)
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "SHOW DATABASES", "u": "lisa", "p": "wrong"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_AuthenticatedDatabases_AuthorizedBasicAuth(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	srvr.CreateUser("lisa", "password", true)
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	auth := make(map[string]string)
	auth["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte("lisa:password"))
	query := map[string]string{"q": "SHOW DATABASES"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, auth, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_AuthenticatedDatabases_UnauthorizedBasicAuth(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	srvr.CreateUser("lisa", "password", true)
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	auth := make(map[string]string)
	auth["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte("lisa:wrong"))
	query := map[string]string{"q": "SHOW DATABASES"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, auth, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_GrantDBPrivilege(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	// Create a cluster admin that will grant privilege to "john".
	srvr.CreateUser("lisa", "password", true)
	// Create user that will be granted a privilege.
	srvr.CreateUser("john", "password", false)
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	auth := make(map[string]string)
	auth["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte("lisa:password"))
	query := map[string]string{"q": "GRANT READ ON foo TO john"}

	status, _ := MustHTTP("GET", s.URL+`/query`, query, auth, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}

	u := srvr.User("john")
	if p, ok := u.Privileges["foo"]; !ok {
		t.Fatal(`expected john to have privileges on foo but he has none`)
	} else if p != influxql.ReadPrivilege {
		t.Fatalf(`expected john to have read privilege on foo but he has %s`, p.String())
	}

	// Make sure update persists after server restart.
	srvr.Restart()

	u = srvr.User("john")
	if p, ok := u.Privileges["foo"]; !ok {
		t.Fatal(`expected john to have privileges on foo but he has none after restart`)
	} else if p != influxql.ReadPrivilege {
		t.Fatalf(`expected john to have read privilege on foo but he has %s after restart`, p.String())
	}
}

func TestHandler_RevokeAdmin(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	// Create a cluster admin that will revoke admin from "john".
	srvr.CreateUser("lisa", "password", true)
	// Create user that will have cluster admin revoked.
	srvr.CreateUser("john", "password", true)
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	auth := make(map[string]string)
	auth["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte("lisa:password"))
	query := map[string]string{"q": "REVOKE ALL PRIVILEGES FROM john"}

	status, body := MustHTTP("GET", s.URL+`/query`, query, auth, "")

	if status != http.StatusOK {
		t.Log(body)
		t.Fatalf("unexpected status: %d", status)
	}

	if u := srvr.User("john"); u.Admin {
		t.Fatal(`expected user "john" not to be admin`)
	}

	// Make sure update persists after server restart.
	srvr.Restart()

	if u := srvr.User("john"); u.Admin {
		t.Fatal(`expected user "john" not to be admin after restart`)
	}
}

func TestHandler_RevokeDBPrivilege(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	// Create a cluster admin that will revoke privilege from "john".
	srvr.CreateUser("lisa", "password", true)
	// Create user that will have privilege revoked.
	srvr.CreateUser("john", "password", false)
	u := srvr.User("john")
	u.Privileges["foo"] = influxql.ReadPrivilege
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	auth := make(map[string]string)
	auth["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte("lisa:password"))
	query := map[string]string{"q": "REVOKE READ ON foo FROM john"}

	status, body := MustHTTP("GET", s.URL+`/query`, query, auth, "")

	if status != http.StatusOK {
		t.Log(body)
		t.Fatalf("unexpected status: %d", status)
	}

	if p := u.Privileges["foo"]; p != influxql.NoPrivileges {
		t.Fatal(`expected user "john" not to have privileges on foo`)
	}

	// Make sure update persists after server restart.
	srvr.Restart()

	if p := u.Privileges["foo"]; p != influxql.NoPrivileges {
		t.Fatal(`expected user "john" not to have privileges on foo after restart`)
	}
}

func TestHandler_DropSeries(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z","fields": {"value": 100}}]}`)

	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	}

	query := map[string]string{"db": "foo", "q": "DROP SERIES FROM cpu"}
	status, _ = MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_serveWriteSeries(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "default", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z","fields": {"value": 100}}]}`)

	if status != http.StatusNoContent {
		t.Fatalf("unexpected status for post: %d", status)
	}
	query := map[string]string{"db": "foo", "q": "select * from cpu"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status for get: %d", status)
	}
	if !strings.Contains(body, `"name":"cpu"`) {
		t.Fatalf("Write doesn't match query results. Response body is %s.", body)
	}
}

func TestHandler_serveDump(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "default", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z","fields": {"value": 100}}]}`)

	if status != http.StatusNoContent {
		t.Fatalf("unexpected status for post: %d", status)

	}
	query := map[string]string{"db": "foo", "q": "select * from cpu"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status for get: %d", status)
	}

	time.Sleep(500 * time.Millisecond) // Shouldn't committed data be readable?
	query = map[string]string{"db": "foo"}
	status, body = MustHTTP("GET", s.URL+`/dump`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status for get: %d", status)
	}
	if !strings.Contains(body, `"name":"cpu"`) {
		t.Fatalf("Write doesn't match query results. Response body is %s.", body)
	}
}

func TestHandler_serveWriteSeriesWithNoFields(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z"}]}`)

	expected := fmt.Sprintf(`{"error":"%s"}`, influxdb.ErrFieldsRequired.Error())

	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != expected {
		t.Fatalf("result mismatch:\n\texp=%s\n\tgot=%s\n", expected, body)
	}
}

func TestHandler_serveWriteSeriesWithNullFields(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "fields": {"country": null}}]}`)

	expected := fmt.Sprintf(`{"error":"%s"}`, influxdb.ErrFieldIsNull.Error())

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	} else if body != expected {
		t.Fatalf("result mismatch:\n\texp=%s\n\tgot=%s\n", expected, body)
	}
}

func TestHandler_serveWriteSeriesWithAuthNilUser(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewAuthenticatedAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z","fields": {"value": 100}}]}`)

	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}

	response := `{"error":"user is required to write to database \"foo\""}`
	if body != response {
		t.Fatalf("unexpected body: expected %s, actual %s", response, body)
	}
}

func TestHandler_serveWriteSeries_noDatabaseExists(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z","fields": {"value": 100}}]}`)

	expectedStatus := http.StatusNotFound
	if status != expectedStatus {
		t.Fatalf("unexpected status: expected: %d, actual: %d", expectedStatus, status)
	}

	response := `{"error":"database not found: \"foo\""}`
	if body != response {
		t.Fatalf("unexpected body: expected %s, actual %s", response, body)
	}
}

func TestHandler_serveWriteSeries_errorHasJsonContentType(t *testing.T) {
	srvr := OpenAuthlessServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	req, err := http.NewRequest("POST", s.URL+`/write`, bytes.NewBufferString("{}"))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("unexpected Content-Type.  expected %q, actual: %q", "application/json", ct)
	}
}

func TestHandler_serveWriteSeries_queryHasJsonContentType(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	srvr.SetDefaultRetentionPolicy("foo", "bar")

	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z", "fields": {"value": 100}}]}`)
	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	}
	time.Sleep(100 * time.Millisecond) // Ensure data node picks up write.

	srvr.Restart() // Ensure data is queryable across restarts.

	params := url.Values{}
	params.Add("db", "foo")
	params.Add("q", "select * from cpu")
	req, err := http.NewRequest("GET", s.URL+`/query?`+params.Encode(), bytes.NewBufferString(""))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("unexpected Content-Type.  expected %q, actual: %q", "application/json", ct)
	}

	// now test a query error
	params.Del("db")

	req_error, err := http.NewRequest("GET", s.URL+`/query?`+params.Encode(), bytes.NewBufferString(""))
	if err != nil {
		panic(err)
	}

	req_error.Header.Set("Accept-Encoding", "gzip")

	resp_error, err := http.DefaultClient.Do(req_error)
	if err != nil {
		panic(err)
	}

	if cte := resp_error.Header.Get("Content-Type"); cte != "application/json" {
		t.Fatalf("unexpected Content-Type.  expected %q, actual: %q", "application/json", cte)
	}
}

func TestHandler_serveWriteSeries_invalidJSON(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z","fields": {"value": 100}}]}`)

	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: expected: %d, actual: %d", http.StatusBadRequest, status)
	}

	response := `{"error":"invalid character 'o' in literal false (expecting 'a')"}`
	if body != response {
		t.Fatalf("unexpected body: expected %s, actual %s", response, body)
	}
}

func TestHandler_serveWriteSeries_noDatabaseSpecified(t *testing.T) {
	srvr := OpenAuthenticatedServer()
	s := NewAPIServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{}`)

	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: expected: %d, actual: %d", http.StatusBadRequest, status)
	}

	response := `{"error":"database is required"}`
	if body != response {
		t.Fatalf("unexpected body: expected %s, actual %s", response, body)
	}
}

func TestHandler_serveWriteSeriesNonZeroTime(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	srvr.SetDefaultRetentionPolicy("foo", "bar")

	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z", "fields": {"value": 100}}]}`)
	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	}
	time.Sleep(100 * time.Millisecond) // Ensure data node picks up write.

	srvr.Restart() // Ensure data is queryable across restarts.

	query := map[string]string{"db": "foo", "q": "select value from cpu"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Logf("query %s\n", query)
		t.Log(body)
		t.Errorf("unexpected status: %d", status)
	}

	r := &influxdb.Response{}
	if err := json.Unmarshal([]byte(body), r); err != nil {
		t.Logf("query : %s\n", query)
		t.Log(body)
		t.Error(err)
	}
	if len(r.Results) != 1 {
		t.Fatalf("unexpected results count")
	}

	result := r.Results[0]
	if len(result.Series) != 1 {
		t.Fatalf("unexpected row count, expected: %d, actual: %d", 1, len(result.Series))
	}
}

func TestHandler_serveWriteSeriesZeroTime(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	srvr.SetDefaultRetentionPolicy("foo", "bar")

	s := NewAPIServer(srvr)
	defer s.Close()

	now := time.Now()

	status, _ := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"fields": {"value": 100}}]}`)

	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	}
	time.Sleep(100 * time.Millisecond) // Ensure data node picks up write.

	srvr.Restart() // Ensure data is queryable across restarts.

	query := map[string]string{"db": "foo", "q": "select value from cpu"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Logf("query %s\n", query)
		t.Log(body)
		t.Errorf("unexpected status: %d", status)
	}

	r := &influxdb.Response{}
	if err := json.Unmarshal([]byte(body), r); err != nil {
		t.Logf("query : %s\n", query)
		t.Log(body)
		t.Error(err)
	}

	if len(r.Results) != 1 {
		t.Fatalf("unexpected results count")
	}
	result := r.Results[0]
	if len(result.Series) != 1 {
		t.Fatalf("unexpected row count, expected: %d, actual: %d", 1, len(result.Series))
	}
	row := result.Series[0]
	timestamp, _ := time.Parse(time.RFC3339Nano, row.Values[0][0].(string))
	if timestamp.IsZero() {
		t.Fatalf("failed to write a default time, actual: %v", row.Values[0][0])
	}
	if !timestamp.After(now) {
		t.Fatalf("time was not valid. expected something after %v, actual: %v", now, timestamp)
	}
}

func TestHandler_serveWriteSeriesBatch(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	srvr.SetDefaultRetentionPolicy("foo", "bar")

	s := NewAPIServer(srvr)
	defer s.Close()

	batch := `
{
    "database": "foo",
    "retentionPolicy": "bar",
    "points": [
        {
            "name": "disk",
            "time": "2009-11-10T23:00:00Z",
            "tags": {
                "host": "server01"
            },
            "fields": {
                "full": false
            }
        },
        {
            "name": "disk",
            "time": "2009-11-10T23:00:01Z",
            "tags": {
                "host": "server01"
            },
            "fields": {
                "full": true
            }
        },
        {
            "name": "disk",
            "time": "2009-11-10T23:00:02Z",
            "tags": {
                "host": "server02"
            },
            "fields": {
                "full_pct": 64
            }
        }
    ]
}
`
	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, batch)
	if status != http.StatusNoContent {
		t.Log(body)
		t.Fatalf("unexpected status: %d", status)
	}
	time.Sleep(200 * time.Millisecond) // Ensure data node picks up write.

	query := map[string]string{"db": "foo", "q": "select * from disk"}
	status, body = MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Logf("query %s\n", query)
		t.Log(body)
		t.Errorf("unexpected status: %d", status)
	}

	r := &influxdb.Response{}
	if err := json.Unmarshal([]byte(body), r); err != nil {
		t.Logf("query : %s\n", query)
		t.Log(body)
		t.Error(err)
	}
	if len(r.Results) != 1 {
		t.Fatalf("unexpected results count")
	}
	result := r.Results[0]
	if len(result.Series) != 1 {
		t.Fatalf("unexpected row count, expected: %d, actual: %d", 1, len(result.Series))
	}
	if len(result.Series[0].Columns) != 3 {
		t.Fatalf("unexpected column count, expected: %d, actual: %d", 3, len(result.Series[0].Columns))
	}
	if len(result.Series[0].Values) != 3 {
		t.Fatalf("unexpected values count, expected: %d, actual: %d", 3, len(result.Series[0].Values))
	}
}

func TestHandler_serveWriteSeriesFieldTypeConflict(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	srvr.SetDefaultRetentionPolicy("foo", "bar")

	s := NewAPIServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"fields": {"value": 100}}]}`)
	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	}

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"fields": {"value": "foo"}}]}`)
	if status != http.StatusInternalServerError {
		t.Errorf("unexpected status: %d", status)
	}

	r := &influxdb.Response{}
	if err := json.Unmarshal([]byte(body), r); err != nil {
		t.Log(body)
		t.Error(err)
	}
	if len(r.Results) != 0 {
		t.Fatalf("unexpected results count")
	}
	if r.Err.Error() != "field \"value\" is type string, mapped as type float" {
		t.Fatalf("unexpected error returned, actual: %s", r.Err.Error())
	}
}

// str2iface converts an array of strings to an array of interfaces.
func str2iface(strs []string) []interface{} {
	a := make([]interface{}, 0, len(strs))
	for _, s := range strs {
		a = append(a, interface{}(s))
	}
	return a
}

// Ensure the snapshot handler can write a snapshot as a tar archive over HTTP.
func TestSnapshotHandler(t *testing.T) {
	// Create handler and mock the snapshot creator.
	var h httpd.SnapshotHandler
	h.CreateSnapshotWriter = func() (*influxdb.SnapshotWriter, error) {
		return &influxdb.SnapshotWriter{
			Snapshot: &influxdb.Snapshot{
				Files: []influxdb.SnapshotFile{
					{Name: "meta", Size: 5, Index: 12},
					{Name: "shards/1", Size: 6, Index: 15},
				},
			},
			FileWriters: map[string]influxdb.SnapshotFileWriter{
				"meta":     influxdb.NopWriteToCloser(bytes.NewBufferString("55555")),
				"shards/1": influxdb.NopWriteToCloser(bytes.NewBufferString("666666")),
			},
		}, nil
	}

	// Execute handler with an existing snapshot to diff.
	// The "shards/1" has a higher index in the diff so it won't be included in the snapshot.
	w := httptest.NewRecorder()
	r, _ := http.NewRequest(
		"GET", "http://localhost/data/snapshot",
		strings.NewReader(`{"files":[{"name":"meta","index":10},{"name":"shards/1","index":20}]}`),
	)
	h.ServeHTTP(w, r)

	// Verify status code is successful and the snapshot was written.
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	} else if w.Body == nil {
		t.Fatal("body not written")
	}

	// Read snapshot.
	tr := tar.NewReader(w.Body)
	if hdr, err := tr.Next(); err != nil {
		t.Fatal(err)
	} else if hdr.Name != "manifest" {
		t.Fatalf("unexpected snapshot file: %s", hdr.Name)
	}
	if b, err := ioutil.ReadAll(tr); err != nil {
		t.Fatal(err)
	} else if string(b) != `{"files":[{"name":"meta","size":5,"index":12}]}` {
		t.Fatalf("unexpected manifest: %s", b)
	}
}

// Ensure that the server will stream out results if a chunked response is requested
func TestHandler_ChunkedResponses(t *testing.T) {
	srvr := OpenAuthlessServer()
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	srvr.SetDefaultRetentionPolicy("foo", "bar")

	s := NewAPIServer(srvr)
	defer s.Close()

	status, errString := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [
			{"name": "cpu", "tags": {"host": "server01"},"time": "2009-11-10T23:00:00Z", "fields": {"value": 100}},
			{"name": "cpu", "tags": {"host": "server02"},"time": "2009-11-10T23:30:00Z", "fields": {"value": 25}}]}`)
	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d - %s", status, errString)
	}
	time.Sleep(100 * time.Millisecond) // Ensure data node picks up write.

	resp, err := chunkedQuery(s.URL, "foo", "select value from cpu")
	if err != nil {
		t.Fatalf("error making request: %s", err.Error())
	}
	defer resp.Body.Close()

	for i := 0; i < 2; i++ {
		chunk := make([]byte, 2048, 2048)
		n, err := resp.Body.Read(chunk)
		if err != nil {
			t.Fatalf("error reading response: %s", err.Error())
		}
		response := &influxdb.Response{}
		err = json.Unmarshal(chunk[0:n], response)
		if err != nil {
			t.Fatalf("error unmarshaling resultsz: %s", err.Error())
		}
		if len(response.Results) != 1 {
			t.Fatalf("didn't get 1 result: %s\n", mustMarshalJSON(response))
		}
		if len(response.Results[0].Series) != 1 {
			t.Fatalf("didn't get 1 series: %s\n", mustMarshalJSON(response))
		}
		var vals [][]interface{}
		if i == 0 {
			vals = [][]interface{}{{"2009-11-10T23:00:00Z", 100}}
		} else {
			vals = [][]interface{}{{"2009-11-10T23:30:00Z", 25}}
		}
		if mustMarshalJSON(vals) != mustMarshalJSON(response.Results[0].Series[0].Values) {
			t.Fatalf("values weren't what was expected:\n  exp: %s\n  got: %s", mustMarshalJSON(vals), mustMarshalJSON(response.Results[0].Series[0].Values))
		}
	}
}

// batchWrite JSON Unmarshal tests

// Utility functions for this test suite.

func chunkedQuery(host, db, q string) (*http.Response, error) {
	params := map[string]string{"db": db, "q": q, "chunked": "true", "chunk_size": "1"}
	query := url.Values{}
	for k, v := range params {
		query.Set(k, v)
	}
	return http.Get(host + "/query?" + query.Encode())
}

func MustHTTP(verb, path string, params, headers map[string]string, body string) (int, string) {
	req, err := http.NewRequest(verb, path, bytes.NewBuffer([]byte(body)))
	if err != nil {
		panic(err)
	}

	if params != nil {
		q := url.Values{}
		for k, v := range params {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimRight(string(b), "\n")

}

// MustParseURL parses a string into a URL. Panic on error.
func MustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err.Error())
	}
	return u
}

// marshalJSON marshals input to a string of JSON or panics on error.
func mustMarshalJSON(i interface{}) string {
	b, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	return string(b)
}

*/

// NewHandler represents a test wrapper for httpd.Handler.
type Handler struct {
	*httpd.Handler
	MetaStore     HandlerMetaStore
	QueryExecutor HandlerQueryExecutor
	SeriesWriter  HandlerSeriesWriter
}

// NewHandler returns a new instance of Handler.
func NewHandler(requireAuthentication bool) *Handler {
	h := &Handler{
		Handler: httpd.NewHandler(requireAuthentication, true, "0.0.0"),
	}
	h.Handler.MetaStore = &h.MetaStore
	h.Handler.QueryExecutor = &h.QueryExecutor
	h.Handler.SeriesWriter = &h.SeriesWriter
	return h
}

// HandlerMetaStore is a mock implementation of Handler.MetaStore.
type HandlerMetaStore struct {
	DatabaseFn     func(name string) (*meta.DatabaseInfo, error)
	AuthenticateFn func(username, password string) (ui *meta.UserInfo, err error)
	UsersFn        func() ([]meta.UserInfo, error)
}

func (s *HandlerMetaStore) Database(name string) (*meta.DatabaseInfo, error) {
	return s.DatabaseFn(name)
}

func (s *HandlerMetaStore) Authenticate(username, password string) (ui *meta.UserInfo, err error) {
	return s.AuthenticateFn(username, password)
}

func (s *HandlerMetaStore) Users() ([]meta.UserInfo, error) {
	return s.UsersFn()
}

// HandlerQueryExecutor is a mock implementation of Handler.QueryExecutor.
type HandlerQueryExecutor struct {
	ExecuteQueryFn func(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error)
}

func (e *HandlerQueryExecutor) ExecuteQuery(q *influxql.Query, db string, user *meta.UserInfo, chunkSize int) (<-chan *influxql.Result, error) {
	return e.ExecuteQueryFn(q, db, user, chunkSize)
}

// HandlerSeriesWriter is a mock implementation of Handler.SeriesWriter.
type HandlerSeriesWriter struct {
	WriteSeriesFn func(database, retentionPolicy string, points []tsdb.Point) error
}

func (w *HandlerSeriesWriter) WriteSeries(database, retentionPolicy string, points []tsdb.Point) error {
	return w.WriteSeriesFn(database, retentionPolicy, points)
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
