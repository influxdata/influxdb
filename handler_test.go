package influxdb_test

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	//"fmt"
	"github.com/influxdb/influxdb"
)

func init() {
	influxdb.BcryptCost = 4
}

func TestHandler_Databases(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateDatabase("bar")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/db`, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `["bar","foo"]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDatabase(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/db`, `{"name": "foo"}`)

	if status != http.StatusCreated {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDatabase_BadRequest_NoName(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/db`, `{"BadRequest": 1}`)

	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `database name required` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDatabase_BadRequest_InvalidJSON(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/db`, `"BadRequest": 1`)

	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `json: cannot unmarshal string into Go value of type struct { Name string "json:\"name\"" }` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDatabase_Conflict(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/db`, `{"name": "foo"}`)

	if status != http.StatusConflict {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `database exists` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteDatabase(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/db/foo`, "")

	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteDatabase_NotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/db/foo`, "")

	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `database not found` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Shards(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	srvr.CreateShardsIfNotExists("foo", "bar", time.Time{})
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/db/foo/shards`, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"id":3,"startTime":"0001-01-01T00:00:00Z","endTime":"0001-01-01T00:00:00Z"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Shards_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/db/foo/shards`, "")

	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `database not found` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_RetentionPolicies(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/db/foo/retention_policies`, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"name":"bar","replicaN":1,"splitN":1}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_RetentionPolicies_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/db/foo/retention_policies`, "")

	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `database not found` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateRetentionPolicy(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	policy := `{"name": "bar", "duration": 1000000, "replicaN": 1, "splitN": 2}`
	status, body := MustHTTP("POST", s.URL+`/db/foo/retention_policies`, policy)

	if status != http.StatusCreated {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	policy := `{"name": "bar", "duration": 1000000, "replicaN": 1, "splitN": 2}`
	status, body := MustHTTP("POST", s.URL+`/db/foo/retention_policies`, policy)

	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "database not found" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateRetentionPolicy_Conflict(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()
	policy := `{"name": "newName", "duration": 1000000, "replicaN": 1, "splitN": 2}`
	MustHTTP("POST", s.URL+`/db/foo/retention_policies`, policy)

	status, body := MustHTTP("POST", s.URL+`/db/foo/retention_policies`, policy)

	if status != http.StatusConflict {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "retention policy exists" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateRetentionPolicy_BadRequest(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	policy := `{"name": "bar", "duration": "**BAD**", "replicaN": 1, "splitN": 2}`
	status, body := MustHTTP("POST", s.URL+`/db/foo/retention_policies`, policy)

	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "json: cannot unmarshal string into Go value of type time.Duration" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_UpdateRetentionPolicy(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("PUT", s.URL+`/db/foo/retention_policies/bar`,
		`{"name": "newName", "duration": 1000000, "replicaN": 1, "splitN": 2}`)

	// Verify updated policy.
	p, _ := srvr.RetentionPolicy("foo", "newName")
	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "" {
		t.Fatalf("unexpected body: %s", body)
	} else if p.Name != "newName" {
		t.Fatalf("unexpected policy name: %s", p.Name)
	}
}

func TestHandler_UpdateRetentionPolicy_BadRequest(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	newPolicy := `{"name": "newName", "duration": "BadRequest", "replicaN": 1, "splitN": 2}`
	status, body := MustHTTP("PUT", s.URL+`/db/foo/retention_policies/bar`, newPolicy)

	// Verify response.
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "json: cannot unmarshal string into Go value of type time.Duration" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_UpdateRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	newPolicy := `{"name": "newName", "duration": 1000000, "replicaN": 1, "splitN": 2}`
	status, body := MustHTTP("PUT", s.URL+`/db/foo/retention_policies/bar`, newPolicy)

	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "database not found" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_UpdateRetentionPolicy_NotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	newPolicy := `{"name": "newName", "duration": 1000000, "replicaN": 1, "splitN": 2}`
	status, body := MustHTTP("PUT", s.URL+`/db/foo/retention_policies/bar`, newPolicy)

	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "retention policy not found" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteRetentionPolicy(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/db/foo/retention_policies/bar`, "")
	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/db/foo/retention_policies/bar`, "")

	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "database not found" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteRetentionPolicy_NotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/db/foo/retention_policies/bar`, "")

	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "retention policy not found" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Ping(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("GET", s.URL+`/ping`, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_Users_NoUsers(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/users`, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "[]" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Users_OneUser(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", true)
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/users`, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"name":"jdoe","admin":true}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Users_MultipleUsers(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", false)
	srvr.CreateUser("mclark", "1337", true)
	srvr.CreateUser("csmith", "1337", false)
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/users`, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"name":"csmith"},{"name":"jdoe"},{"name":"mclark","admin":true}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateUser(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/users`, `{"name":"jdoe","password":"1337"}`)
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateUser_BadRequest(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/users`, `{"name":0xBAD,"password":"1337"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `invalid character 'x' after object key:value pair` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateUser_InternalServerError(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/users`, `{"name":""}`)
	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `username required` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_UpdateUser(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", false)
	s := NewHTTPServer(srvr)
	defer s.Close()

	// Save original password hash.
	hash := srvr.User("jdoe").Hash

	// Update user password.
	status, body := MustHTTP("PUT", s.URL+`/users/jdoe`, `{"password": "7331"}`)
	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `` {
		t.Fatalf("unexpected body: %s", body)
	} else if srvr.User("jdoe").Hash == hash {
		t.Fatalf("expected password hash to change")
	}
}

func TestHandler_UpdateUser_PasswordBadRequest(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", false)
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("PUT", s.URL+`/users/jdoe`, `{"password": 10}`)
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `json: cannot unmarshal number into Go value of type string` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteUser(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", false)
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/users/jdoe`, "")
	if status != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteUser_UserNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/users/jdoe`, "")
	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `user not found` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func MustHTTP(verb, url, body string) (int, string) {
	req, err := http.NewRequest(verb, url, bytes.NewBuffer([]byte(body)))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "applicaton/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimRight(string(b), "\n")
}

// Server is a test HTTP server that wraps a handler
type HTTPServer struct {
	*httptest.Server
	Handler *influxdb.Handler
}

func NewHTTPServer(s *Server) *HTTPServer {
	h := influxdb.NewHandler(s.Server)
	return &HTTPServer{httptest.NewServer(h), h}
}

func (s *HTTPServer) Close() {
	s.Server.Close()
}
