package influxdb_test

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "LIST DATABASES"}, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"rows":[{"columns":["Name"],"values":[["bar"],["foo"]]}]}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDatabase(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "CREATE DATABASE foo"}, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDatabase_BadRequest_NoName(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "CREATE DATABASE"}, nil, "")
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_CreateDatabase_Conflict(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "CREATE DATABASE foo"}, nil, "")
	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"error":"database exists"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteDatabase(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "DROP DATABASE foo"}, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteDatabase_NotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "DROP DATABASE bar"}, nil, "")
	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"error":"database not found"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_RetentionPolicies(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "LIST RETENTION POLICIES foo"}, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"rows":[{"columns":["Name"],"values":[["bar"]]}]}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_RetentionPolicies_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "LIST RETENTION POLICIES foo"}, nil, "")

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"error":"database not found"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateRetentionPolicy(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_CreateRetentionPolicy_Conflict(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1"}
	MustHTTP("GET", s.URL+`/query`, query, nil, "")

	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_CreateRetentionPolicy_BadRequest(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE RETENTION POLICY bar ON foo DURATION ***BAD*** REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_UpdateRetentionPolicy(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "ALTER RETENTION POLICY bar ON foo REPLICATION 42 DURATION 1m"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	// Verify updated policy.
	p, _ := srvr.RetentionPolicy("foo", "bar")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "" {
		t.Fatalf("unexpected body: %s", body)
	} else if p.ReplicaN != 42 {
		t.Fatalf("unexpected replication factor: %d", p.ReplicaN)
	}
}

func TestHandler_UpdateRetentionPolicy_BadRequest(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "ALTER RETENTION POLICY bar ON foo DURATION ***BAD*** REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	// Verify response.
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_UpdateRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "ALTER RETENTION POLICY bar ON foo DURATION 1h REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	// Verify response.
	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_UpdateRetentionPolicy_NotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "ALTER RETENTION POLICY qux ON foo DURATION 1h REPLICATION 1"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	// Verify response.
	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_DeleteRetentionPolicy(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "DROP RETENTION POLICY bar ON foo"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteRetentionPolicy_DatabaseNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "DROP RETENTION POLICY bar ON qux"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"error":"database not found"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteRetentionPolicy_NotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "DROP RETENTION POLICY bar ON foo"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"error":"retention policy not found"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Ping(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("GET", s.URL+`/ping`, nil, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_Users_NoUsers(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/users`, nil, nil, "")

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "[]" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Users_OneUser(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", true)
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/users`, nil, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"name":"jdoe","admin":true}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Users_MultipleUsers(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", false)
	srvr.CreateUser("mclark", "1337", true)
	srvr.CreateUser("csmith", "1337", false)
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/users`, nil, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"name":"csmith"},{"name":"jdoe"},{"name":"mclark","admin":true}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateUser(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": `CREATE USER testuser WITH PASSWORD "1337"`}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateUser_BadRequest(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE USER 0xBAD WITH PASSWORD pwd1337"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "error parsing query: found 0, expected identifier at line 1, char 13" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateUser_BadRequest_NoName(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE USER WITH PASSWORD pwd1337"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "error parsing query: found WITH, expected identifier at line 1, char 13" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateUser_BadRequest_NoPassword(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "CREATE USER jdoe"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != "error parsing query: found EOF, expected WITH at line 1, char 18" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_UpdateUser(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", false)
	s := NewHTTPServer(srvr)
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
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("jdoe", "1337", false)
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("PUT", s.URL+`/users/jdoe`, nil, nil, `{"password": 10}`)
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

	query := map[string]string{"q": "DROP USER jdoe"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteUser_UserNotFound(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "DROP USER jdoe"}
	status, body := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"error":"user not found"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DataNodes(t *testing.T) {
	t.Skip()
	srvr := OpenUninitializedServer(NewMessagingClient())
	srvr.CreateDataNode(MustParseURL("http://localhost:1000"))
	srvr.CreateDataNode(MustParseURL("http://localhost:2000"))
	srvr.CreateDataNode(MustParseURL("http://localhost:3000"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("GET", s.URL+`/data_nodes`, nil, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `[{"id":1,"url":"http://localhost:1000"},{"id":2,"url":"http://localhost:2000"},{"id":3,"url":"http://localhost:3000"}]` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDataNode(t *testing.T) {
	t.Skip()
	srvr := OpenUninitializedServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/data_nodes`, nil, nil, `{"url":"http://localhost:1000"}`)
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `{"id":1,"url":"http://localhost:1000"}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDataNode_BadRequest(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/data_nodes`, nil, nil, `{"name":`)
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `unexpected EOF` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_CreateDataNode_InternalServerError(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/data_nodes`, nil, nil, `{"url":""}`)
	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d, %s", status, body)
	} else if body != `data node url required` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteDataNode(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDataNode(MustParseURL("http://localhost:1000"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/data_nodes/1`, nil, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_DeleteUser_DataNodeNotFound(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("DELETE", s.URL+`/data_nodes/10000`, nil, nil, "")
	if status != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", status)
	} else if body != `data node not found` {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Perform a subset of endpoint testing, with authentication enabled.

func TestHandler_AuthenticatedCreateAdminUser(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	s := NewAuthenticatedHTTPServer(srvr)
	defer s.Close()

	// Attempting to create a non-admin user should fail.
	query := map[string]string{"q": "CREATE USER maeve WITH PASSWORD pass"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}

	// Creating the first admin user, without supplying authentication
	// credentials should be OK.
	query = map[string]string{"q": "CREATE USER orla WITH PASSWORD pass WITH ALL PRIVILEGES"}
	status, _ = MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}

	// Creating a second admin user, without supplying authentication
	// credentials should fail.
	query = map[string]string{"q": "CREATE USER louise WITH PASSWORD pass WITH ALL PRIVILEGES"}
	status, _ = MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}

}

func TestHandler_AuthenticatedDatabases_Unauthorized(t *testing.T) {
	t.Skip()
	srvr := OpenServer(NewMessagingClient())
	s := NewAuthenticatedHTTPServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("GET", s.URL+`/query`, map[string]string{"q": "LIST DATABASES"}, nil, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_AuthenticatedDatabases_AuthorizedQueryParams(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("lisa", "password", true)
	s := NewAuthenticatedHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "LIST DATABASES", "u": "lisa", "p": "password"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_AuthenticatedDatabases_UnauthorizedQueryParams(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("lisa", "password", true)
	s := NewAuthenticatedHTTPServer(srvr)
	defer s.Close()

	query := map[string]string{"q": "LIST DATABASES", "u": "lisa", "p": "wrong"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, nil, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_AuthenticatedDatabases_AuthorizedBasicAuth(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("lisa", "password", true)
	s := NewAuthenticatedHTTPServer(srvr)
	defer s.Close()

	auth := make(map[string]string)
	auth["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte("lisa:password"))
	query := map[string]string{"q": "LIST DATABASES"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, auth, "")
	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_AuthenticatedDatabases_UnauthorizedBasicAuth(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateUser("lisa", "password", true)
	s := NewAuthenticatedHTTPServer(srvr)
	defer s.Close()

	auth := make(map[string]string)
	auth["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte("lisa:wrong"))
	query := map[string]string{"q": "LIST DATABASES"}
	status, _ := MustHTTP("GET", s.URL+`/query`, query, auth, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_serveWriteSeries(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	srvr.CreateDatabase("foo")
	srvr.CreateRetentionPolicy("foo", influxdb.NewRetentionPolicy("bar"))
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, _ := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2009-11-10T23:00:00Z","values": {"value": 100}}]}`)

	if status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
}

func TestHandler_serveWriteSeries_noDatabaseExists(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2009-11-10T23:00:00Z","values": {"value": 100}}]}`)

	expectedStatus := http.StatusNotFound
	if status != expectedStatus {
		t.Fatalf("unexpected status: expected: %d, actual: %d", expectedStatus, status)
	}

	response := `{"error":"database not found: \"foo\""}`
	if body != response {
		t.Fatalf("unexpected body: expected %s, actual %s", response, body)
	}
}

func TestHandler_serveWriteSeries_invalidJSON(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{"database" : foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2009-11-10T23:00:00Z","values": {"value": 100}}]}`)

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: expected: %d, actual: %d", http.StatusInternalServerError, status)
	}

	response := `{"error":"invalid character 'o' in literal false (expecting 'a')"}`
	if body != response {
		t.Fatalf("unexpected body: expected %s, actual %s", response, body)
	}
}

func TestHandler_serveWriteSeries_noDatabaseSpecified(t *testing.T) {
	srvr := OpenServer(NewMessagingClient())
	s := NewHTTPServer(srvr)
	defer s.Close()

	status, body := MustHTTP("POST", s.URL+`/write`, nil, nil, `{}`)

	if status != http.StatusInternalServerError {
		t.Fatalf("unexpected status: expected: %d, actual: %d", http.StatusInternalServerError, status)
	}

	response := `{"error":"database is required"}`
	if body != response {
		t.Fatalf("unexpected body: expected %s, actual %s", response, body)
	}
}

// Utility functions for this test suite.

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

	client := &http.Client{}
	resp, err := client.Do(req)
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

// Server is a test HTTP server that wraps a handler
type HTTPServer struct {
	*httptest.Server
	Handler *influxdb.Handler
}

func NewHTTPServer(s *Server) *HTTPServer {
	h := influxdb.NewHandler(s.Server)
	return &HTTPServer{httptest.NewServer(h), h}
}

func NewAuthenticatedHTTPServer(s *Server) *HTTPServer {
	h := influxdb.NewHandler(s.Server)
	h.AuthenticationEnabled = true
	return &HTTPServer{httptest.NewServer(h), h}
}

func (s *HTTPServer) Close() {
	s.Server.Close()
}
