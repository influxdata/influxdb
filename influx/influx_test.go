package influx_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	gojwt "github.com/dgrijalva/jwt-go"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/influx"
	"github.com/influxdata/chronograf/log"
)

// NewClient initializes an HTTP Client for InfluxDB.
func NewClient(host string, lg chronograf.Logger) (*influx.Client, error) {
	l := lg.WithField("host", host)
	u, err := url.Parse(host)
	if err != nil {
		l.Error("Error initialize influx client: err:", err)
		return nil, err
	}
	return &influx.Client{
		URL:    u,
		Logger: l,
	}, nil
}

func Test_Influx_MakesRequestsToQueryEndpoint(t *testing.T) {
	t.Parallel()
	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(`{}`))
		called = true
		if path := r.URL.Path; path != "/query" {
			t.Error("Expected the path to contain `/query` but was", path)
		}
	}))
	defer ts.Close()

	var series chronograf.TimeSeries
	series, err := NewClient(ts.URL, log.New(log.DebugLevel))
	if err != nil {
		t.Fatal("Unexpected error initializing client: err:", err)
	}

	query := chronograf.Query{
		Command: "show databases",
	}
	_, err = series.Query(context.Background(), query)
	if err != nil {
		t.Fatal("Expected no error but was", err)
	}

	if called == false {
		t.Error("Expected http request to Influx but there was none")
	}
}

type MockAuthorization struct {
	Bearer string
	Error  error
}

func (m *MockAuthorization) Set(req *http.Request) error {
	return m.Error
}
func Test_Influx_AuthorizationBearer(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(`{}`))
		auth := r.Header.Get("Authorization")
		tokenString := strings.Split(auth, " ")[1]
		token, err := gojwt.Parse(tokenString, func(token *gojwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*gojwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
			}
			return []byte("42"), nil
		})
		if err != nil {
			t.Errorf("Invalid token %v", err)
		}

		if claims, ok := token.Claims.(gojwt.MapClaims); ok && token.Valid {
			got := claims["username"]
			want := "AzureDiamond"
			if got != want {
				t.Errorf("Test_Influx_AuthorizationBearer got %s want %s", got, want)
			}
			return
		}
		t.Errorf("Invalid token %v", token)
	}))
	defer ts.Close()

	src := &chronograf.Source{
		Username:     "AzureDiamond",
		URL:          ts.URL,
		SharedSecret: "42",
	}
	series := &influx.Client{
		Logger: log.New(log.DebugLevel),
	}
	series.Connect(context.Background(), src)

	query := chronograf.Query{
		Command: "show databases",
	}
	_, err := series.Query(context.Background(), query)
	if err != nil {
		t.Fatal("Expected no error but was", err)
	}
}

func Test_Influx_AuthorizationBearerCtx(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(`{}`))
		got := r.Header.Get("Authorization")
		if got == "" {
			t.Error("Test_Influx_AuthorizationBearerCtx got empty string")
		}
		incomingToken := strings.Split(got, " ")[1]

		alg := func(token *gojwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*gojwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return []byte("hunter2"), nil
		}
		claims := &gojwt.MapClaims{}
		token, err := gojwt.ParseWithClaims(string(incomingToken), claims, alg)
		if err != nil {
			t.Errorf("Test_Influx_AuthorizationBearerCtx unexpected claims error %v", err)
		}
		if !token.Valid {
			t.Error("Test_Influx_AuthorizationBearerCtx unexpected valid claim")
		}
		if err := claims.Valid(); err != nil {
			t.Errorf("Test_Influx_AuthorizationBearerCtx not expires already %v", err)
		}
		user := (*claims)["username"].(string)
		if user != "AzureDiamond" {
			t.Errorf("Test_Influx_AuthorizationBearerCtx expected username AzureDiamond but got %s", user)
		}
	}))
	defer ts.Close()

	series := &influx.Client{
		Logger: log.New(log.DebugLevel),
	}

	err := series.Connect(context.Background(), &chronograf.Source{
		Username:           "AzureDiamond",
		SharedSecret:       "hunter2",
		URL:                ts.URL,
		InsecureSkipVerify: true,
	})

	query := chronograf.Query{
		Command: "show databases",
	}
	_, err = series.Query(context.Background(), query)
	if err != nil {
		t.Fatal("Expected no error but was", err)
	}
}

func Test_Influx_AuthorizationBearerFailure(t *testing.T) {
	t.Parallel()
	bearer := &MockAuthorization{
		Error: fmt.Errorf("cracked1337"),
	}

	u, _ := url.Parse("http://haxored.net")
	u.User = url.UserPassword("AzureDiamond", "hunter2")
	series := &influx.Client{
		URL:           u,
		Authorization: bearer,
		Logger:        log.New(log.DebugLevel),
	}

	query := chronograf.Query{
		Command: "show databases",
	}
	_, err := series.Query(context.Background(), query)
	if err == nil {
		t.Fatal("Test_Influx_AuthorizationBearerFailure Expected error but received nil")
	}
}

func Test_Influx_HTTPS_Failure(t *testing.T) {
	called := false
	ts := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer ts.Close()

	ctx := context.Background()
	var series chronograf.TimeSeries
	series, err := NewClient(ts.URL, log.New(log.DebugLevel))
	if err != nil {
		t.Fatal("Unexpected error initializing client: err:", err)
	}

	src := chronograf.Source{
		URL: ts.URL,
	}
	if err := series.Connect(ctx, &src); err != nil {
		t.Fatal("Unexpected error connecting to client: err:", err)
	}

	query := chronograf.Query{
		Command: "show databases",
	}
	_, err = series.Query(ctx, query)
	if err == nil {
		t.Error("Expected error but was successful")
	}

	if called == true {
		t.Error("Expected http request to fail, but, succeeded")
	}

}

func Test_Influx_HTTPS_InsecureSkipVerify(t *testing.T) {
	t.Parallel()
	called := false
	q := ""
	ts := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(`{}`))
		called = true
		if path := r.URL.Path; path != "/query" {
			t.Error("Expected the path to contain `/query` but was", path)
		}
		values := r.URL.Query()
		q = values.Get("q")
	}))
	defer ts.Close()

	ctx := context.Background()
	var series chronograf.TimeSeries
	series, err := NewClient(ts.URL, log.New(log.DebugLevel))
	if err != nil {
		t.Fatal("Unexpected error initializing client: err:", err)
	}

	src := chronograf.Source{
		URL:                ts.URL,
		InsecureSkipVerify: true,
	}
	if err := series.Connect(ctx, &src); err != nil {
		t.Fatal("Unexpected error connecting to client: err:", err)
	}

	query := chronograf.Query{
		Command: "show databases",
	}
	_, err = series.Query(ctx, query)
	if err != nil {
		t.Fatal("Expected no error but was", err)
	}

	if called == false {
		t.Error("Expected http request to Influx but there was none")
	}
	called = false
	q = ""
	query = chronograf.Query{
		Command: "select $field from cpu",
		TemplateVars: chronograf.TemplateVars{
			chronograf.BasicTemplateVar{
				Var: "$field",
				Values: []chronograf.BasicTemplateValue{
					{
						Value: "usage_user",
						Type:  "fieldKey",
					},
				},
			},
		},
	}
	_, err = series.Query(ctx, query)
	if err != nil {
		t.Fatal("Expected no error but was", err)
	}

	if called == false {
		t.Error("Expected http request to Influx but there was none")
	}

	if q != `select "usage_user" from cpu` {
		t.Errorf("Unexpected query: %s", q)
	}
}

func Test_Influx_CancelsInFlightRequests(t *testing.T) {
	t.Parallel()

	started := make(chan bool, 1)
	finished := make(chan bool, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		started <- true
		time.Sleep(20 * time.Millisecond)
		finished <- true
	}))
	defer func() {
		ts.CloseClientConnections()
		ts.Close()
	}()

	series, _ := NewClient(ts.URL, log.New(log.DebugLevel))
	ctx, cancel := context.WithCancel(context.Background())

	errs := make(chan (error))
	go func() {
		query := chronograf.Query{
			Command: "show databases",
		}

		_, err := series.Query(ctx, query)
		errs <- err
	}()

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case s := <-started:
		if !s {
			t.Errorf("Expected cancellation during request processing. Started: %t", s)
		}
	case <-timer.C:
		t.Fatalf("Expected server to finish")
	}

	cancel()

	select {
	case f := <-finished:
		if !f {
			t.Errorf("Expected cancellation during request processing. Finished: %t", f)
		}
	case <-timer.C:
		t.Fatalf("Expected server to finish")
	}

	err := <-errs
	if err != chronograf.ErrUpstreamTimeout {
		t.Error("Expected timeout error but wasn't. err was", err)
	}
}

func Test_Influx_RejectsInvalidHosts(t *testing.T) {
	_, err := NewClient(":", log.New(log.DebugLevel))
	if err == nil {
		t.Fatal("Expected err but was nil")
	}
}

func Test_Influx_ReportsInfluxErrs(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cl, err := NewClient(ts.URL, log.New(log.DebugLevel))
	if err != nil {
		t.Fatal("Encountered unexpected error while initializing influx client: err:", err)
	}

	_, err = cl.Query(context.Background(), chronograf.Query{
		Command: "show shards",
		DB:      "_internal",
		RP:      "autogen",
	})
	if err == nil {
		t.Fatal("Expected an error but received none")
	}
}

func TestClient_Roles(t *testing.T) {
	c := &influx.Client{}
	_, err := c.Roles(context.Background())
	if err == nil {
		t.Errorf("Client.Roles() want error")
	}
}
