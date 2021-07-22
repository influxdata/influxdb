package launcher_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	nethttp "net/http"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/tenant"
)

// Default context.
var ctx = context.Background()

func TestLauncher_Setup(t *testing.T) {
	l := launcher.NewTestLauncher()
	l.RunOrFail(t, ctx)
	defer l.ShutdownOrFail(t, ctx)

	client, err := http.NewHTTPClient(l.URL(), "", false)
	if err != nil {
		t.Fatal(err)
	}

	svc := &tenant.OnboardClientService{Client: client}
	if results, err := svc.OnboardInitialUser(ctx, &platform.OnboardingRequest{
		User:     "USER",
		Password: "PASSWORD",
		Org:      "ORG",
		Bucket:   "BUCKET",
	}); err != nil {
		t.Fatal(err)
	} else if results.User.ID == 0 {
		t.Fatal("expected user id")
	} else if results.Org.ID == 0 {
		t.Fatal("expected org id")
	} else if results.Bucket.ID == 0 {
		t.Fatal("expected bucket id")
	} else if results.Auth.Token == "" {
		t.Fatal("expected auth token")
	}
}

// This is to mimic the UI using cookies as sessions
// rather than authorizations
func TestLauncher_SetupWithUsers(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	r, err := nethttp.NewRequest("POST", l.URL()+"/api/v2/signin", nil)
	if err != nil {
		t.Fatal(err)
	}

	r.SetBasicAuth("USER", "PASSWORD")

	resp, err := nethttp.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	cookies := resp.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected 1 cookie but received %d", len(cookies))
	}

	user2 := &platform.User{
		Name: "USER2",
	}

	b, _ := json.Marshal(user2)
	r = l.NewHTTPRequestOrFail(t, "POST", "/api/v2/users", l.Auth.Token, string(b))

	resp, err = nethttp.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusCreated {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	r, err = nethttp.NewRequest("GET", l.URL()+"/api/v2/users", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.AddCookie(cookies[0])

	resp, err = nethttp.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusOK {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	exp := struct {
		Users []platform.User `json:"users"`
	}{}
	err = json.Unmarshal(body, &exp)
	if err != nil {
		t.Fatalf("unexpected error unmarshalling user: %v", err)
	}
	if len(exp.Users) != 2 {
		t.Fatalf("unexpected 2 users: %#+v", exp)
	}
}
