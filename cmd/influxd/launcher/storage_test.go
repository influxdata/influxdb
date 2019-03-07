package launcher_test

import (
	"fmt"
	"io/ioutil"
	nethttp "net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
)

func TestStorage_WriteAndQuery(t *testing.T) {
	l := RunLauncherOrFail(t, ctx)

	org1 := l.OnBoardOrFail(t, &influxdb.OnboardingRequest{
		User:     "USER-1",
		Password: "PASSWORD-1",
		Org:      "ORG-01",
		Bucket:   "BUCKET",
	})
	org2 := l.OnBoardOrFail(t, &influxdb.OnboardingRequest{
		User:     "USER-2",
		Password: "PASSWORD-1",
		Org:      "ORG-02",
		Bucket:   "BUCKET",
	})

	defer l.ShutdownOrFail(t, ctx)

	// Execute single write against the server.
	l.WriteOrFail(t, org1, `m,k=v1 f=100i 946684800000000000`)
	l.WriteOrFail(t, org2, `m,k=v2 f=200i 946684800000000000`)

	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`

	exp := `,result,table,_start,_stop,_time,_value,_measurement,k,_field` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,m,v1,f` + "\r\n\r\n"
	if got := l.FluxQueryOrFail(t, org1.Org, org1.Auth.Token, qs); !cmp.Equal(got, exp) {
		t.Errorf("unexpected query results -got/+exp\n%s", cmp.Diff(got, exp))
	}

	exp = `,result,table,_start,_stop,_time,_value,_measurement,k,_field` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,200,m,v2,f` + "\r\n\r\n"
	if got := l.FluxQueryOrFail(t, org2.Org, org2.Auth.Token, qs); !cmp.Equal(got, exp) {
		t.Errorf("unexpected query results -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

// WriteOrFail attempts a write to the organization and bucket identified by to or fails if there is an error.
func (l *Launcher) WriteOrFail(tb testing.TB, to *influxdb.OnboardingResults, data string) {
	tb.Helper()
	resp, err := nethttp.DefaultClient.Do(l.NewHTTPRequestOrFail(tb, "POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", to.Org.ID, to.Bucket.ID), to.Auth.Token, data))
	if err != nil {
		tb.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		tb.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		tb.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		tb.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}
}

// FluxQueryOrFail performs a query to the specified organization and returns the results
// or fails if there is an error.
func (l *Launcher) FluxQueryOrFail(tb testing.TB, org *influxdb.Organization, token string, query string) string {
	tb.Helper()

	b, err := http.SimpleQuery(l.URL(), query, org.Name, token)
	if err != nil {
		tb.Fatal(err)
	}

	return string(b)
}
