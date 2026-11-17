package launcher_test

import (
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	th "github.com/influxdata/influxdb/v2/pkg/testing/helper"
)

// TestLauncher_Scrapers creates and updates a scraper target, and checks that a
// target can't be given a bucket that doesn't exist.
func TestLauncher_Scrapers(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	const (
		missingBucketID = "aaaaaaaaaaaaaaaa"
		bucketNotFound  = `{"code":"not found","message":"bucket not found"}`
	)

	// do sends a request and returns the status code and body.
	do := func(t *testing.T, method, path, body string) (int, string) {
		t.Helper()
		req := l.NewHTTPRequestOrFail(t, method, path, l.Auth.Token, body)
		req.Header.Set("Content-Type", "application/json")
		resp, err := nethttp.DefaultClient.Do(req)
		require.NoError(t, err)
		defer th.CheckedClose(t, resp.Body)()
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(raw)
	}

	createBody := func(bucketID string) string {
		return fmt.Sprintf(`{"name":"s","type":"prometheus","url":"http://localhost:1/metrics","orgID":%q,"bucketID":%q}`,
			l.Org.ID, bucketID)
	}

	type target struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		BucketID string `json:"bucketID"`
		Bucket   string `json:"bucket"`
	}
	decode := func(t *testing.T, raw string) target {
		t.Helper()
		var got target
		require.NoError(t, json.Unmarshal([]byte(raw), &got))
		return got
	}

	code, raw := do(t, "POST", "/api/v2/scrapers", createBody(l.Bucket.ID.String()))
	require.Equalf(t, nethttp.StatusCreated, code, "body: %s", raw)
	created := decode(t, raw)
	require.NotEmpty(t, created.ID)
	require.Equal(t, target{ID: created.ID, Name: "s", BucketID: l.Bucket.ID.String(), Bucket: l.Bucket.Name}, created)

	t.Run("rename", func(t *testing.T) {
		code, raw := do(t, "PATCH", "/api/v2/scrapers/"+created.ID, `{"name":"renamed"}`)
		require.Equalf(t, nethttp.StatusOK, code, "body: %s", raw)
		want := target{ID: created.ID, Name: "renamed", BucketID: l.Bucket.ID.String(), Bucket: l.Bucket.Name}
		require.Equal(t, want, decode(t, raw))
	})

	t.Run("create with a bucket that does not exist", func(t *testing.T) {
		code, raw := do(t, "POST", "/api/v2/scrapers", createBody(missingBucketID))
		require.Equalf(t, nethttp.StatusNotFound, code, "body: %s", raw)
		require.JSONEq(t, bucketNotFound, raw)
	})

	t.Run("update with a bucket that does not exist", func(t *testing.T) {
		code, raw := do(t, "PATCH", "/api/v2/scrapers/"+created.ID, fmt.Sprintf(`{"bucketID":%q}`, missingBucketID))
		require.Equalf(t, nethttp.StatusNotFound, code, "body: %s", raw)
		require.JSONEq(t, bucketNotFound, raw)

		code, raw = do(t, "GET", "/api/v2/scrapers/"+created.ID, "")
		require.Equalf(t, nethttp.StatusOK, code, "body: %s", raw)
		require.Equal(t, l.Bucket.ID.String(), decode(t, raw).BucketID)
	})
}
