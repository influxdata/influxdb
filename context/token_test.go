package context_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
)

func TestGetToken(t *testing.T) {
	ctx := context.Background()
	ctx = icontext.SetAuthorizer(ctx, &influxdb.Authorization{
		Token: "howdy",
	})
	got, err := icontext.GetToken(ctx)
	if err != nil {
		t.Errorf("unexpected error while retrieving token: %v", err)
	}

	if want := "howdy"; got != want {
		t.Errorf("GetToken() want %s, got %s", want, got)
	}
}
