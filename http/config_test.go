package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/platform"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestConfigHandler(t *testing.T) {
	t.Run("known types", func(t *testing.T) {
		stringFlag := "some string"
		boolFlag := true
		idFlag := platform.ID(1)

		opts := []cli.Opt{
			{
				DestP: &stringFlag,
				Flag:  "string-flag",
			},
			{
				DestP: &boolFlag,
				Flag:  "bool-flag",
			},
			{
				DestP: &idFlag,
				Flag:  "id-flag",
			},
		}

		want := map[string]interface{}{
			"config": map[string]interface{}{
				"string-flag": stringFlag,
				"bool-flag":   boolFlag,
				"id-flag":     idFlag,
			},
		}
		wantJsonBytes, err := json.Marshal(want)
		require.NoError(t, err)
		var wantDecoded map[string]interface{}
		require.NoError(t, json.NewDecoder(bytes.NewReader(wantJsonBytes)).Decode(&wantDecoded))

		h, err := NewConfigHandler(zaptest.NewLogger(t), opts)
		require.NoError(t, err)

		rr := httptest.NewRecorder()

		r, err := http.NewRequest(http.MethodGet, "/", nil)
		require.NoError(t, err)
		ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, influxdb.OperPermissions()))
		r = r.WithContext(ctx)
		h.ServeHTTP(rr, r)
		rs := rr.Result()

		var gotDecoded map[string]interface{}
		require.NoError(t, json.NewDecoder(rs.Body).Decode(&gotDecoded))
		require.Equal(t, gotDecoded, wantDecoded)
	})

	t.Run("unknown type", func(t *testing.T) {
		var floatFlag float64

		opts := []cli.Opt{
			{
				DestP: &floatFlag,
				Flag:  "float-flag",
			},
		}

		h, err := NewConfigHandler(zaptest.NewLogger(t), opts)
		require.Nil(t, h)
		require.Equal(t, errInvalidType(&floatFlag, "float-flag"), err)
	})
}

func TestConfigHandler_Authorization(t *testing.T) {
	tests := []struct {
		name       string
		permList   []influxdb.Permission
		wantStatus int
	}{
		{
			"authorized to see config",
			influxdb.OperPermissions(),
			http.StatusOK,
		},
		{
			"not authorized to see config",
			influxdb.ReadAllPermissions(),
			http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			rr := httptest.NewRecorder()

			r, err := http.NewRequest(http.MethodGet, "/", nil)
			require.NoError(t, err)
			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, tt.permList))
			r = r.WithContext(ctx)

			h := ConfigHandler{
				api: kithttp.NewAPI(kithttp.WithLog(zaptest.NewLogger(t))),
			}
			h.mwAuthorize(next).ServeHTTP(rr, r)
			rs := rr.Result()

			require.Equal(t, tt.wantStatus, rs.StatusCode)
		})
	}
}
