package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type restoreServiceMock struct {
	influxdb.RestoreService

	restoreBucketFn func(ctx context.Context, id platform.ID, dbInfo []byte) (map[uint64]uint64, error)
}

func (s *restoreServiceMock) RestoreBucket(ctx context.Context, id platform.ID, dbInfo []byte) (map[uint64]uint64, error) {
	return s.restoreBucketFn(ctx, id, dbInfo)
}

func TestRestoreBucketMetadata_OnConflict(t *testing.T) {
	orgID := platform.ID(1)
	existingID := platform.ID(2)
	newID := platform.ID(3)

	manifest := influxdb.BucketMetadataManifest{
		OrganizationID: orgID,
		BucketID:       platform.ID(4),
		BucketName:     "telegraf",
		RetentionPolicies: []influxdb.RetentionPolicyManifest{
			{Name: "autogen", Duration: time.Hour, ShardGroupDuration: time.Hour},
		},
	}

	// newHandler returns a RestoreHandler whose services append each call to
	// *calls, so tests can require both the exact operations and their order.
	newHandler := func(t *testing.T, bucketExists bool, restoreErr error, calls *[]string) *RestoreHandler {
		buckets := mock.NewBucketService()
		buckets.FindBucketByNameFn = func(_ context.Context, gotOrg platform.ID, name string) (*influxdb.Bucket, error) {
			*calls = append(*calls, "find")
			require.Equal(t, orgID, gotOrg)
			require.Equal(t, manifest.BucketName, name)
			if !bucketExists {
				return nil, &errors.Error{Code: errors.ENotFound, Msg: "bucket not found"}
			}
			return &influxdb.Bucket{ID: existingID, OrgID: orgID, Name: name}, nil
		}
		buckets.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
			*calls = append(*calls, "create")
			if bucketExists {
				return &errors.Error{Code: errors.EConflict, Msg: "bucket with name telegraf already exists"}
			}
			b.ID = newID
			return nil
		}
		buckets.DeleteBucketFn = func(_ context.Context, id platform.ID) error {
			*calls = append(*calls, fmt.Sprintf("delete:%s", id))
			return nil
		}

		return NewRestoreHandler(&RestoreBackend{
			Logger:           zaptest.NewLogger(t),
			HTTPErrorHandler: kithttp.NewErrorHandler(zaptest.NewLogger(t)),
			BucketService:    buckets,
			RestoreService: &restoreServiceMock{
				restoreBucketFn: func(_ context.Context, id platform.ID, dbInfo []byte) (map[uint64]uint64, error) {
					*calls = append(*calls, fmt.Sprintf("restore:%s", id))
					require.NotEmpty(t, dbInfo)
					if restoreErr != nil {
						return nil, restoreErr
					}
					return map[uint64]uint64{10: 20}, nil
				},
			},
		})
	}

	postManifest := func(t *testing.T, h *RestoreHandler, onConflict string) *httptest.ResponseRecorder {
		body, err := json.Marshal(manifest)
		require.NoError(t, err)

		path := restoreBucketMetadataPath
		if onConflict != "" {
			path += "?onConflict=" + onConflict
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", path, bytes.NewReader(body)))
		return w
	}

	decodeMappings := func(t *testing.T, r io.Reader) influxdb.RestoredBucketMappings {
		var mappings influxdb.RestoredBucketMappings
		require.NoError(t, json.NewDecoder(r).Decode(&mappings))
		return mappings
	}

	// "replace" is deliberately not a server-side option: it would have to
	// delete the existing bucket before any shard data is uploaded. The CLI
	// implements replace by restoring to a temporary bucket and swapping.
	for _, onConflict := range []string{"overwrite", "replace"} {
		t.Run(fmt.Sprintf("invalid value %s", onConflict), func(t *testing.T) {
			var calls []string
			w := postManifest(t, newHandler(t, false, nil, &calls), onConflict)

			require.Equal(t, 400, w.Code)
			require.Empty(t, calls)
		})
	}

	t.Run("default errors on existing bucket", func(t *testing.T) {
		var calls []string
		w := postManifest(t, newHandler(t, true, nil, &calls), "")

		require.Equal(t, 422, w.Code)
		require.Equal(t, []string{"create"}, calls)
	})

	t.Run("skip returns existing bucket with no shard mappings", func(t *testing.T) {
		var calls []string
		w := postManifest(t, newHandler(t, true, nil, &calls), "skip")

		require.Equal(t, 200, w.Code)
		mappings := decodeMappings(t, w.Body)
		require.Equal(t, existingID, mappings.ID)
		require.Empty(t, mappings.ShardMappings)
		require.Equal(t, []string{"create", "find"}, calls)
	})

	t.Run("skip restores normally when bucket is missing", func(t *testing.T) {
		var calls []string
		w := postManifest(t, newHandler(t, false, nil, &calls), "skip")

		require.Equal(t, 201, w.Code)
		mappings := decodeMappings(t, w.Body)
		require.Equal(t, newID, mappings.ID)
		require.Equal(t, manifest.BucketName, mappings.Name)
		require.Equal(t, []influxdb.RestoredShardMapping{{OldId: 10, NewId: 20}}, mappings.ShardMappings)
		require.Equal(t, []string{"create", fmt.Sprintf("restore:%s", newID)}, calls)
	})

	t.Run("failed restore cleans up the new bucket", func(t *testing.T) {
		var calls []string
		restoreErr := &errors.Error{Code: errors.EInternal, Msg: "restore blew up"}
		w := postManifest(t, newHandler(t, false, restoreErr, &calls), "skip")

		require.Equal(t, 500, w.Code)
		require.Equal(t, []string{
			"create",
			fmt.Sprintf("restore:%s", newID),
			fmt.Sprintf("delete:%s", newID),
		}, calls)
	})
}