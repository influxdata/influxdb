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
		deleted := false
		buckets.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
			*calls = append(*calls, "create")
			if bucketExists && !deleted {
				return &errors.Error{Code: errors.EConflict, Msg: "bucket with name telegraf already exists"}
			}
			b.ID = newID
			return nil
		}
		buckets.DeleteBucketFn = func(_ context.Context, id platform.ID) error {
			*calls = append(*calls, fmt.Sprintf("delete:%s", id))
			if id == existingID {
				deleted = true
			}
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

	t.Run("invalid value", func(t *testing.T) {
		var calls []string
		w := postManifest(t, newHandler(t, false, nil, &calls), "overwrite")

		require.Equal(t, 400, w.Code)
		require.Empty(t, calls)
	})

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
		require.Equal(t, []string{"find"}, calls)
	})

	t.Run("replace deletes existing bucket then restores the new one", func(t *testing.T) {
		var calls []string
		w := postManifest(t, newHandler(t, true, nil, &calls), "replace")

		require.Equal(t, 201, w.Code)
		mappings := decodeMappings(t, w.Body)
		require.Equal(t, newID, mappings.ID)
		require.Equal(t, manifest.BucketName, mappings.Name)
		require.Equal(t, []influxdb.RestoredShardMapping{{OldId: 10, NewId: 20}}, mappings.ShardMappings)

		// The exact sequence matters: the old bucket must be deleted before the
		// new one is created, and the restore must run against the new bucket.
		require.Equal(t, []string{
			"find",
			fmt.Sprintf("delete:%s", existingID),
			"create",
			fmt.Sprintf("restore:%s", newID),
		}, calls)
	})

	t.Run("replace with failed restore cleans up the new bucket", func(t *testing.T) {
		var calls []string
		restoreErr := &errors.Error{Code: errors.EInternal, Msg: "restore blew up"}
		w := postManifest(t, newHandler(t, true, restoreErr, &calls), "replace")

		require.Equal(t, 500, w.Code)
		// The old bucket is gone and the restore failed; the handler must still
		// have attempted the restore against the new bucket, then deleted it.
		require.Equal(t, []string{
			"find",
			fmt.Sprintf("delete:%s", existingID),
			"create",
			fmt.Sprintf("restore:%s", newID),
			fmt.Sprintf("delete:%s", newID),
		}, calls)
	})

	for _, onConflict := range []string{"skip", "replace"} {
		t.Run(fmt.Sprintf("%s restores normally when bucket is missing", onConflict), func(t *testing.T) {
			var calls []string
			w := postManifest(t, newHandler(t, false, nil, &calls), onConflict)

			require.Equal(t, 201, w.Code)
			mappings := decodeMappings(t, w.Body)
			require.Equal(t, newID, mappings.ID)
			require.Equal(t, []string{"find", "create", fmt.Sprintf("restore:%s", newID)}, calls)
		})
	}
}