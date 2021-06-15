// +build sqlite_json
// +build sqlite_foreign_keys

package annotations

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/sqlite/migrations"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	idGen = snowflake.NewIDGenerator()
)

func TestAnnotationsCRUD(t *testing.T) {
	t.Parallel()

	// intialize some variables that can be shared across tests
	// the timeline for the 3 test annotations start & end times is visualized below.
	//                 now
	//                  v
	//  |---|---|---|---|
	//  ^   ^   ^   ^   ^
	//          st1    et1
	//      st2    et2
	//  st3     et3

	et1 := time.Now().UTC()
	st1 := et1.Add(-10 * time.Minute)

	et2 := et1.Add(-5 * time.Minute)
	st2 := et2.Add(-10 * time.Minute)

	et3 := et1.Add(-10 * time.Minute)
	st3 := et2.Add(-15 * time.Minute)

	// used for tests involving time filters
	earlierEt1 := et1.Add(-1 * time.Millisecond)
	laterSt3 := st3.Add(1 * time.Millisecond)
	beforeAny := st3.Add(-1 * time.Millisecond)
	afterAny := et1.Add(1 * time.Millisecond)

	orgID := *influxdbtesting.IDPtr(1)
	otherOrgID := *influxdbtesting.IDPtr(2)
	ctx := context.Background()

	s1 := influxdb.StoredAnnotation{
		OrgID:     orgID,
		StreamTag: "stream1",
		Summary:   "summary1",
		Message:   "message1",
		Stickers:  map[string]string{"stick1": "val1", "stick2": "val2"},
		Duration:  timesToDuration(st1, et1),
		Lower:     st1.Format(time.RFC3339Nano),
		Upper:     et1.Format(time.RFC3339Nano),
	}

	c1, err := s1.ToCreate()
	require.NoError(t, err)

	s2 := influxdb.StoredAnnotation{
		OrgID:     orgID,
		StreamTag: "stream2",
		Summary:   "summary2",
		Message:   "message2",
		Stickers:  map[string]string{"stick2": "val2", "stick3": "val3", "stick4": "val4"},
		Duration:  timesToDuration(st2, et2),
		Lower:     st2.Format(time.RFC3339Nano),
		Upper:     et2.Format(time.RFC3339Nano),
	}

	c2, err := s2.ToCreate()
	require.NoError(t, err)

	s3 := influxdb.StoredAnnotation{
		OrgID:     orgID,
		StreamTag: "stream2",
		Summary:   "summary3",
		Message:   "message3",
		Stickers:  map[string]string{"stick1": "val2"},
		Duration:  timesToDuration(st3, et3),
		Lower:     st3.Format(time.RFC3339Nano),
		Upper:     et3.Format(time.RFC3339Nano),
	}

	c3, err := s3.ToCreate()
	require.NoError(t, err)

	// helper function for setting up the database with data that can be used for tests
	// that involve querying the database. uses the annotations objects initialized above
	// via the closure.
	populateAnnotationsData := func(t *testing.T, svc *Service) []influxdb.AnnotationEvent {
		t.Helper()

		got, err := svc.CreateAnnotations(ctx, orgID, []influxdb.AnnotationCreate{*c1, *c2, *c3})
		require.NoError(t, err)
		assertAnnotationEvents(t, got, []influxdb.AnnotationEvent{
			{AnnotationCreate: *c1},
			{AnnotationCreate: *c2},
			{AnnotationCreate: *c3},
		})

		return got
	}

	t.Run("create annotations", func(t *testing.T) {
		svc, clean := newTestService(t)
		defer clean(t)

		tests := []struct {
			name    string
			creates []influxdb.AnnotationCreate
			want    []influxdb.AnnotationEvent
		}{
			{
				"empty creates list returns empty events list",
				[]influxdb.AnnotationCreate{},
				[]influxdb.AnnotationEvent{},
			},
			{
				"creates annotations successfully",
				[]influxdb.AnnotationCreate{*c1, *c2, *c3},
				[]influxdb.AnnotationEvent{
					{AnnotationCreate: *c1},
					{AnnotationCreate: *c2},
					{AnnotationCreate: *c3},
				},
			},
		}

		for _, tt := range tests {
			got, err := svc.CreateAnnotations(ctx, orgID, tt.creates)
			require.NoError(t, err)
			assertAnnotationEvents(t, got, tt.want)
		}
	})

	t.Run("select with filters", func(t *testing.T) {
		svc, clean := newTestService(t)
		defer clean(t)
		populateAnnotationsData(t, svc)

		tests := []struct {
			name  string
			orgID platform.ID
			f     influxdb.AnnotationListFilter
			want  []influxdb.StoredAnnotation
		}{
			{
				"time filter is inclusive - gets all",
				orgID,
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &st3,
						EndTime:   &et1,
					},
				},
				[]influxdb.StoredAnnotation{s1, s2, s3},
			},
			{
				"doesn't get results for other org",
				otherOrgID,
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &st3,
						EndTime:   &et1,
					},
				},
				[]influxdb.StoredAnnotation{},
			},
			{
				"end time will filter out annotations",
				orgID,
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &st3,
						EndTime:   &earlierEt1,
					},
				},
				[]influxdb.StoredAnnotation{s2, s3},
			},
			{
				"start time will filter out annotations",
				orgID,
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &laterSt3,
						EndTime:   &et1,
					},
				},
				[]influxdb.StoredAnnotation{s1, s2},
			},
			{
				"time can filter out all annotations if it's too soon",
				orgID,
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &beforeAny,
						EndTime:   &beforeAny,
					},
				},
				[]influxdb.StoredAnnotation{},
			},
			{
				"time can filter out all annotations if it's too late",
				orgID,
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &afterAny,
						EndTime:   &afterAny,
					},
				},
				[]influxdb.StoredAnnotation{},
			},
			{
				"time can filter out all annotations if it's too narrow",
				orgID,
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &laterSt3,
						EndTime:   &et3,
					},
				},
				[]influxdb.StoredAnnotation{},
			},
			{
				"can filter by stickers - one sticker matches one",
				orgID,
				influxdb.AnnotationListFilter{
					StickerIncludes: map[string]string{"stick1": "val2"},
				},
				[]influxdb.StoredAnnotation{s3},
			},
			{
				"can filter by stickers - one sticker matches multiple",
				orgID,
				influxdb.AnnotationListFilter{
					StickerIncludes: map[string]string{"stick2": "val2"},
				},
				[]influxdb.StoredAnnotation{s1, s2},
			},
			{
				"can filter by stickers - matching key but wrong value",
				orgID,
				influxdb.AnnotationListFilter{
					StickerIncludes: map[string]string{"stick2": "val3"},
				},
				[]influxdb.StoredAnnotation{},
			},
			{
				"can filter by stream - matches one",
				orgID,
				influxdb.AnnotationListFilter{
					StreamIncludes: []string{"stream1"},
				},
				[]influxdb.StoredAnnotation{s1},
			},
			{
				"can filter by stream - matches multiple",
				orgID,
				influxdb.AnnotationListFilter{
					StreamIncludes: []string{"stream2"},
				},
				[]influxdb.StoredAnnotation{s2, s3},
			},
			{
				"can filter by stream - no match",
				orgID,
				influxdb.AnnotationListFilter{
					StreamIncludes: []string{"badStream"},
				},
				[]influxdb.StoredAnnotation{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.f.Validate(time.Now)
				got, err := svc.ListAnnotations(ctx, tt.orgID, tt.f)
				require.NoError(t, err)
				assertStoredAnnotations(t, got, tt.want)
			})
		}
	})

	t.Run("get by id", func(t *testing.T) {
		svc, clean := newTestService(t)
		defer clean(t)
		anns := populateAnnotationsData(t, svc)

		tests := []struct {
			name    string
			id      platform.ID
			want    *influxdb.AnnotationEvent
			wantErr error
		}{
			{
				"gets the first one by id",
				anns[0].ID,
				&anns[0],
				nil,
			},
			{
				"gets the second one by id",
				anns[1].ID,
				&anns[1],
				nil,
			},
			{
				"has the correct error if not found",
				idGen.ID(),
				nil,
				errAnnotationNotFound,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := svc.GetAnnotation(ctx, tt.id)
				require.Equal(t, tt.wantErr, err)

				if tt.want == nil {
					require.Nil(t, got)
				} else {
					e, err := got.ToEvent()
					require.NoError(t, err)
					require.Equal(t, tt.want, e)
				}
			})
		}
	})

	t.Run("delete multiple with a filter", func(t *testing.T) {
		t.Run("delete by stream id", func(t *testing.T) {
			svc, clean := newTestService(t)
			defer clean(t)
			populateAnnotationsData(t, svc)

			ctx := context.Background()

			lf := influxdb.AnnotationListFilter{BasicFilter: influxdb.BasicFilter{}}
			lf.Validate(time.Now)
			ans, err := svc.ListAnnotations(ctx, orgID, lf)
			require.NoError(t, err)

			annID1 := ans[0].ID
			streamID1 := ans[0].StreamID
			st1, err := time.Parse(time.RFC3339Nano, ans[0].Lower)
			require.NoError(t, err)
			et1, err := time.Parse(time.RFC3339Nano, ans[0].Upper)
			require.NoError(t, err)

			streamID2 := ans[1].StreamID
			st2, err := time.Parse(time.RFC3339Nano, ans[1].Lower)
			require.NoError(t, err)
			et2, err := time.Parse(time.RFC3339Nano, ans[1].Upper)
			require.NoError(t, err)

			tests := []struct {
				name         string
				deleteOrgID  platform.ID
				id           platform.ID
				filter       influxdb.AnnotationDeleteFilter
				shouldDelete bool
			}{
				{
					"matches stream id but not time range",
					orgID,
					annID1,
					influxdb.AnnotationDeleteFilter{
						StreamID:  streamID1,
						StartTime: &st2,
						EndTime:   &et2,
					},
					false,
				},
				{
					"matches time range but not stream id",
					orgID,
					annID1,
					influxdb.AnnotationDeleteFilter{
						StreamID:  streamID2,
						StartTime: &st1,
						EndTime:   &et1,
					},
					false,
				},
				{
					"doesn't delete for other org",
					otherOrgID,
					annID1,
					influxdb.AnnotationDeleteFilter{
						StreamID:  streamID1,
						StartTime: &st1,
						EndTime:   &et1,
					},
					false,
				},
				{
					"matches stream id and time range",
					orgID,
					annID1,
					influxdb.AnnotationDeleteFilter{
						StreamID:  streamID1,
						StartTime: &st1,
						EndTime:   &et1,
					},
					true,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					err := svc.DeleteAnnotations(ctx, tt.deleteOrgID, tt.filter)
					require.NoError(t, err)

					lf := influxdb.AnnotationListFilter{BasicFilter: influxdb.BasicFilter{}}
					lf.Validate(time.Now)
					list, err := svc.ListAnnotations(ctx, orgID, lf)
					require.NoError(t, err)
					get, getErr := svc.GetAnnotation(ctx, tt.id)

					if tt.shouldDelete {
						require.Equal(t, 2, len(list))
						require.Nil(t, get)
						require.Equal(t, errAnnotationNotFound, getErr)
					} else {
						require.Equal(t, 3, len(list))
						require.NoError(t, getErr)
						require.Equal(t, *get, ans[0])
					}
				})
			}
		})

		t.Run("delete with non-id filters", func(t *testing.T) {
			svc, clean := newTestService(t)
			defer clean(t)
			populateAnnotationsData(t, svc)

			tests := []struct {
				name        string
				deleteOrgID platform.ID
				filter      influxdb.AnnotationDeleteFilter
				wantList    []influxdb.StoredAnnotation
			}{
				{
					"matches stream tag but not time range",
					orgID,
					influxdb.AnnotationDeleteFilter{
						StreamTag: "stream1",
						StartTime: &st1,
						EndTime:   &earlierEt1,
					},
					[]influxdb.StoredAnnotation{s1, s2, s3},
				},
				{
					"matches stream tag and time range",
					orgID,
					influxdb.AnnotationDeleteFilter{
						StreamTag: "stream1",
						StartTime: &st1,
						EndTime:   &et1,
					},
					[]influxdb.StoredAnnotation{s2, s3},
				},
				{
					"matches stream tag for multiple",
					orgID,
					influxdb.AnnotationDeleteFilter{
						StreamTag: "stream2",
						StartTime: &st3,
						EndTime:   &et1,
					},
					[]influxdb.StoredAnnotation{s1},
				},
				{
					"matches stream tag but wrong org",
					otherOrgID,
					influxdb.AnnotationDeleteFilter{
						StreamTag: "stream1",
						StartTime: &st1,
						EndTime:   &et1,
					},
					[]influxdb.StoredAnnotation{s1, s2, s3},
				},

				{
					"matches stickers but not time range",
					orgID,
					influxdb.AnnotationDeleteFilter{
						Stickers:  map[string]string{"stick1": "val1"},
						StartTime: &st1,
						EndTime:   &earlierEt1,
					},
					[]influxdb.StoredAnnotation{s1, s2, s3},
				},
				{
					"matches stickers and time range",
					orgID,
					influxdb.AnnotationDeleteFilter{
						Stickers:  map[string]string{"stick1": "val1"},
						StartTime: &st1,
						EndTime:   &et1,
					},
					[]influxdb.StoredAnnotation{s2, s3},
				},
				{
					"matches stickers for multiple",
					orgID,
					influxdb.AnnotationDeleteFilter{
						Stickers:  map[string]string{"stick2": "val2"},
						StartTime: &st2,
						EndTime:   &et1,
					},
					[]influxdb.StoredAnnotation{s3},
				},
				{
					"matches stickers but wrong org",
					otherOrgID,
					influxdb.AnnotationDeleteFilter{
						Stickers:  map[string]string{"stick1": "val1"},
						StartTime: &st1,
						EndTime:   &et1,
					},
					[]influxdb.StoredAnnotation{s1, s2, s3},
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					svc, clean := newTestService(t)
					defer clean(t)
					populateAnnotationsData(t, svc)

					err := svc.DeleteAnnotations(ctx, tt.deleteOrgID, tt.filter)
					require.NoError(t, err)

					f := influxdb.AnnotationListFilter{}
					f.Validate(time.Now)
					list, err := svc.ListAnnotations(ctx, orgID, f)
					require.NoError(t, err)
					assertStoredAnnotations(t, tt.wantList, list)
				})
			}
		})
	})

	t.Run("delete a single annotation by id", func(t *testing.T) {
		svc, clean := newTestService(t)
		defer clean(t)
		ans := populateAnnotationsData(t, svc)

		tests := []struct {
			name         string
			id           platform.ID
			shouldDelete bool
		}{
			{
				"has the correct error if not found",
				idGen.ID(),
				false,
			},
			{
				"deletes the first one by id",
				ans[0].ID,
				true,
			},
			{
				"deletes the second one by id",
				ans[1].ID,
				true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := svc.DeleteAnnotation(ctx, tt.id)

				if tt.shouldDelete {
					require.NoError(t, err)
				} else {
					require.Equal(t, errAnnotationNotFound, err)
				}

				got, err := svc.GetAnnotation(ctx, tt.id)
				require.Equal(t, errAnnotationNotFound, err)
				require.Nil(t, got)
			})
		}
	})

	t.Run("update a single annotation by id", func(t *testing.T) {
		svc, clean := newTestService(t)
		defer clean(t)
		ans := populateAnnotationsData(t, svc)

		updatedTime := time.Time{}.Add(time.Minute)

		tests := []struct {
			name    string
			id      platform.ID
			update  influxdb.AnnotationCreate
			wantErr error
		}{
			{
				"has the correct error if not found",
				idGen.ID(),
				influxdb.AnnotationCreate{
					StreamTag: "updated tag",
					Summary:   "updated summary",
					Message:   "updated message",
					Stickers:  map[string]string{"updated": "sticker"},
					EndTime:   &updatedTime,
					StartTime: &updatedTime,
				},
				errAnnotationNotFound,
			},
			{
				"updates the first one by id",
				ans[0].ID,
				influxdb.AnnotationCreate{
					StreamTag: "updated tag",
					Summary:   "updated summary",
					Message:   "updated message",
					Stickers:  map[string]string{"updated": "sticker"},
					EndTime:   &updatedTime,
					StartTime: &updatedTime,
				},
				nil,
			},
			{
				"updates the second one by id",
				ans[1].ID,
				influxdb.AnnotationCreate{
					StreamTag: "updated tag2",
					Summary:   "updated summary2",
					Message:   "updated message2",
					Stickers:  map[string]string{"updated2": "sticker2"},
					EndTime:   &updatedTime,
					StartTime: &updatedTime,
				},
				nil,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				want := &influxdb.AnnotationEvent{ID: tt.id, AnnotationCreate: tt.update}
				if tt.wantErr != nil {
					want = nil
				}

				got, err := svc.UpdateAnnotation(ctx, tt.id, tt.update)
				require.Equal(t, tt.wantErr, err)
				require.Equal(t, want, got)

				if tt.wantErr == nil {
					new, err := svc.GetAnnotation(ctx, tt.id)
					require.NoError(t, err)
					e, err := new.ToEvent()
					require.NoError(t, err)
					require.Equal(t, got, e)
				}
			})
		}
	})

	t.Run("deleted streams cascade to deleted annotations", func(t *testing.T) {
		svc, clean := newTestService(t)
		defer clean(t)

		ctx := context.Background()
		ans := populateAnnotationsData(t, svc)
		sort.Slice(ans, func(i, j int) bool {
			return ans[i].StreamTag < ans[j].StreamTag
		})

		// annotations s2 and s3 have the stream tag of "stream2", so get the id of that stream
		id := ans[1].ID
		a, err := svc.GetAnnotation(ctx, id)
		require.NoError(t, err)
		streamID := a.StreamID

		// delete the stream
		err = svc.DeleteStreamByID(ctx, streamID)
		require.NoError(t, err)

		// s1 should still be there
		s1, err := svc.GetAnnotation(ctx, ans[0].ID)
		require.NoError(t, err)

		// both s2 and s3 should now be deleted
		f := influxdb.AnnotationListFilter{}
		f.Validate(time.Now)
		remaining, err := svc.ListAnnotations(ctx, orgID, f)
		require.NoError(t, err)
		require.Equal(t, []influxdb.StoredAnnotation{*s1}, remaining)
	})

	t.Run("renamed streams are reflected in subsequent annotation queries", func(t *testing.T) {
		svc, clean := newTestService(t)
		defer clean(t)

		ctx := context.Background()
		populateAnnotationsData(t, svc)

		// get all the annotations with the tag "stream2"
		f := influxdb.AnnotationListFilter{StreamIncludes: []string{"stream2"}}
		f.Validate(time.Now)
		originalList, err := svc.ListAnnotations(ctx, orgID, f)
		require.NoError(t, err)
		assertStoredAnnotations(t, []influxdb.StoredAnnotation{s2, s3}, originalList)

		// check that the original list has the right stream tag for all annotations
		for _, a := range originalList {
			require.Equal(t, "stream2", a.StreamTag)
		}

		// update the name for stream2
		streamID := originalList[0].StreamID
		_, err = svc.UpdateStream(ctx, streamID, influxdb.Stream{Name: "new name", Description: "new desc"})
		require.NoError(t, err)

		// get all the annotations with the new tag
		f = influxdb.AnnotationListFilter{StreamIncludes: []string{"new name"}}
		f.Validate(time.Now)
		newList, err := svc.ListAnnotations(ctx, orgID, f)
		require.NoError(t, err)

		// check that the new list has the right stream tag for all annotations
		for _, a := range newList {
			require.Equal(t, "new name", a.StreamTag)
		}

		// verify that the new list of annotations is the same as the original except for the stream name change
		require.Equal(t, len(originalList), len(newList))

		sort.Slice(originalList, func(i, j int) bool {
			return originalList[i].ID < originalList[j].ID
		})

		sort.Slice(newList, func(i, j int) bool {
			return originalList[i].ID < originalList[j].ID
		})

		for i := range newList {
			originalList[i].StreamTag = "new name"
			require.Equal(t, originalList[i], newList[i])
		}
	})
}

func TestStreamsCRUDSingle(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)

	ctx := context.Background()
	orgID := *influxdbtesting.IDPtr(1)

	stream := influxdb.Stream{
		Name:        "testName",
		Description: "original description",
	}

	var err error
	var s1, s2, s3 *influxdb.ReadStream

	t.Run("create a single stream", func(t *testing.T) {
		s1, err = svc.CreateOrUpdateStream(ctx, orgID, stream)
		require.NoError(t, err)
		require.Equal(t, stream.Name, s1.Name)
		require.Equal(t, stream.Description, s1.Description)
	})

	t.Run("stream updates", func(t *testing.T) {
		u1 := influxdb.Stream{
			Name:        "testName",
			Description: "updated description",
		}

		u2 := influxdb.Stream{
			Name:        "otherName",
			Description: "other description",
		}

		t.Run("updating an existing stream with CreateOrUpdateStream does not change id but does change description", func(t *testing.T) {
			s2, err = svc.CreateOrUpdateStream(ctx, orgID, u1)
			require.NoError(t, err)
			require.Equal(t, stream.Name, s2.Name)
			require.Equal(t, u1.Description, s2.Description)
			require.Equal(t, s1.ID, s2.ID)
		})

		t.Run("updating a non-existant stream with UpdateStream returns not found error", func(t *testing.T) {
			readGot, err := svc.UpdateStream(ctx, idGen.ID(), u2)
			require.Nil(t, readGot)
			require.Equal(t, errStreamNotFound, err)
		})

		t.Run("updating an existing stream with UpdateStream changes both name & description", func(t *testing.T) {
			s3, err = svc.UpdateStream(ctx, s2.ID, u2)
			require.NoError(t, err)
			require.Equal(t, s2.ID, s3.ID)
			require.Equal(t, u2.Name, s3.Name)
			require.Equal(t, u2.Description, s3.Description)
		})
	})

	t.Run("getting a stream", func(t *testing.T) {
		t.Run("non-existant stream returns a not found error", func(t *testing.T) {
			storedGot, err := svc.GetStream(ctx, idGen.ID())
			require.Nil(t, storedGot)
			require.Equal(t, errStreamNotFound, err)
		})

		t.Run("existing stream returns without error", func(t *testing.T) {
			storedGot, err := svc.GetStream(ctx, s3.ID)
			require.NoError(t, err)
			require.Equal(t, s3.Name, storedGot.Name)
			require.Equal(t, s3.Description, storedGot.Description)
		})
	})

	t.Run("deleting a stream", func(t *testing.T) {
		t.Run("non-existant stream returns a not found error", func(t *testing.T) {
			err := svc.DeleteStreamByID(ctx, idGen.ID())
			require.Equal(t, errStreamNotFound, err)
		})

		t.Run("deletes an existing stream without error", func(t *testing.T) {
			err := svc.DeleteStreamByID(ctx, s1.ID)
			require.NoError(t, err)

			storedGot, err := svc.GetStream(ctx, s1.ID)
			require.Nil(t, storedGot)
			require.Equal(t, err, errStreamNotFound)
		})
	})
}

func TestStreamsCRUDMany(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)

	ctx := context.Background()

	orgID1 := influxdbtesting.IDPtr(1)
	orgID2 := influxdbtesting.IDPtr(2)
	orgID3 := influxdbtesting.IDPtr(3)

	// populate the database with some streams for testing delete and select many
	combos := map[platform.ID][]string{
		*orgID1: {"org1_s1", "org1_s2", "org1_s3", "org1_s4"},
		*orgID2: {"org2_s1"},
		*orgID3: {"org3_s1", "org3_s2"},
	}

	for orgID, streams := range combos {
		for _, s := range streams {
			_, err := svc.CreateOrUpdateStream(ctx, orgID, influxdb.Stream{
				Name: s,
			})
			require.NoError(t, err)
		}
	}

	t.Run("all streams can be listed for each org if passing an empty list", func(t *testing.T) {
		for orgID, streams := range combos {
			got, err := svc.ListStreams(ctx, orgID, influxdb.StreamListFilter{
				StreamIncludes: []string{},
			})
			require.NoError(t, err)
			assertStreamNames(t, streams, got)
		}
	})

	t.Run("can select specific streams and get only those for that org", func(t *testing.T) {
		for orgID, streams := range combos {
			got, err := svc.ListStreams(ctx, orgID, influxdb.StreamListFilter{
				StreamIncludes: streams,
			})
			require.NoError(t, err)
			assertStreamNames(t, streams, got)
		}
	})

	t.Run("can delete a single stream with DeleteStreams, but does not delete streams for other org", func(t *testing.T) {
		err := svc.DeleteStreams(ctx, *orgID1, influxdb.BasicStream{
			Names: []string{"org1_s1", "org2_s1"},
		})
		require.NoError(t, err)

		got, err := svc.ListStreams(ctx, *orgID1, influxdb.StreamListFilter{
			StreamIncludes: []string{},
		})
		require.NoError(t, err)
		assertStreamNames(t, []string{"org1_s2", "org1_s3", "org1_s4"}, got)

		got, err = svc.ListStreams(ctx, *orgID2, influxdb.StreamListFilter{
			StreamIncludes: []string{},
		})
		require.NoError(t, err)
		assertStreamNames(t, []string{"org2_s1"}, got)
	})

	t.Run("can delete all streams for all orgs", func(t *testing.T) {
		for orgID, streams := range combos {
			err := svc.DeleteStreams(ctx, orgID, influxdb.BasicStream{
				Names: streams,
			})
			require.NoError(t, err)

			got, err := svc.ListStreams(ctx, orgID, influxdb.StreamListFilter{
				StreamIncludes: []string{},
			})
			require.NoError(t, err)
			require.Equal(t, []influxdb.StoredStream{}, got)
		}
	})
}

func assertAnnotationEvents(t *testing.T, got, want []influxdb.AnnotationEvent) {
	t.Helper()

	require.Equal(t, len(want), len(got))

	sort.Slice(want, func(i, j int) bool {
		return want[i].StreamTag < want[j].StreamTag
	})

	sort.Slice(got, func(i, j int) bool {
		return got[i].StreamTag < got[j].StreamTag
	})

	for idx, w := range want {
		w.ID = got[idx].ID
		require.Equal(t, w, got[idx])
	}
}

// should make these are lists similar
func assertStoredAnnotations(t *testing.T, got, want []influxdb.StoredAnnotation) {
	t.Helper()

	require.Equal(t, len(want), len(got))

	sort.Slice(want, func(i, j int) bool {
		return want[i].ID < want[j].ID
	})

	sort.Slice(got, func(i, j int) bool {
		return got[i].ID < got[j].ID
	})

	for idx, w := range want {
		w.ID = got[idx].ID
		w.StreamID = got[idx].StreamID
		require.Equal(t, w, got[idx])
	}
}

func assertStreamNames(t *testing.T, want []string, got []influxdb.StoredStream) {
	t.Helper()

	storedNames := make([]string, len(got))
	for i, s := range got {
		storedNames[i] = s.Name
	}

	require.ElementsMatch(t, want, storedNames)
}

func newTestService(t *testing.T) (*Service, func(t *testing.T)) {
	t.Helper()

	store, clean := sqlite.NewTestStore(t)
	ctx := context.Background()

	sqliteMigrator := sqlite.NewMigrator(store, zap.NewNop())
	err := sqliteMigrator.Up(ctx, migrations.All)
	require.NoError(t, err)

	svc := NewService(zap.NewNop(), store)

	return svc, clean
}
