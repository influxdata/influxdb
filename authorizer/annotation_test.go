package authorizer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
)

var (
	annOrgID1 = influxdbtesting.IDPtr(1)
	annOrgID2 = influxdbtesting.IDPtr(10)
	rID       = influxdbtesting.IDPtr(2)
)

func Test_CreateAnnotations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantRet []influxdb.AnnotationEvent
		wantErr error
	}{
		{
			"authorized to create annotation(s) with the specified org",
			[]influxdb.AnnotationEvent{{ID: *rID}},
			nil,
		},
		{
			"not authorized to create annotation(s) with the specified org",
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/annotations is unauthorized", annOrgID1),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			var perm influxdb.Permission
			if tt.wantErr == nil {
				perm = newTestAnnotationsPermission(influxdb.WriteAction, annOrgID1)
				svc.EXPECT().
					CreateAnnotations(gomock.Any(), *annOrgID1, []influxdb.AnnotationCreate{{}}).
					Return(tt.wantRet, nil)
			} else {
				perm = newTestAnnotationsPermission(influxdb.ReadAction, annOrgID1)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.CreateAnnotations(ctx, *annOrgID1, []influxdb.AnnotationCreate{{}})
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_ListAnnotations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantRet influxdb.ReadAnnotations
		wantErr error
	}{
		{
			"authorized to list annotations for the specified org",
			influxdb.ReadAnnotations{},
			nil,
		},
		{
			"not authorized to list annotations for the specified org",
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("read:orgs/%s/annotations is unauthorized", annOrgID1),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			var perm influxdb.Permission
			if tt.wantErr == nil {
				perm = newTestAnnotationsPermission(influxdb.ReadAction, annOrgID1)
				svc.EXPECT().
					ListAnnotations(gomock.Any(), *annOrgID1, influxdb.AnnotationListFilter{}).
					Return(tt.wantRet, nil)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.ListAnnotations(ctx, *annOrgID1, influxdb.AnnotationListFilter{})
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_GetAnnotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		permissionOrg *platform.ID
		wantRet       *influxdb.StoredAnnotation
		wantErr       error
	}{
		{
			"authorized to access annotation by id",
			annOrgID1,
			&influxdb.StoredAnnotation{
				ID:    *rID,
				OrgID: *annOrgID1,
			},
			nil,
		},
		{
			"not authorized to access annotation by id",
			annOrgID2,
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("read:orgs/%s/annotations/%s is unauthorized", annOrgID1, rID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			svc.EXPECT().
				GetAnnotation(gomock.Any(), *rID).
				Return(&influxdb.StoredAnnotation{
					ID:    *rID,
					OrgID: *annOrgID1,
				}, nil)

			perm := newTestAnnotationsPermission(influxdb.ReadAction, tt.permissionOrg)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.GetAnnotation(ctx, *rID)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_DeleteAnnotations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr error
	}{
		{
			"authorized to delete annotations with the specified org",
			nil,
		},
		{
			"not authorized to delete annotations with the specified org",
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/annotations is unauthorized", annOrgID1),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			var perm influxdb.Permission
			if tt.wantErr == nil {
				perm = newTestAnnotationsPermission(influxdb.WriteAction, annOrgID1)
				svc.EXPECT().
					DeleteAnnotations(gomock.Any(), *annOrgID1, influxdb.AnnotationDeleteFilter{}).
					Return(nil)
			} else {
				perm = newTestAnnotationsPermission(influxdb.ReadAction, annOrgID1)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			err := s.DeleteAnnotations(ctx, *annOrgID1, influxdb.AnnotationDeleteFilter{})
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func Test_DeleteAnnotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		permissionOrg *platform.ID
		wantErr       error
	}{
		{
			"authorized to delete annotation by id",
			annOrgID1,
			nil,
		},
		{
			"not authorized to delete annotation by id",
			annOrgID2,
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/annotations/%s is unauthorized", annOrgID1, rID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			svc.EXPECT().
				GetAnnotation(gomock.Any(), *rID).
				Return(&influxdb.StoredAnnotation{
					ID:    *rID,
					OrgID: *annOrgID1,
				}, nil)

			perm := newTestAnnotationsPermission(influxdb.WriteAction, tt.permissionOrg)

			if tt.wantErr == nil {
				svc.EXPECT().
					DeleteAnnotation(gomock.Any(), *rID).
					Return(nil)
			}

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			err := s.DeleteAnnotation(ctx, *rID)
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func Test_UpdateAnnotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		permissionOrg *platform.ID
		wantRet       *influxdb.AnnotationEvent
		wantErr       error
	}{
		{
			"authorized to update annotation by id",
			annOrgID1,
			&influxdb.AnnotationEvent{},
			nil,
		},
		{
			"not authorized to update annotation by id",
			annOrgID2,
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/annotations/%s is unauthorized", annOrgID1, rID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			svc.EXPECT().
				GetAnnotation(gomock.Any(), *rID).
				Return(&influxdb.StoredAnnotation{
					ID:    *rID,
					OrgID: *annOrgID1,
				}, nil)

			perm := newTestAnnotationsPermission(influxdb.WriteAction, tt.permissionOrg)

			if tt.wantErr == nil {
				svc.EXPECT().
					UpdateAnnotation(gomock.Any(), *rID, influxdb.AnnotationCreate{}).
					Return(tt.wantRet, nil)
			}

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.UpdateAnnotation(ctx, *rID, influxdb.AnnotationCreate{})
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_ListStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantRet []influxdb.ReadStream
		wantErr error
	}{
		{
			"authorized to list streams for the specified org",
			[]influxdb.ReadStream{},
			nil,
		},
		{
			"not authorized to list streams for the specified org",
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("read:orgs/%s/annotations is unauthorized", annOrgID1),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			var perm influxdb.Permission
			if tt.wantErr == nil {
				perm = newTestAnnotationsPermission(influxdb.ReadAction, annOrgID1)
				svc.EXPECT().
					ListStreams(gomock.Any(), *annOrgID1, influxdb.StreamListFilter{}).
					Return(tt.wantRet, nil)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.ListStreams(ctx, *annOrgID1, influxdb.StreamListFilter{})
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_GetStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		permissionOrg *platform.ID
		wantRet       *influxdb.StoredStream
		wantErr       error
	}{
		{
			"authorized to access stream by id",
			annOrgID1,
			&influxdb.StoredStream{
				ID:    *rID,
				OrgID: *annOrgID1,
			},
			nil,
		},
		{
			"not authorized to access stream by id",
			annOrgID2,
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("read:orgs/%s/annotations/%s is unauthorized", annOrgID1, rID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			svc.EXPECT().
				GetStream(gomock.Any(), *rID).
				Return(&influxdb.StoredStream{
					ID:    *rID,
					OrgID: *annOrgID1,
				}, nil)

			perm := newTestAnnotationsPermission(influxdb.ReadAction, tt.permissionOrg)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.GetStream(ctx, *rID)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func newTestAnnotationsPermission(action influxdb.Action, orgID *platform.ID) influxdb.Permission {
	return influxdb.Permission{
		Action: action,
		Resource: influxdb.Resource{
			Type:  influxdb.AnnotationsResourceType,
			OrgID: orgID,
		},
	}
}

func Test_CreateOrUpdateStream(t *testing.T) {
	t.Parallel()

	var (
		testStreamName = "test stream"
		testStream     = influxdb.Stream{
			Name: testStreamName,
		}
	)

	// this set of tests does not actually test any authorization logic since the
	// implementing code calls other methods, depending on the result of ListStreams.
	// so these tests are to make sure that the branch logic and resulting calls work correctly.
	t.Run("updating a stream", func(t *testing.T) {
		tests := []struct {
			name            string
			permissionOrg   *platform.ID
			existingStreams []influxdb.ReadStream
			getStreamRet    *influxdb.StoredStream
			getStreamErr    error
			wantRet         *influxdb.ReadStream
			wantErr         error
		}{
			{
				"authorized to update an existing stream",
				annOrgID1,
				[]influxdb.ReadStream{{ID: *rID}},
				&influxdb.StoredStream{ID: *rID, OrgID: *annOrgID1},
				nil,
				&influxdb.ReadStream{},
				nil,
			},
			{
				"not authorized to update an existing stream - no read access",
				annOrgID2,
				[]influxdb.ReadStream{{ID: *rID}},
				nil,
				&errors.Error{
					Msg:  fmt.Sprintf("read:orgs/%s/annotations/%s is unauthorized", annOrgID2, rID),
					Code: errors.EUnauthorized,
				},
				nil,
				&errors.Error{
					Msg:  fmt.Sprintf("read:orgs/%s/annotations/%s is unauthorized", annOrgID2, rID),
					Code: errors.EUnauthorized,
				},
			},
			{
				"not authorized to update an existing stream - read access but no write access",
				annOrgID2,
				[]influxdb.ReadStream{{ID: *rID}},
				&influxdb.StoredStream{ID: *rID, OrgID: *annOrgID1},
				nil,
				nil,
				&errors.Error{
					Msg:  fmt.Sprintf("write:orgs/%s/annotations/%s is unauthorized", annOrgID2, rID),
					Code: errors.EUnauthorized,
				},
			},
		}

		// this set of tests will verify the AuthorizeCreate works correctly when
		// creating a new stream
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrlr := gomock.NewController(t)
				svc := mock.NewMockAnnotationService(ctrlr)
				s := authorizer.NewAnnotationService(svc)

				svc.EXPECT().
					ListStreams(gomock.Any(), *tt.permissionOrg, influxdb.StreamListFilter{
						StreamIncludes: []string{testStreamName},
					}).
					Return(tt.existingStreams, nil)

				svc.EXPECT().
					GetStream(gomock.Any(), tt.existingStreams[0].ID).
					Return(tt.getStreamRet, tt.getStreamErr)

				if tt.getStreamErr == nil {
					svc.EXPECT().
						UpdateStream(gomock.Any(), tt.getStreamRet.ID, testStream).
						Return(tt.wantRet, tt.wantErr)
				}

				ctx := context.Background()
				got, err := s.CreateOrUpdateStream(ctx, *tt.permissionOrg, testStream)
				require.Equal(t, tt.wantErr, err)
				require.Equal(t, tt.wantRet, got)
			})
		}
	})

	t.Run("creating a stream", func(t *testing.T) {
		tests := []struct {
			name            string
			existingStreams []influxdb.ReadStream
			wantRet         *influxdb.ReadStream
			wantErr         error
		}{
			{
				"authorized to create a stream with the specified org",
				[]influxdb.ReadStream{},
				&influxdb.ReadStream{},
				nil,
			},
			{
				"not authorized to create a stream with the specified org",
				[]influxdb.ReadStream{},
				nil,
				&errors.Error{
					Msg:  fmt.Sprintf("write:orgs/%s/annotations is unauthorized", annOrgID1),
					Code: errors.EUnauthorized,
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrlr := gomock.NewController(t)
				svc := mock.NewMockAnnotationService(ctrlr)
				s := authorizer.NewAnnotationService(svc)

				svc.EXPECT().
					ListStreams(gomock.Any(), *annOrgID1, influxdb.StreamListFilter{
						StreamIncludes: []string{testStreamName},
					}).
					Return(tt.existingStreams, nil)

				var perm influxdb.Permission
				if tt.wantErr == nil {
					perm = newTestAnnotationsPermission(influxdb.WriteAction, annOrgID1)
					svc.EXPECT().
						CreateOrUpdateStream(gomock.Any(), *annOrgID1, testStream).
						Return(tt.wantRet, nil)
				} else {
					perm = newTestAnnotationsPermission(influxdb.ReadAction, annOrgID1)
				}

				ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
				got, err := s.CreateOrUpdateStream(ctx, *annOrgID1, testStream)
				require.Equal(t, tt.wantErr, err)
				require.Equal(t, tt.wantRet, got)
			})
		}
	})
}

func Test_UpdateStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		permissionOrg *platform.ID
		wantRet       *influxdb.ReadStream
		wantErr       error
	}{
		{
			"authorized to update stream by id",
			annOrgID1,
			&influxdb.ReadStream{},
			nil,
		},
		{
			"not authorized to update stream by id",
			annOrgID2,
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/annotations/%s is unauthorized", annOrgID1, rID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			svc.EXPECT().
				GetStream(gomock.Any(), *rID).
				Return(&influxdb.StoredStream{
					ID:    *rID,
					OrgID: *annOrgID1,
				}, nil)

			perm := newTestAnnotationsPermission(influxdb.WriteAction, tt.permissionOrg)

			if tt.wantErr == nil {
				svc.EXPECT().
					UpdateStream(gomock.Any(), *rID, influxdb.Stream{}).
					Return(tt.wantRet, nil)
			}

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.UpdateStream(ctx, *rID, influxdb.Stream{})
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_DeleteStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr error
	}{
		{
			"authorized to delete streams with the specified org",
			nil,
		},
		{
			"not authorized to delete streams with the specified org",
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/annotations is unauthorized", annOrgID1),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			var perm influxdb.Permission
			if tt.wantErr == nil {
				perm = newTestAnnotationsPermission(influxdb.WriteAction, annOrgID1)
				svc.EXPECT().
					DeleteStreams(gomock.Any(), *annOrgID1, influxdb.BasicStream{}).
					Return(nil)
			} else {
				perm = newTestAnnotationsPermission(influxdb.ReadAction, annOrgID1)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			err := s.DeleteStreams(ctx, *annOrgID1, influxdb.BasicStream{})
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func Test_DeleteStreamByID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		permissionOrg *platform.ID
		wantErr       error
	}{
		{
			"authorized to delete stream by id",
			annOrgID1,
			nil,
		},
		{
			"not authorized to delete stream by id",
			annOrgID2,
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/annotations/%s is unauthorized", annOrgID1, rID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockAnnotationService(ctrlr)
			s := authorizer.NewAnnotationService(svc)

			svc.EXPECT().
				GetStream(gomock.Any(), *rID).
				Return(&influxdb.StoredStream{
					ID:    *rID,
					OrgID: *annOrgID1,
				}, nil)

			perm := newTestAnnotationsPermission(influxdb.WriteAction, tt.permissionOrg)

			if tt.wantErr == nil {
				svc.EXPECT().
					DeleteStreamByID(gomock.Any(), *rID).
					Return(nil)
			}

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			err := s.DeleteStreamByID(ctx, *rID)
			require.Equal(t, tt.wantErr, err)
		})
	}
}
