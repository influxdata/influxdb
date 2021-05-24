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
	notebookSvc "github.com/influxdata/influxdb/v2/notebooks/service"
	notebookMocks "github.com/influxdata/influxdb/v2/notebooks/service/mocks"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
)

var (
	orgID1 = influxdbtesting.IDPtr(1)
	orgID2 = influxdbtesting.IDPtr(10)
	nbID   = influxdbtesting.IDPtr(2)
)

func Test_GetNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		notebookOrg   *platform.ID
		permissionOrg *platform.ID
		wantRet       *notebookSvc.Notebook
		wantErr       error
	}{
		{
			"authorized to access notebook by id",
			orgID1,
			orgID1,
			newTestNotebook(*orgID1),
			nil,
		},
		{
			"not authorized to access notebook by id",
			orgID1,
			orgID2,
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("read:orgs/%s/notebooks/%s is unauthorized", orgID1, nbID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := notebookMocks.NewMockNotebookService(ctrlr)
			s := authorizer.NewNotebookService(svc)

			svc.EXPECT().
				GetNotebook(gomock.Any(), *nbID).
				Return(newTestNotebook(*orgID1), nil)

			perm := newTestPermission(influxdb.ReadAction, tt.permissionOrg)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.GetNotebook(ctx, *nbID)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_CreateNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		notebookOrg   *platform.ID
		permissionOrg *platform.ID
		wantRet       *notebookSvc.Notebook
		wantErr       error
	}{
		{
			"authorized to create a notebook with the given org",
			orgID1,
			orgID1,
			newTestNotebook(*orgID1),
			nil,
		},
		{
			"not authorized to create a notebook with the given org",
			orgID1,
			orgID2,
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/notebooks is unauthorized", orgID1),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := notebookMocks.NewMockNotebookService(ctrlr)
			s := authorizer.NewNotebookService(svc)

			perm := newTestPermission(influxdb.WriteAction, tt.permissionOrg)
			nb := newTestReqBody(*tt.notebookOrg)

			if tt.wantErr == nil {
				svc.EXPECT().
					CreateNotebook(gomock.Any(), nb).
					Return(tt.wantRet, nil)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.CreateNotebook(ctx, nb)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_UpdateNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		notebookOrg   *platform.ID
		permissionOrg *platform.ID
		wantRet       *notebookSvc.Notebook
		wantErr       error
	}{
		{
			"authorized to update notebook by id",
			orgID1,
			orgID1,
			newTestNotebook(*orgID1),
			nil,
		},
		{
			"not authorized to update notebook by id",
			orgID1,
			orgID2,
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/notebooks/%s is unauthorized", orgID1, nbID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := notebookMocks.NewMockNotebookService(ctrlr)
			s := authorizer.NewNotebookService(svc)

			svc.EXPECT().
				GetNotebook(gomock.Any(), *nbID).
				Return(newTestNotebook(*tt.notebookOrg), nil)

			perm := newTestPermission(influxdb.WriteAction, tt.permissionOrg)
			nb := newTestReqBody(*tt.notebookOrg)

			if tt.wantErr == nil {
				svc.EXPECT().
					UpdateNotebook(gomock.Any(), *nbID, nb).
					Return(tt.wantRet, nil)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.UpdateNotebook(ctx, *nbID, nb)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func Test_DeleteNotebook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		notebookOrg   *platform.ID
		permissionOrg *platform.ID
		wantErr       error
	}{
		{
			"authorized to delete notebook by id",
			orgID1,
			orgID1,
			nil,
		},
		{
			"not authorized to delete notebook by id",
			orgID1,
			orgID2,
			&errors.Error{
				Msg:  fmt.Sprintf("write:orgs/%s/notebooks/%s is unauthorized", orgID1, nbID),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := notebookMocks.NewMockNotebookService(ctrlr)
			s := authorizer.NewNotebookService(svc)

			svc.EXPECT().
				GetNotebook(gomock.Any(), *nbID).
				Return(newTestNotebook(*tt.notebookOrg), nil)

			perm := newTestPermission(influxdb.WriteAction, tt.permissionOrg)

			if tt.wantErr == nil {
				svc.EXPECT().
					DeleteNotebook(gomock.Any(), *nbID).
					Return(nil)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got := s.DeleteNotebook(ctx, *nbID)
			require.Equal(t, tt.wantErr, got)
		})
	}
}

func Test_ListNotebooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		notebookOrg   *platform.ID
		permissionOrg *platform.ID
		wantRet       []*notebookSvc.Notebook
		wantErr       error
	}{
		{
			"authorized to list notebooks for the specified org",
			orgID1,
			orgID1,
			[]*notebookSvc.Notebook{},
			nil,
		},
		{
			"not authorized to list notebooks for the specified org",
			orgID1,
			orgID2,
			nil,
			&errors.Error{
				Msg:  fmt.Sprintf("read:orgs/%s/notebooks is unauthorized", orgID1),
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := notebookMocks.NewMockNotebookService(ctrlr)
			s := authorizer.NewNotebookService(svc)

			perm := newTestPermission(influxdb.ReadAction, tt.permissionOrg)
			filter := notebookSvc.NotebookListFilter{OrgID: *tt.notebookOrg}

			if tt.wantErr == nil {
				svc.EXPECT().
					ListNotebooks(gomock.Any(), filter).
					Return(tt.wantRet, nil)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{perm}))
			got, err := s.ListNotebooks(ctx, filter)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.wantRet, got)
		})
	}
}

func newTestNotebook(orgID platform.ID) *notebookSvc.Notebook {
	return &notebookSvc.Notebook{
		OrgID: orgID,
		ID:    *nbID,
		Name:  "test notebook",
		Spec: notebookSvc.NotebookSpec{
			"hello": "goodbye",
		},
	}
}

func newTestReqBody(orgID platform.ID) *notebookSvc.NotebookReqBody {
	return &notebookSvc.NotebookReqBody{
		OrgID: orgID,
		Name:  "testing",
		Spec: notebookSvc.NotebookSpec{
			"hello": "goodbye",
		},
	}
}

func newTestPermission(action influxdb.Action, orgID *platform.ID) influxdb.Permission {
	return influxdb.Permission{
		Action: action,
		Resource: influxdb.Resource{
			Type:  influxdb.NotebooksResourceType,
			OrgID: orgID,
		},
	}
}
