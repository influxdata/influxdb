package authorizer_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/require"
)

func Test_BackupSqlStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		permList []influxdb.Permission
		wantErr  error
	}{
		{
			"authorized to do the backup",
			influxdb.OperPermissions(),
			nil,
		},
		{
			"not authorized to do the backup",
			influxdb.ReadAllPermissions(),
			&errors.Error{
				Msg:  "write:authorizations is unauthorized",
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockSqlBackupRestoreService(ctrlr)
			s := authorizer.NewSqlBackupRestoreService(svc)

			w := bytes.NewBuffer([]byte{})

			if tt.wantErr == nil {
				svc.EXPECT().
					BackupSqlStore(gomock.Any(), w).
					Return(nil)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, tt.permList))
			err := s.BackupSqlStore(ctx, w)
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func Test_RestoreSqlStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		permList []influxdb.Permission
		wantErr  error
	}{
		{
			"authorized to do the restore",
			influxdb.OperPermissions(),
			nil,
		},
		{
			"not authorized to do the restore",
			influxdb.ReadAllPermissions(),
			&errors.Error{
				Msg:  "write:authorizations is unauthorized",
				Code: errors.EUnauthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrlr := gomock.NewController(t)
			svc := mock.NewMockSqlBackupRestoreService(ctrlr)
			s := authorizer.NewSqlBackupRestoreService(svc)

			w := bytes.NewBuffer([]byte{})

			if tt.wantErr == nil {
				svc.EXPECT().
					RestoreSqlStore(gomock.Any(), w).
					Return(nil)
			}

			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, tt.permList))
			err := s.RestoreSqlStore(ctx, w)
			require.Equal(t, tt.wantErr, err)
		})
	}
}
