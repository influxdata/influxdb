package service

import (
	"testing"

	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/stretchr/testify/require"
)

func TestValidatePage(t *testing.T) {
	tests := []struct {
		name        string
		page        Page
		expectError error
	}{
		{
			name: "ok",
			page: Page{
				Offset: 5,
				Limit:  10,
			},
			expectError: nil,
		},
		{
			name: "negative offset",
			page: Page{
				Offset: -5,
				Limit:  10,
			},
			expectError: ErrOffsetNegative,
		},
		{
			name: "negative limit",
			page: Page{
				Offset: 5,
				Limit:  -10,
			},
			expectError: ErrLimitLTEZero,
		},
		{
			name: "zero limit",
			page: Page{
				Offset: 5,
				Limit:  0,
			},
			expectError: ErrLimitLTEZero,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.page.Validate()
			if tt.expectError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, tt.expectError, err)
			}
		})
	}
}

func TestValidateCreate(t *testing.T) {
	tests := []struct {
		name        string
		create      NotebookCreate
		expectError error
	}{
		{
			name: "ok",
			create: NotebookCreate{
				Name:  "Example",
				OrgID: snowflake.NewIDGenerator().ID(),
			},
			expectError: nil,
		},
		{
			name: "missing name",
			create: NotebookCreate{
				OrgID: snowflake.NewIDGenerator().ID(),
			},
			expectError: ErrNameRequired,
		},
		{
			name: "missing org ID",
			create: NotebookCreate{
				Name: "Example",
			},
			expectError: ErrOrgIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.create.Validate()
			if tt.expectError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, tt.expectError, err)
			}
		})
	}
}

func TestValidateUpdate(t *testing.T) {
	tests := []struct {
		name        string
		update      NotebookUpdate
		expectError error
	}{
		{
			name: "ok",
			update: NotebookUpdate{
				Name: "Example",
				Spec: map[string]interface{}{},
			},
			expectError: nil,
		},
		{
			name: "missing name",
			update: NotebookUpdate{
				Spec: map[string]interface{}{},
			},
			expectError: ErrNameRequired,
		},
		{
			name: "missing spec",
			update: NotebookUpdate{
				Name: "Example",
			},
			expectError: ErrSpecRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.update.Validate()
			if tt.expectError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, tt.expectError, err)
			}
		})
	}
}
