package service

import (
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
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

func TestValidateReqBody(t *testing.T) {
	testID, _ := platform.IDFromString("1234123412341234")

	tests := []struct {
		name        string
		body        NotebookReqBody
		expectError error
	}{
		{
			name: "ok",
			body: NotebookReqBody{
				OrgID: *testID,
				Name:  "Example",
				Spec:  map[string]interface{}{},
			},
			expectError: nil,
		},
		{
			name: "missing name",
			body: NotebookReqBody{
				OrgID: *testID,
				Spec:  map[string]interface{}{},
			},
			expectError: ErrNameRequired,
		},
		{
			name: "missing spec",
			body: NotebookReqBody{
				OrgID: *testID,
				Name:  "Example",
			},
			expectError: ErrSpecRequired,
		},
		{
			name: "missing orgID",
			body: NotebookReqBody{
				Name: "Example",
				Spec: map[string]interface{}{},
			},
			expectError: ErrOrgIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.body.Validate()
			if tt.expectError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, tt.expectError, err)
			}
		})
	}
}
