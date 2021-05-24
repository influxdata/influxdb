package authorizer_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
)

func Test_Agent(t *testing.T) {
	t.Run("OrgPermissions", func(t *testing.T) {
		tests := []struct {
			name        string
			action      influxdb.Action
			orgID       platform.ID
			permissions []influxdb.Permission
			shouldErr   bool
		}{
			{
				name:   "read valid org  is successful",
				action: influxdb.ReadAction,
				orgID:  3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
			},
			{
				name:   "write from valid org is successful",
				action: influxdb.WriteAction,
				orgID:  3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
			},
			{
				name:   "read from org with only both privileges is successful",
				action: influxdb.ReadAction,
				orgID:  3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
			},
			{
				name:   "write from org with only both privileges is successful",
				action: influxdb.WriteAction,
				orgID:  3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
			},
			{
				name:   "read from invalid org errors",
				action: influxdb.ReadAction,
				orgID:  3333,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
				shouldErr: true,
			},
			{
				name:   "write from invalid org errors",
				action: influxdb.WriteAction,
				orgID:  3333,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
				shouldErr: true,
			},
			{
				name:   "read from org with only write privileges should errors",
				action: influxdb.ReadAction,
				orgID:  3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
				shouldErr: true,
			},
			{
				name:   "write from org with only read privileges should errors",
				action: influxdb.WriteAction,
				orgID:  3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
				shouldErr: true,
			},
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				ctx := icontext.SetAuthorizer(context.TODO(), mock.NewMockAuthorizer(false, tt.permissions))

				agent := new(authorizer.AuthAgent)

				err := agent.OrgPermissions(ctx, tt.orgID, tt.action)
				if tt.shouldErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("IsWritable", func(t *testing.T) {
		tests := []struct {
			name         string
			resourceType influxdb.ResourceType
			orgID        platform.ID
			permissions  []influxdb.Permission
			shouldErr    bool
		}{
			{
				name:         "valid org write perms is always successful",
				resourceType: influxdb.LabelsResourceType,
				orgID:        3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
				},
			},
			{
				name:         "valid resource write perm is always successful",
				resourceType: influxdb.LabelsResourceType,
				orgID:        3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			{
				name:         "valid org and resource write perm is always successful",
				resourceType: influxdb.LabelsResourceType,
				orgID:        3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			{
				name:         "read only org perm errors",
				resourceType: influxdb.LabelsResourceType,
				orgID:        3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   idPtr(3),
						},
					},
				},
				shouldErr: true,
			},
			{
				name:         "read only resource perms errors",
				resourceType: influxdb.LabelsResourceType,
				orgID:        3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
				shouldErr: true,
			},
			{
				name:         "read only org and resource resource perms errors",
				resourceType: influxdb.LabelsResourceType,
				orgID:        3,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   idPtr(3),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
				shouldErr: true,
			},
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				ctx := icontext.SetAuthorizer(context.TODO(), mock.NewMockAuthorizer(false, tt.permissions))

				agent := new(authorizer.AuthAgent)

				err := agent.IsWritable(ctx, tt.orgID, tt.resourceType)
				if tt.shouldErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}

			t.Run(tt.name, fn)
		}
	})
}
