package server_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/roles"
	"github.com/influxdata/chronograf/server"
)

func TestMappedRole(t *testing.T) {
	type args struct {
		org       *chronograf.Organization
		principal oauth2.Principal
	}
	type wants struct {
		role *chronograf.Role
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "single mapping all wildcards",
			args: args{
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.ViewerRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.ViewerRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "two mapping all wildcards",
			args: args{
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.ViewerRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.EditorRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.EditorRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "two mapping all wildcards, different order",
			args: args{
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.EditorRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.ViewerRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.EditorRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "two mappings with explicit principal",
			args: args{
				principal: oauth2.Principal{
					Subject: "billieta@influxdata.com",
					Issuer:  "google",
					Group:   "influxdata.com",
				},
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      "oauth2",
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.MemberRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.MemberRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.MemberRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "different two mapping all wildcards",
			args: args{
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.ViewerRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.MemberRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.ViewerRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "different two mapping all wildcards, different ordering",
			args: args{
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.MemberRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.ViewerRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.ViewerRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "three mapping all wildcards",
			args: args{
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.EditorRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.ViewerRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.AdminRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.AdminRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "three mapping only one match",
			args: args{
				principal: oauth2.Principal{
					Subject: "billieta@influxdata.com",
					Issuer:  "google",
					Group:   "influxdata.com",
				},
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    "google",
							Scheme:      chronograf.MappingWildcard,
							Group:       "influxdata.com",
							GrantedRole: roles.EditorRoleName,
						},
						chronograf.Mapping{
							Provider:    "google",
							Scheme:      chronograf.MappingWildcard,
							Group:       "not_influxdata",
							GrantedRole: roles.AdminRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      "ldap",
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.AdminRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.EditorRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "three mapping only two matches",
			args: args{
				principal: oauth2.Principal{
					Subject: "billieta@influxdata.com",
					Issuer:  "google",
					Group:   "influxdata.com",
				},
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    "google",
							Scheme:      chronograf.MappingWildcard,
							Group:       "influxdata.com",
							GrantedRole: roles.AdminRoleName,
						},
						chronograf.Mapping{
							Provider:    "google",
							Scheme:      chronograf.MappingWildcard,
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.EditorRoleName,
						},
						chronograf.Mapping{
							Provider:    chronograf.MappingWildcard,
							Scheme:      "ldap",
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.AdminRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.AdminRoleName,
					Organization: "cool",
				},
			},
		},
		{
			name: "missing provider",
			args: args{
				principal: oauth2.Principal{
					Subject: "billieta@influxdata.com",
					Issuer:  "google",
					Group:   "influxdata.com",
				},
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    "",
							Scheme:      "ldap",
							Group:       chronograf.MappingWildcard,
							GrantedRole: roles.AdminRoleName,
						},
					},
				},
			},
			wants: wants{
				role: nil,
			},
		},
		{
			name: "user is in multiple github groups",
			args: args{
				principal: oauth2.Principal{
					Subject: "billieta@influxdata.com",
					Issuer:  "github",
					Group:   "influxdata,another,mimi",
				},
				org: &chronograf.Organization{
					ID:   "cool",
					Name: "Cool Org",
					Mappings: []chronograf.Mapping{
						chronograf.Mapping{
							Provider:    "github",
							Scheme:      chronograf.MappingWildcard,
							Group:       "influxdata",
							GrantedRole: roles.MemberRoleName,
						},
						chronograf.Mapping{
							Provider:    "github",
							Scheme:      chronograf.MappingWildcard,
							Group:       "mimi",
							GrantedRole: roles.EditorRoleName,
						},
						chronograf.Mapping{
							Provider:    "github",
							Scheme:      chronograf.MappingWildcard,
							Group:       "another",
							GrantedRole: roles.AdminRoleName,
						},
					},
				},
			},
			wants: wants{
				role: &chronograf.Role{
					Name:         roles.AdminRoleName,
					Organization: "cool",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			role := server.MappedRole(tt.args.org, tt.args.principal)

			if diff := cmp.Diff(role, tt.wants.role); diff != "" {
				t.Errorf("%q. MappedRole():\n-got/+want\ndiff %s", tt.name, diff)
			}
		})
	}
}
