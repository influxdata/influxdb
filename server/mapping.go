package server

import (
	"strings"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/roles"
)

func MappedRole(o *chronograf.Organization, p oauth2.Principal) *chronograf.Role {
	roles := []*chronograf.Role{}
	for _, mapping := range o.Mappings {
		role := applyMapping(mapping, p)
		if role != nil {
			role.Organization = o.ID
			roles = append(roles, role)
		}
	}

	return maxRole(roles)
}

func applyMapping(m chronograf.Mapping, p oauth2.Principal) *chronograf.Role {
	switch m.Provider {
	case chronograf.MappingWildcard, p.Issuer:
	default:
		return nil
	}

	switch m.Scheme {
	case chronograf.MappingWildcard, "oauth2":
	default:
		return nil
	}

	if m.Group == chronograf.MappingWildcard {
		return &chronograf.Role{
			Name: m.GrantedRole,
		}
	}

	groups := strings.Split(p.Group, ",")

	match := matchGroup(m.Group, groups)

	if match {
		return &chronograf.Role{
			Name: m.GrantedRole,
		}
	}

	return nil
}

func matchGroup(match string, groups []string) bool {
	for _, group := range groups {
		if match == group {
			return true
		}
	}

	return false
}

func maxRole(roles []*chronograf.Role) *chronograf.Role {
	var max *chronograf.Role
	for _, role := range roles {
		max = maximumRole(max, role)
	}

	return max
}

func maximumRole(r1, r2 *chronograf.Role) *chronograf.Role {
	if r1 == nil {
		return r2
	}
	if r2 == nil {
		return r2
	}
	if r1.Name == roles.AdminRoleName {
		return r1
	}
	if r2.Name == roles.AdminRoleName {
		return r2
	}

	if r1.Name == roles.EditorRoleName {
		return r1
	}
	if r2.Name == roles.EditorRoleName {
		return r2
	}

	if r1.Name == roles.ViewerRoleName {
		return r1
	}
	if r2.Name == roles.ViewerRoleName {
		return r2
	}

	if r1.Name == roles.MemberRoleName {
		return r1
	}
	if r2.Name == roles.MemberRoleName {
		return r2
	}

	return nil
}
