package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
)

func (s *Service) mapPrincipalToRoles(ctx context.Context, p oauth2.Principal) ([]chronograf.Role, error) {
	mappings, err := s.Store.Mappings(ctx).All(ctx)
	if err != nil {
		return nil, err
	}
	roles := []chronograf.Role{}
MappingsLoop:
	for _, mapping := range mappings {
		if applyMapping(mapping, p) {
			org, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &mapping.Organization})
			if err != nil {
				return nil, err
			}

			for _, role := range roles {
				if role.Organization == org.ID {
					continue MappingsLoop
				}
			}
			roles = append(roles, chronograf.Role{Organization: org.ID, Name: org.DefaultRole})
		}
	}
	fmt.Println("Here 2")

	return roles, nil
}

func applyMapping(m chronograf.Mapping, p oauth2.Principal) bool {
	switch m.Provider {
	case chronograf.MappingWildcard, p.Issuer:
	default:
		return false
	}

	switch m.Scheme {
	case chronograf.MappingWildcard, "oauth2":
	default:
		return false
	}

	if m.Group == chronograf.MappingWildcard {
		return true
	}

	groups := strings.Split(p.Group, ",")

	return matchGroup(m.Group, groups)
}

func matchGroup(match string, groups []string) bool {
	for _, group := range groups {
		if match == group {
			return true
		}
	}

	return false
}

//func (r *organizationRequest) ValidMappings() error {
//	for _, m := range r.Mappings {
//		if m.Provider == "" {
//			return fmt.Errorf("mapping must specify provider")
//		}
//		if m.Scheme == "" {
//			return fmt.Errorf("mapping must specify scheme")
//		}
//		if m.Group == "" {
//			return fmt.Errorf("mapping must specify group")
//		}
//		if m.GrantedRole == "" {
//			return fmt.Errorf("mapping must specify grantedRole")
//		}
//	}
//
//	return nil
//}
