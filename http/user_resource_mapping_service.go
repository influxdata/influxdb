package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
)

type UserResourceMappingService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
	BasePath           string
}

func (s *UserResourceMappingService) FindUserResourceMappings(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.UserResourceMapping, int, error) {
	url, err := newURL(s.Addr, s.BasePath)
	if err != nil {
		return nil, 0, err
	}

	query := url.Query()

	// this is not how this is going to work, lol
	if filter.ResourceID != nil {
		query.Add("resourceID", filter.ResourceID.String())
	}
	if filter.UserID != nil {
		query.Add("userID", filter.UserID.String())
	}
	if filter.UserType != "" {
		query.Add("userType", string(filter.UserType))
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	// TODO(jm): make this actually work
	return nil, 0, nil
}

func (s *UserResourceMappingService) CreateUserResourceMapping(ctx context.Context, m *platform.UserResourceMapping) error {
	if m.ResourceID == nil {
		return kerrors.InvalidDataf("resource ID is required")
	}

	if m.UserID == nil {
		return kerrors.InvalidDataf("user ID is required")
	}

	if m.UserType == "" {
		return kerrors.InvalidDataf("user type is required")
	}

	url, err := newURL(s.Addr, resourceIDPath(s.BasePath, m.ResourceID))
	if err != nil {
		return err
	}

	octets, err := json.Marshal(m)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(m); err != nil {
		return err
	}

	return nil
}

func (s *UserResourceMappingService) DeleteUserResourceMapping(ctx context.Context, resourceID platform.ID, userID platform.ID) error {
	url, err := newURL(s.Addr, memberIDPath(s.BasePath, resourceID, userID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

func resourceIDPath(basePath string, resourceID platform.ID) string {
	return path.Join(basePath, resourceID.String())
}

func memberIDPath(basePath string, resourceID platform.ID, memberID platform.ID) string {
	return path.Join(basePath, resourceID.String(), "members", memberID.String())
}
