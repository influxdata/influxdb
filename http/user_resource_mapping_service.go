package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// TODO(jm): how is basepath going to be populated?
type UserResourceMappingService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
	BasePath           string
}

// newPostMemberHandler returns a handler func for a POST to /members or /owners endpoints
func newPostMemberHandler(s platform.UserResourceMappingService, resourceType platform.ResourceType, userType platform.UserType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodePostOrgMemberRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		mapping := &platform.UserResourceMapping{
			ResourceID:   req.OrgID,
			ResourceType: resourceType,
			UserID:       req.MemberID,
			UserType:     userType,
		}

		if err := s.CreateUserResourceMapping(ctx, mapping); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusCreated, mapping); err != nil {
			EncodeError(ctx, err, w)
			return
		}
	}
}

type postOrgMemberRequest struct {
	MemberID platform.ID
	OrgID    platform.ID
}

func decodePostOrgMemberRequest(ctx context.Context, r *http.Request) (*postOrgMemberRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var oid platform.ID
	if err := oid.DecodeFromString(id); err != nil {
		return nil, err
	}

	u := &platform.User{}
	if err := json.NewDecoder(r.Body).Decode(u); err != nil {
		return nil, err
	}

	if u.ID == nil {
		return nil, kerrors.InvalidDataf("user id missing")
	}

	return &postOrgMemberRequest{
		MemberID: u.ID,
		OrgID:    oid,
	}, nil
}

// newPostMemberHandler returns a handler func for a GET to /members or /owners endpoints
func newGetMembersHandler(s platform.UserResourceMappingService, userType platform.UserType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodeGetOrgRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		filter := platform.UserResourceMappingFilter{
			ResourceID: req.OrgID,
			UserType:   platform.Member,
		}
		mappings, _, err := s.FindUserResourceMappings(ctx, filter)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusOK, mappings); err != nil {
			EncodeError(ctx, err, w)
			return
		}
	}
}

// newDeleteMemberHandler returns a handler func for a DELETE to /members or /owners endpoints
func newDeleteMemberHandler(s platform.UserResourceMappingService, userType platform.UserType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodeDeleteOrgMemberRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := s.DeleteUserResourceMapping(ctx, req.OrgID, req.MemberID); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

type deleteOrgMemberRequest struct {
	MemberID platform.ID
	OrgID    platform.ID
}

func decodeDeleteOrgMemberRequest(ctx context.Context, r *http.Request) (*deleteOrgMemberRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing resource id")
	}

	var oid platform.ID
	if err := oid.DecodeFromString(id); err != nil {
		return nil, err
	}

	id = params.ByName("mid")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing member id")
	}

	var mid platform.ID
	if err := mid.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteOrgMemberRequest{
		MemberID: mid,
		OrgID:    oid,
	}, nil
}

func (s *UserResourceMappingService) FindUserResourceMappings(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.UserResourceMapping, int, error) {
	url, err := newURL(s.Addr, s.BasePath)
	if err != nil {
		return nil, 0, err
	}

	query := url.Query()

	// this is not how this is going to work, lol
	if filter.ResourceID.Valid() {
		query.Add("resourceID", filter.ResourceID.String())
	}
	if filter.UserID.Valid() {
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
	if !m.ResourceID.Valid() {
		return kerrors.InvalidDataf("resource ID is required")
	}

	if !m.UserID.Valid() {
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
