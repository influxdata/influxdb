package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	platform "github.com/influxdata/influxdb"
	kerrors "github.com/influxdata/influxdb/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// TODO(jm): how is basepath going to be populated?
type UserResourceMappingService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
	BasePath           string
}

type resourceUserResponse struct {
	Role platform.UserType `json:"role"`
	*userResponse
}

func newResourceUserResponse(u *platform.User, userType platform.UserType) *resourceUserResponse {
	return &resourceUserResponse{
		Role:         userType,
		userResponse: newUserResponse(u),
	}
}

type resourceUsersResponse struct {
	Links map[string]string       `json:"links"`
	Users []*resourceUserResponse `json:"users"`
}

func newResourceUsersResponse(opts platform.FindOptions, f platform.UserResourceMappingFilter, users []*platform.User) *resourceUsersResponse {
	rs := resourceUsersResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/%s/%s/%ss", f.ResourceType, f.ResourceID, f.UserType),
		},
		Users: make([]*resourceUserResponse, 0, len(users)),
	}

	for _, user := range users {
		rs.Users = append(rs.Users, newResourceUserResponse(user, f.UserType))
	}
	return &rs
}

// newPostMemberHandler returns a handler func for a POST to /members or /owners endpoints
func newPostMemberHandler(s platform.UserResourceMappingService, userService platform.UserService, resourceType platform.ResourceType, userType platform.UserType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodePostMemberRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		user, err := userService.FindUserByID(ctx, req.MemberID)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		mapping := &platform.UserResourceMapping{
			ResourceID:   req.ResourceID,
			ResourceType: resourceType,
			UserID:       req.MemberID,
			UserType:     userType,
		}

		if err := s.CreateUserResourceMapping(ctx, mapping); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusCreated, newResourceUserResponse(user, userType)); err != nil {
			EncodeError(ctx, err, w)
			return
		}
	}
}

type postMemberRequest struct {
	MemberID   platform.ID
	ResourceID platform.ID
}

func decodePostMemberRequest(ctx context.Context, r *http.Request) (*postMemberRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var rid platform.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	u := &platform.User{}
	if err := json.NewDecoder(r.Body).Decode(u); err != nil {
		return nil, err
	}

	if !u.ID.Valid() {
		return nil, kerrors.InvalidDataf("user id missing or invalid")
	}

	return &postMemberRequest{
		MemberID:   u.ID,
		ResourceID: rid,
	}, nil
}

// newGetMembersHandler returns a handler func for a GET to /members or /owners endpoints
func newGetMembersHandler(s platform.UserResourceMappingService, userService platform.UserService, resourceType platform.ResourceType, userType platform.UserType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodeGetMembersRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		filter := platform.UserResourceMappingFilter{
			ResourceID:   req.ResourceID,
			ResourceType: resourceType,
			UserType:     userType,
		}

		opts := platform.FindOptions{}
		mappings, _, err := s.FindUserResourceMappings(ctx, filter)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		users := make([]*platform.User, 0, len(mappings))
		for _, m := range mappings {
			user, err := userService.FindUserByID(ctx, m.UserID)
			if err != nil {
				EncodeError(ctx, err, w)
				return
			}

			users = append(users, user)
		}

		if err := encodeResponse(ctx, w, http.StatusOK, newResourceUsersResponse(opts, filter, users)); err != nil {
			EncodeError(ctx, err, w)
			return
		}
	}
}

type getMembersRequest struct {
	MemberID   platform.ID
	ResourceID platform.ID
}

func decodeGetMembersRequest(ctx context.Context, r *http.Request) (*getMembersRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getMembersRequest{
		ResourceID: i,
	}

	return req, nil
}

// newDeleteMemberHandler returns a handler func for a DELETE to /members or /owners endpoints
func newDeleteMemberHandler(s platform.UserResourceMappingService, userType platform.UserType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodeDeleteMemberRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := s.DeleteUserResourceMapping(ctx, req.ResourceID, req.MemberID); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

type deleteMemberRequest struct {
	MemberID   platform.ID
	ResourceID platform.ID
}

func decodeDeleteMemberRequest(ctx context.Context, r *http.Request) (*deleteMemberRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing resource id")
	}

	var rid platform.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	id = params.ByName("userID")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing member id")
	}

	var mid platform.ID
	if err := mid.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteMemberRequest{
		MemberID:   mid,
		ResourceID: rid,
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
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	// TODO(jm): make this actually work
	return nil, 0, nil
}

func (s *UserResourceMappingService) CreateUserResourceMapping(ctx context.Context, m *platform.UserResourceMapping) error {
	if err := m.Validate(); err != nil {
		return err
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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

	return CheckError(resp)
}

func resourceIDPath(basePath string, resourceID platform.ID) string {
	return path.Join(basePath, resourceID.String())
}

func memberIDPath(basePath string, resourceID platform.ID, memberID platform.ID) string {
	return path.Join(basePath, resourceID.String(), "members", memberID.String())
}
