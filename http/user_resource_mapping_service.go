package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"go.uber.org/zap"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb"
)

// UserResourceMappingService is the struct of urm service
type UserResourceMappingService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

type resourceUserResponse struct {
	Role influxdb.UserType `json:"role"`
	*UserResponse
}

func newResourceUserResponse(u *influxdb.User, userType influxdb.UserType) *resourceUserResponse {
	return &resourceUserResponse{
		Role:         userType,
		UserResponse: newUserResponse(u),
	}
}

type resourceUsersResponse struct {
	Links map[string]string       `json:"links"`
	Users []*resourceUserResponse `json:"users"`
}

func newResourceUsersResponse(opts influxdb.FindOptions, f influxdb.UserResourceMappingFilter, users []*influxdb.User) *resourceUsersResponse {
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

// MemberBackend is all services and associated parameters required to construct
// member handler.
type MemberBackend struct {
	influxdb.HTTPErrorHandler
	logger *zap.Logger

	ResourceType influxdb.ResourceType
	UserType     influxdb.UserType

	UserResourceMappingService influxdb.UserResourceMappingService
	UserService                influxdb.UserService
}

// newPostMemberHandler returns a handler func for a POST to /members or /owners endpoints
func newPostMemberHandler(b MemberBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req, err := decodePostMemberRequest(ctx, r)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		user, err := b.UserService.FindUserByID(ctx, req.MemberID)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		mapping := &influxdb.UserResourceMapping{
			ResourceID:   req.ResourceID,
			ResourceType: b.ResourceType,
			UserID:       req.MemberID,
			UserType:     b.UserType,
		}

		if err := b.UserResourceMappingService.CreateUserResourceMapping(ctx, mapping); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}
		b.logger.Debug("member/owner created", zap.String("mapping", fmt.Sprint(mapping)))

		if err := encodeResponse(ctx, w, http.StatusCreated, newResourceUserResponse(user, b.UserType)); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}
	}
}

type postMemberRequest struct {
	MemberID   influxdb.ID
	ResourceID influxdb.ID
}

func decodePostMemberRequest(ctx context.Context, r *http.Request) (*postMemberRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var rid influxdb.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	u := &influxdb.User{}
	if err := json.NewDecoder(r.Body).Decode(u); err != nil {
		return nil, err
	}

	if !u.ID.Valid() {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "user id missing or invalid",
		}
	}

	return &postMemberRequest{
		MemberID:   u.ID,
		ResourceID: rid,
	}, nil
}

// newGetMembersHandler returns a handler func for a GET to /members or /owners endpoints
func newGetMembersHandler(b MemberBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req, err := decodeGetMembersRequest(ctx, r)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		filter := influxdb.UserResourceMappingFilter{
			ResourceID:   req.ResourceID,
			ResourceType: b.ResourceType,
			UserType:     b.UserType,
		}

		opts := influxdb.FindOptions{}
		mappings, _, err := b.UserResourceMappingService.FindUserResourceMappings(ctx, filter)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		users := make([]*influxdb.User, 0, len(mappings))
		for _, m := range mappings {
			if m.MappingType == influxdb.OrgMappingType {
				continue
			}
			user, err := b.UserService.FindUserByID(ctx, m.UserID)
			if err != nil {
				b.HandleHTTPError(ctx, err, w)
				return
			}

			users = append(users, user)
		}
		b.logger.Debug("members/owners retrieved", zap.String("users", fmt.Sprint(users)))

		if err := encodeResponse(ctx, w, http.StatusOK, newResourceUsersResponse(opts, filter, users)); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}
	}
}

type getMembersRequest struct {
	MemberID   influxdb.ID
	ResourceID influxdb.ID
}

func decodeGetMembersRequest(ctx context.Context, r *http.Request) (*getMembersRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getMembersRequest{
		ResourceID: i,
	}

	return req, nil
}

// newDeleteMemberHandler returns a handler func for a DELETE to /members or /owners endpoints
func newDeleteMemberHandler(b MemberBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req, err := decodeDeleteMemberRequest(ctx, r)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		if err := b.UserResourceMappingService.DeleteUserResourceMapping(ctx, req.ResourceID, req.MemberID); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}
		b.logger.Debug("member deleted", zap.String("resourceID", req.ResourceID.String()), zap.String("memberID", req.MemberID.String()))

		w.WriteHeader(http.StatusNoContent)
	}
}

type deleteMemberRequest struct {
	MemberID   influxdb.ID
	ResourceID influxdb.ID
}

func decodeDeleteMemberRequest(ctx context.Context, r *http.Request) (*deleteMemberRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing resource id",
		}
	}

	var rid influxdb.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	id = params.ByName("userID")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing member id",
		}
	}

	var mid influxdb.ID
	if err := mid.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteMemberRequest{
		MemberID:   mid,
		ResourceID: rid,
	}, nil
}

// FindUserResourceMappings returns the user resource mappings
func (s *UserResourceMappingService) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	url, err := NewURL(s.Addr, resourceIDPath(filter.ResourceType, filter.ResourceID, string(filter.UserType)+"s"))
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	SetToken(s.Token, req)

	hc := NewClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	results := new(resourceUsersResponse)
	if err := json.NewDecoder(resp.Body).Decode(results); err != nil {
		return nil, 0, err
	}

	urs := make([]*influxdb.UserResourceMapping, len(results.Users))
	for k, item := range results.Users {
		urs[k] = &influxdb.UserResourceMapping{
			ResourceID:   filter.ResourceID,
			ResourceType: filter.ResourceType,
			UserID:       item.User.ID,
			UserType:     item.Role,
		}
	}
	return urs, len(urs), nil
}

// CreateUserResourceMapping will create a user resource mapping
func (s *UserResourceMappingService) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	url, err := NewURL(s.Addr, resourceIDPath(m.ResourceType, m.ResourceID, string(m.UserType)+"s"))
	if err != nil {
		return err
	}

	octets, err := json.Marshal(influxdb.User{ID: m.UserID})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := NewClient(url.Scheme, s.InsecureSkipVerify)

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

// DeleteUserResourceMapping will delete user resource mapping based in criteria.
func (s *UserResourceMappingService) DeleteUserResourceMapping(ctx context.Context, resourceID influxdb.ID, userID influxdb.ID) error {
	// default to use org resource type, and member resource type since it doesn't matter.
	url, err := NewURL(s.Addr, resourceIDUserPath(influxdb.OrgsResourceType, resourceID, influxdb.Member, userID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := NewClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp)
}

func resourceIDPath(resourceType influxdb.ResourceType, resourceID influxdb.ID, p string) string {
	return path.Join("/api/v2/", string(resourceType), resourceID.String(), p)
}

func resourceIDUserPath(resourceType influxdb.ResourceType, resourceID influxdb.ID, userType influxdb.UserType, userID influxdb.ID) string {
	return path.Join("/api/v2/", string(resourceType), resourceID.String(), string(userType)+"s", userID.String())
}
