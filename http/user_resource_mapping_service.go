package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/pkg/httpc"
	"go.uber.org/zap"
)

var _ influxdb.UserResourceMappingService = (*UserResourceMappingService)(nil)

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
	log *zap.Logger

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
		b.log.Debug("Member/owner created", zap.String("mapping", fmt.Sprint(mapping)))

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
		b.log.Debug("Members/owners retrieved", zap.String("users", fmt.Sprint(users)))

		if err := encodeResponse(ctx, w, http.StatusOK, newResourceUsersResponse(opts, filter, users)); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}
	}
}

type getMembersRequest struct {
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

// newGetMemberHandler returns a handler func for a GET to /members/:userID or /owners/:userID endpoints.
func newGetMemberHandler(b MemberBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		req, err := decodeGetMemberRequest(ctx, r)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		filter := influxdb.UserResourceMappingFilter{
			ResourceID:   req.ResourceID,
			ResourceType: b.ResourceType,
			UserType:     b.UserType,
			UserID:       req.MemberID,
		}

		mappings, _, err := b.UserResourceMappingService.FindUserResourceMappings(ctx, filter)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}
		fms := mappings[:0]
		for _, m := range mappings {
			if m.MappingType == influxdb.OrgMappingType {
				continue
			}
			fms = append(fms, m)
		}
		if len(fms) == 0 {
			b.HandleHTTPError(ctx, &influxdb.Error{
				Msg:  fmt.Sprintf("no user found matching filter %v", filter),
				Code: influxdb.ENotFound,
			}, w)
			return
		}
		if len(fms) > 1 {
			b.HandleHTTPError(ctx, &influxdb.Error{
				Msg:  fmt.Sprintf("please report this error: multiple users found matching filter %v", filter),
				Code: influxdb.EInternal,
			}, w)
			return
		}
		m := mappings[0]
		u, err := b.UserService.FindUserByID(ctx, m.UserID)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}
		b.log.Debug("Member/owner retrieved", zap.String("user", fmt.Sprint(u)))

		if err := encodeResponse(ctx, w, http.StatusOK, newResourceUserResponse(u, m.UserType)); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}
	}
}

type getMemberRequest struct {
	MemberID   influxdb.ID
	ResourceID influxdb.ID
}

func decodeGetMemberRequest(ctx context.Context, r *http.Request) (*getMemberRequest, error) {
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

	id = params.ByName("userID")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing userID",
		}
	}

	var uid influxdb.ID
	if err := uid.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getMemberRequest{
		ResourceID: rid,
		MemberID:   uid,
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
		b.log.Debug("Member deleted", zap.String("resourceID", req.ResourceID.String()), zap.String("memberID", req.MemberID.String()))

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

type UserResourceMappingService struct {
	Client *httpc.Client
}

func validateFilter(filter influxdb.UserResourceMappingFilter) error {
	if !filter.ResourceID.Valid() {
		return fmt.Errorf("invalid filter: resource id")
	}
	if err := filter.ResourceType.Valid(); err != nil {
		return fmt.Errorf("invalid filter: resource type: %v", err)
	}
	if err := filter.UserType.Valid(); err != nil {
		return fmt.Errorf("invalid filter: user type: %v", err)
	}
	// NOTE: UserID is not compulsory.
	return nil
}

// FindUserResourceMapping finds the user resource mappings matching the specified filter.
func (s *UserResourceMappingService) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	if err := validateFilter(filter); err != nil {
		return nil, 0, err
	}
	var users []*resourceUserResponse
	if filter.UserID.Valid() {
		var r resourceUserResponse
		err := s.Client.
			Get(resourceIDUserPath(filter.ResourceType, filter.ResourceID, filter.UserType, filter.UserID)).
			DecodeJSON(&r).
			Do(ctx)
		if err != nil {
			return nil, 0, err
		}
		users = []*resourceUserResponse{&r}
	} else {
		var r resourceUsersResponse
		err := s.Client.
			Get(resourceIDPath(filter.ResourceType, filter.ResourceID, string(filter.UserType)+"s")).
			DecodeJSON(&r).
			Do(ctx)
		if err != nil {
			return nil, 0, err
		}
		users = r.Users
	}

	urms := make([]*influxdb.UserResourceMapping, len(users))
	for k, item := range users {
		urms[k] = &influxdb.UserResourceMapping{
			ResourceID:   filter.ResourceID,
			ResourceType: filter.ResourceType,
			UserID:       item.User.ID,
			UserType:     item.Role,
		}
	}
	return urms, len(urms), nil
}

// CreateUserResourceMapping will create a user resource mapping
func (s *UserResourceMappingService) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	urlPath := resourceIDPath(m.ResourceType, m.ResourceID, string(m.UserType)+"s")
	return s.Client.
		PostJSON(influxdb.User{ID: m.UserID}, urlPath).
		DecodeJSON(m).
		Do(ctx)
}

// DeleteUserResourceMapping will delete user resource mapping specified.
func (s *UserResourceMappingService) DeleteUserResourceMapping(ctx context.Context, resourceID, userID influxdb.ID) error {
	// TODO(affo): this works but it will end up always hitting the same endpoint. E.g.
	//  api/v2/orgs/0000111100001111/members/1111000011110000
	// The client will never solicit the endpoints we are actually mounting.
	// It works, yes, because delete actually does not need any resource type or user type:
	//  - why not a resource type? Because ids are unique, it does not matter once you have an ID.
	//  - why not a user type? A URM can only be of one type at a time.
	urlPath := resourceIDUserPath(influxdb.OrgsResourceType, resourceID, influxdb.Member, userID)
	return s.Client.
		Delete(urlPath).
		Do(ctx)
}

func resourceIDPath(resourceType influxdb.ResourceType, resourceID influxdb.ID, p string) string {
	return path.Join("/api/v2/", string(resourceType), resourceID.String(), p)
}

func resourceIDUserPath(resourceType influxdb.ResourceType, resourceID influxdb.ID, userType influxdb.UserType, userID influxdb.ID) string {
	return path.Join("/api/v2/", string(resourceType), resourceID.String(), string(userType)+"s", userID.String())
}
