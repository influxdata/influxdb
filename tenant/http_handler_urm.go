package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

type urmHandler struct {
	log     *zap.Logger
	svc     influxdb.UserResourceMappingService
	userSvc influxdb.UserService
	api     *kithttp.API

	rt          influxdb.ResourceType
	idLookupKey string
}

// NewURMHandler generates a mountable handler for URMs. It needs to know how it will be looking up your resource id
// this system assumes you are using chi syntax for query string params `/orgs/{id}/` so it can use chi.URLParam().
func NewURMHandler(log *zap.Logger, rt influxdb.ResourceType, idLookupKey string, uSvc influxdb.UserService, urmSvc influxdb.UserResourceMappingService) http.Handler {
	h := &urmHandler{
		log:     log,
		svc:     urmSvc,
		userSvc: uSvc,
		api:     kithttp.NewAPI(kithttp.WithLog(log)),

		rt:          rt,
		idLookupKey: idLookupKey,
	}

	r := chi.NewRouter()
	r.Get("/", h.getURMsByType)
	r.Post("/", h.postURMByType)
	r.Delete("/{userID}", h.deleteURM)
	return r
}

func (h *urmHandler) getURMsByType(w http.ResponseWriter, r *http.Request) {
	userType := userTypeFromPath(r.URL.Path)
	ctx := r.Context()
	req, err := h.decodeGetRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	filter := influxdb.UserResourceMappingFilter{
		ResourceID:   req.ResourceID,
		ResourceType: h.rt,
		UserType:     userType,
	}
	mappings, _, err := h.svc.FindUserResourceMappings(ctx, filter)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	users := make([]*influxdb.User, 0, len(mappings))
	for _, m := range mappings {
		if m.MappingType == influxdb.OrgMappingType {
			continue
		}
		user, err := h.userSvc.FindUserByID(ctx, m.UserID)
		if err != nil {
			h.api.Err(w, r, err)
			return
		}

		users = append(users, user)
	}
	h.log.Debug("Members/owners retrieved", zap.String("users", fmt.Sprint(users)))

	h.api.Respond(w, r, http.StatusOK, newResourceUsersResponse(filter, users))

}

type getRequest struct {
	ResourceID platform.ID
}

func (h *urmHandler) decodeGetRequest(ctx context.Context, r *http.Request) (*getRequest, error) {
	id := chi.URLParam(r, h.idLookupKey)
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getRequest{
		ResourceID: i,
	}

	return req, nil
}

func (h *urmHandler) postURMByType(w http.ResponseWriter, r *http.Request) {
	userType := userTypeFromPath(r.URL.Path)
	ctx := r.Context()
	req, err := h.decodePostRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	user, err := h.userSvc.FindUserByID(ctx, req.UserID)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	mapping := &influxdb.UserResourceMapping{
		ResourceID:   req.ResourceID,
		ResourceType: h.rt,
		UserID:       req.UserID,
		UserType:     userType,
	}
	if err := h.svc.CreateUserResourceMapping(ctx, mapping); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Member/owner created", zap.String("mapping", fmt.Sprint(mapping)))

	h.api.Respond(w, r, http.StatusCreated, newResourceUserResponse(user, userType))
}

type postRequest struct {
	UserID     platform.ID
	ResourceID platform.ID
}

func (h urmHandler) decodePostRequest(ctx context.Context, r *http.Request) (*postRequest, error) {
	id := chi.URLParam(r, h.idLookupKey)
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	var rid platform.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	u := &influxdb.User{}
	if err := json.NewDecoder(r.Body).Decode(u); err != nil {
		return nil, err
	}

	if !u.ID.Valid() {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "user id missing or invalid",
		}
	}

	return &postRequest{
		UserID:     u.ID,
		ResourceID: rid,
	}, nil
}

func (h *urmHandler) deleteURM(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := h.decodeDeleteRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.svc.DeleteUserResourceMapping(ctx, req.resourceID, req.userID); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Member deleted", zap.String("resourceID", req.resourceID.String()), zap.String("memberID", req.userID.String()))

	w.WriteHeader(http.StatusNoContent)
}

type deleteRequest struct {
	userID     platform.ID
	resourceID platform.ID
}

func (h *urmHandler) decodeDeleteRequest(ctx context.Context, r *http.Request) (*deleteRequest, error) {
	id := chi.URLParam(r, h.idLookupKey)
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	var rid platform.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	id = chi.URLParam(r, "userID")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing member id",
		}
	}

	var uid platform.ID
	if err := uid.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteRequest{
		userID:     uid,
		resourceID: rid,
	}, nil
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

func newResourceUsersResponse(f influxdb.UserResourceMappingFilter, users []*influxdb.User) *resourceUsersResponse {
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

// determine the type of request from the path.
func userTypeFromPath(p string) influxdb.UserType {
	if p == "" {
		return influxdb.Member
	}

	switch path.Base(p) {
	case "members":
		return influxdb.Member
	case "owners":
		return influxdb.Owner
	default:
		return userTypeFromPath(path.Dir(p))
	}
}
