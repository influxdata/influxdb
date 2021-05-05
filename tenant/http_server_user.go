package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

// UserHandler represents an HTTP API handler for users.
type UserHandler struct {
	chi.Router
	api         *kithttp.API
	log         *zap.Logger
	userSvc     influxdb.UserService
	passwordSvc influxdb.PasswordsService
}

const (
	prefixUsers = "/api/v2/users"
	prefixMe    = "/api/v2/me"
)

// NewHTTPUserHandler constructs a new http server.
func NewHTTPUserHandler(log *zap.Logger, userService influxdb.UserService, passwordService influxdb.PasswordsService) *UserHandler {
	svr := &UserHandler{
		api:         kithttp.NewAPI(kithttp.WithLog(log)),
		log:         log,
		userSvc:     userService,
		passwordSvc: passwordService,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	// RESTy routes for "articles" resource
	r.Route("/", func(r chi.Router) {
		r.Post("/", svr.handlePostUser)
		r.Get("/", svr.handleGetUsers)
		r.Put("/password", svr.handlePutUserPassword)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", svr.handleGetUser)
			r.Patch("/", svr.handlePatchUser)
			r.Delete("/", svr.handleDeleteUser)
			r.Get("/permissions", svr.handleGetPermissions)
			r.Put("/password", svr.handlePutUserPassword)
			r.Post("/password", svr.handlePostUserPassword)
		})
	})

	svr.Router = r
	return svr
}

type resourceHandler struct {
	prefix string
	*UserHandler
}

func (h *resourceHandler) Prefix() string {
	return h.prefix
}
func (h *UserHandler) MeResourceHandler() *resourceHandler {
	return &resourceHandler{prefix: prefixMe, UserHandler: h}
}

func (h *UserHandler) UserResourceHandler() *resourceHandler {
	return &resourceHandler{prefix: prefixUsers, UserHandler: h}
}

type passwordSetRequest struct {
	Password string `json:"password"`
}

// handlePutPassword is the HTTP handler for the PUT /api/v2/users/:id/password
func (h *UserHandler) handlePostUserPassword(w http.ResponseWriter, r *http.Request) {
	var body passwordSetRequest
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		})
		return
	}

	param := chi.URLParam(r, "id")
	userID, err := platform.IDFromString(param)
	if err != nil {
		h.api.Err(w, r, &errors.Error{
			Msg: "invalid user ID provided in route",
		})
		return
	}

	err = h.passwordSvc.SetPassword(r.Context(), *userID, body.Password)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *UserHandler) putPassword(ctx context.Context, w http.ResponseWriter, r *http.Request) (username string, err error) {
	req, err := decodePasswordResetRequest(r)
	if err != nil {
		return "", err
	}

	param := chi.URLParam(r, "id")
	userID, err := platform.IDFromString(param)
	if err != nil {
		h.api.Err(w, r, &errors.Error{
			Msg: "invalid user ID provided in route",
		})
		return
	}

	err = h.passwordSvc.CompareAndSetPassword(ctx, *userID, req.PasswordOld, req.PasswordNew)
	if err != nil {
		return "", err
	}
	return req.Username, nil
}

// handlePutPassword is the HTTP handler for the PUT /api/v2/users/:id/password
func (h *UserHandler) handlePutUserPassword(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	_, err := h.putPassword(ctx, w, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("User password updated")
	w.WriteHeader(http.StatusNoContent)
}

type passwordResetRequest struct {
	Username    string
	PasswordOld string
	PasswordNew string
}

type passwordResetRequestBody struct {
	Password string `json:"password"`
}

func decodePasswordResetRequest(r *http.Request) (*passwordResetRequest, error) {
	u, o, ok := r.BasicAuth()
	if !ok {
		return nil, fmt.Errorf("invalid basic auth")
	}

	pr := new(passwordResetRequestBody)
	err := json.NewDecoder(r.Body).Decode(pr)
	if err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	return &passwordResetRequest{
		Username:    u,
		PasswordOld: o,
		PasswordNew: pr.Password,
	}, nil
}

// handlePostUser is the HTTP handler for the POST /api/v2/users route.
func (h *UserHandler) handlePostUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostUserRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if req.User.Status == "" {
		req.User.Status = influxdb.Active
	}

	if err := h.userSvc.CreateUser(ctx, req.User); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("User created", zap.String("user", fmt.Sprint(req.User)))

	h.api.Respond(w, r, http.StatusCreated, newUserResponse(req.User))
}

type postUserRequest struct {
	User *influxdb.User
}

func decodePostUserRequest(ctx context.Context, r *http.Request) (*postUserRequest, error) {
	b := &influxdb.User{}
	if err := json.NewDecoder(r.Body).Decode(b); err != nil {
		return nil, err
	}

	return &postUserRequest{
		User: b,
	}, nil
}

// handleGetMe is the HTTP handler for the GET /api/v2/me.
func (h *UserHandler) handleGetMe(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	a, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	id := a.GetUserID()
	user, err := h.userSvc.FindUserByID(ctx, id)

	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, newUserResponse(user))
}

// handleGetUser is the HTTP handler for the GET /api/v2/users/:id route.
func (h *UserHandler) handleGetUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetUserRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	b, err := h.userSvc.FindUserByID(ctx, req.UserID)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("User retrieved", zap.String("user", fmt.Sprint(b)))

	h.api.Respond(w, r, http.StatusOK, newUserResponse(b))
}

func (h *UserHandler) handleGetPermissions(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		err := &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
		h.api.Err(w, r, err)
		return
	}
	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	ps, err := h.userSvc.FindPermissionForUser(r.Context(), i)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, ps)
}

type getUserRequest struct {
	UserID platform.ID
}

func decodeGetUserRequest(ctx context.Context, r *http.Request) (*getUserRequest, error) {
	id := chi.URLParam(r, "id")
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

	req := &getUserRequest{
		UserID: i,
	}

	return req, nil
}

// handleDeleteUser is the HTTP handler for the DELETE /api/v2/users/:id route.
func (h *UserHandler) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeDeleteUserRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.userSvc.DeleteUser(ctx, req.UserID); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("User deleted", zap.String("userID", fmt.Sprint(req.UserID)))

	w.WriteHeader(http.StatusNoContent)
}

type deleteUserRequest struct {
	UserID platform.ID
}

func decodeDeleteUserRequest(ctx context.Context, r *http.Request) (*deleteUserRequest, error) {
	id := chi.URLParam(r, "id")
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

	return &deleteUserRequest{
		UserID: i,
	}, nil
}

type usersResponse struct {
	Links map[string]string `json:"links"`
	Users []*UserResponse   `json:"users"`
}

func (us usersResponse) ToInfluxdb() []*influxdb.User {
	users := make([]*influxdb.User, len(us.Users))
	for i := range us.Users {
		users[i] = &us.Users[i].User
	}
	return users
}

func newUsersResponse(users []*influxdb.User) *usersResponse {
	res := usersResponse{
		Links: map[string]string{
			"self": "/api/v2/users",
		},
		Users: []*UserResponse{},
	}
	for _, user := range users {
		res.Users = append(res.Users, newUserResponse(user))
	}
	return &res
}

// UserResponse is the response of user
type UserResponse struct {
	Links map[string]string `json:"links"`
	influxdb.User
}

func newUserResponse(u *influxdb.User) *UserResponse {
	return &UserResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/users/%s", u.ID),
		},
		User: *u,
	}
}

// handleGetUsers is the HTTP handler for the GET /api/v2/users route.
func (h *UserHandler) handleGetUsers(w http.ResponseWriter, r *http.Request) {
	// because this is a mounted path in both the /users and the /me route
	// we can get a me request through this handler
	if strings.Contains(r.URL.Path, prefixMe) {
		h.handleGetMe(w, r)
		return
	}

	ctx := r.Context()
	req, err := decodeGetUsersRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	users, _, err := h.userSvc.FindUsers(ctx, req.filter, req.opts)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Users retrieved", zap.String("users", fmt.Sprint(users)))

	h.api.Respond(w, r, http.StatusOK, newUsersResponse(users))
}

type getUsersRequest struct {
	filter influxdb.UserFilter
	opts   influxdb.FindOptions
}

func decodeGetUsersRequest(ctx context.Context, r *http.Request) (*getUsersRequest, error) {
	opts, err := influxdb.DecodeFindOptions(r)
	if err != nil {
		return nil, err
	}

	qp := r.URL.Query()
	req := &getUsersRequest{
		opts: *opts,
	}

	if userID := qp.Get("id"); userID != "" {
		id, err := platform.IDFromString(userID)
		if err != nil {
			return nil, err
		}
		req.filter.ID = id
	}

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	return req, nil
}

// handlePatchUser is the HTTP handler for the PATCH /api/v2/users/:id route.
func (h *UserHandler) handlePatchUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePatchUserRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	b, err := h.userSvc.UpdateUser(ctx, req.UserID, req.Update)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Users updated", zap.String("user", fmt.Sprint(b)))

	h.api.Respond(w, r, http.StatusOK, newUserResponse(b))
}

type patchUserRequest struct {
	Update influxdb.UserUpdate
	UserID platform.ID
}

func decodePatchUserRequest(ctx context.Context, r *http.Request) (*patchUserRequest, error) {
	id := chi.URLParam(r, "id")
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

	var upd influxdb.UserUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	if err := upd.Valid(); err != nil {
		return nil, err
	}

	return &patchUserRequest{
		Update: upd,
		UserID: i,
	}, nil
}
