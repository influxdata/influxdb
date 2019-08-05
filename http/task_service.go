package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	influxdb "github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// TaskBackend is all services and associated parameters required to construct
// the TaskHandler.
type TaskBackend struct {
	platform.HTTPErrorHandler
	Logger *zap.Logger

	TaskService                platform.TaskService
	AuthorizationService       platform.AuthorizationService
	OrganizationService        platform.OrganizationService
	UserResourceMappingService platform.UserResourceMappingService
	LabelService               platform.LabelService
	UserService                platform.UserService
	BucketService              platform.BucketService
}

// NewTaskBackend returns a new instance of TaskBackend.
func NewTaskBackend(b *APIBackend) *TaskBackend {
	return &TaskBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "task")),
		TaskService:                b.TaskService,
		AuthorizationService:       b.AuthorizationService,
		OrganizationService:        b.OrganizationService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		BucketService:              b.BucketService,
	}
}

// TaskHandler represents an HTTP API handler for tasks.
type TaskHandler struct {
	*httprouter.Router
	platform.HTTPErrorHandler
	logger *zap.Logger

	TaskService                platform.TaskService
	AuthorizationService       platform.AuthorizationService
	OrganizationService        platform.OrganizationService
	UserResourceMappingService platform.UserResourceMappingService
	LabelService               platform.LabelService
	UserService                platform.UserService
	BucketService              platform.BucketService
}

const (
	tasksPath              = "/api/v2/tasks"
	tasksIDPath            = "/api/v2/tasks/:id"
	tasksIDLogsPath        = "/api/v2/tasks/:id/logs"
	tasksIDMembersPath     = "/api/v2/tasks/:id/members"
	tasksIDMembersIDPath   = "/api/v2/tasks/:id/members/:userID"
	tasksIDOwnersPath      = "/api/v2/tasks/:id/owners"
	tasksIDOwnersIDPath    = "/api/v2/tasks/:id/owners/:userID"
	tasksIDRunsPath        = "/api/v2/tasks/:id/runs"
	tasksIDRunsIDPath      = "/api/v2/tasks/:id/runs/:rid"
	tasksIDRunsIDLogsPath  = "/api/v2/tasks/:id/runs/:rid/logs"
	tasksIDRunsIDRetryPath = "/api/v2/tasks/:id/runs/:rid/retry"
	tasksIDLabelsPath      = "/api/v2/tasks/:id/labels"
	tasksIDLabelsIDPath    = "/api/v2/tasks/:id/labels/:lid"
)

// NewTaskHandler returns a new instance of TaskHandler.
func NewTaskHandler(b *TaskBackend) *TaskHandler {
	h := &TaskHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		logger:           b.Logger,

		TaskService:                b.TaskService,
		AuthorizationService:       b.AuthorizationService,
		OrganizationService:        b.OrganizationService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		BucketService:              b.BucketService,
	}

	h.HandlerFunc("GET", tasksPath, h.handleGetTasks)
	h.HandlerFunc("POST", tasksPath, h.handlePostTask)

	h.HandlerFunc("GET", tasksIDPath, h.handleGetTask)
	h.HandlerFunc("PATCH", tasksIDPath, h.handleUpdateTask)
	h.HandlerFunc("DELETE", tasksIDPath, h.handleDeleteTask)

	h.HandlerFunc("GET", tasksIDLogsPath, h.handleGetLogs)
	h.HandlerFunc("GET", tasksIDRunsIDLogsPath, h.handleGetLogs)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               platform.TasksResourceType,
		UserType:                   platform.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", tasksIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", tasksIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", tasksIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               platform.TasksResourceType,
		UserType:                   platform.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", tasksIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", tasksIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", tasksIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	h.HandlerFunc("GET", tasksIDRunsPath, h.handleGetRuns)
	h.HandlerFunc("POST", tasksIDRunsPath, h.handleForceRun)
	h.HandlerFunc("GET", tasksIDRunsIDPath, h.handleGetRun)
	h.HandlerFunc("POST", tasksIDRunsIDRetryPath, h.handleRetryRun)
	h.HandlerFunc("DELETE", tasksIDRunsIDPath, h.handleCancelRun)

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     platform.TasksResourceType,
	}
	h.HandlerFunc("GET", tasksIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", tasksIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", tasksIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type taskResponse struct {
	Links  map[string]string `json:"links"`
	Labels []platform.Label  `json:"labels"`
	platform.Task
}

func newTaskResponse(t platform.Task, labels []*platform.Label) taskResponse {
	response := taskResponse{
		Links: map[string]string{
			"self":    fmt.Sprintf("/api/v2/tasks/%s", t.ID),
			"members": fmt.Sprintf("/api/v2/tasks/%s/members", t.ID),
			"owners":  fmt.Sprintf("/api/v2/tasks/%s/owners", t.ID),
			"labels":  fmt.Sprintf("/api/v2/tasks/%s/labels", t.ID),
			"runs":    fmt.Sprintf("/api/v2/tasks/%s/runs", t.ID),
			"logs":    fmt.Sprintf("/api/v2/tasks/%s/logs", t.ID),
		},
		Task:   t,
		Labels: []platform.Label{},
	}

	for _, l := range labels {
		response.Labels = append(response.Labels, *l)
	}

	return response
}

func newTasksPagingLinks(basePath string, ts []*platform.Task, f platform.TaskFilter) *platform.PagingLinks {
	var self, next string
	u := url.URL{
		Path: basePath,
	}

	values := url.Values{}
	for k, vs := range f.QueryParams() {
		for _, v := range vs {
			if v != "" {
				values.Add(k, v)
			}
		}
	}

	u.RawQuery = values.Encode()
	self = u.String()

	if len(ts) >= f.Limit {
		values.Set("after", ts[f.Limit-1].ID.String())
		u.RawQuery = values.Encode()
		next = u.String()
	}

	links := &platform.PagingLinks{
		Self: self,
		Next: next,
	}

	return links
}

type tasksResponse struct {
	Links *platform.PagingLinks `json:"links"`
	Tasks []taskResponse        `json:"tasks"`
}

func newTasksResponse(ctx context.Context, ts []*platform.Task, f platform.TaskFilter, labelService platform.LabelService) tasksResponse {
	rs := tasksResponse{
		Links: newTasksPagingLinks(tasksPath, ts, f),
		Tasks: make([]taskResponse, len(ts)),
	}

	for i := range ts {
		labels, _ := labelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: ts[i].ID})
		rs.Tasks[i] = newTaskResponse(*ts[i], labels)
	}
	return rs
}

type runResponse struct {
	Links map[string]string `json:"links,omitempty"`
	platform.Run
}

func newRunResponse(r platform.Run) runResponse {
	return runResponse{
		Links: map[string]string{
			"self":  fmt.Sprintf("/api/v2/tasks/%s/runs/%s", r.TaskID, r.ID),
			"task":  fmt.Sprintf("/api/v2/tasks/%s", r.TaskID),
			"logs":  fmt.Sprintf("/api/v2/tasks/%s/runs/%s/logs", r.TaskID, r.ID),
			"retry": fmt.Sprintf("/api/v2/tasks/%s/runs/%s/retry", r.TaskID, r.ID),
		},
		Run: r,
	}
}

type runsResponse struct {
	Links map[string]string `json:"links"`
	Runs  []*runResponse    `json:"runs"`
}

func newRunsResponse(rs []*platform.Run, taskID platform.ID) runsResponse {
	r := runsResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/tasks/%s/runs", taskID),
			"task": fmt.Sprintf("/api/v2/tasks/%s", taskID),
		},
		Runs: make([]*runResponse, len(rs)),
	}

	for i := range rs {
		rs := newRunResponse(*rs[i])
		r.Runs[i] = &rs
	}
	return r
}

func (h *TaskHandler) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.logger.Debug("tasks retrieve request", zap.String("r", fmt.Sprint(r)))
	req, err := decodeGetTasksRequest(ctx, r, h.OrganizationService)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	tasks, _, err := h.TaskService.FindTasks(ctx, req.filter)
	if err != nil {
		err = &platform.Error{
			Err: err,
			Msg: "failed to find tasks",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.logger.Debug("tasks retrived", zap.String("tasks", fmt.Sprint(tasks)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTasksResponse(ctx, tasks, req.filter, h.LabelService)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type getTasksRequest struct {
	filter platform.TaskFilter
}

func decodeGetTasksRequest(ctx context.Context, r *http.Request, orgs platform.OrganizationService) (*getTasksRequest, error) {
	qp := r.URL.Query()
	req := &getTasksRequest{}

	if after := qp.Get("after"); after != "" {
		id, err := platform.IDFromString(after)
		if err != nil {
			return nil, err
		}
		req.filter.After = id
	}

	if orgName := qp.Get("org"); orgName != "" {
		o, err := orgs.FindOrganization(ctx, platform.OrganizationFilter{Name: &orgName})
		if err != nil {
			if pErr, ok := err.(*platform.Error); ok && pErr != nil {
				if kv.IsNotFound(err) || pErr.Code == platform.EUnauthorized {
					return nil, &platform.Error{
						Err: errors.New("org not found or unauthorized"),
						Msg: "org " + orgName + " not found or unauthorized",
					}
				}
			}
			return nil, err
		}
		req.filter.Organization = o.Name
		req.filter.OrganizationID = &o.ID
	}
	if oid := qp.Get("orgID"); oid != "" {
		orgID, err := platform.IDFromString(oid)
		if err != nil {
			return nil, err
		}
		req.filter.OrganizationID = orgID
	}

	if userID := qp.Get("user"); userID != "" {
		id, err := platform.IDFromString(userID)
		if err != nil {
			return nil, err
		}
		req.filter.User = id
	}

	if limit := qp.Get("limit"); limit != "" {
		lim, err := strconv.Atoi(limit)
		if err != nil {
			return nil, err
		}
		if lim < 1 || lim > platform.TaskMaxPageSize {
			return nil, &platform.Error{
				Code: platform.EUnprocessableEntity,
				Msg:  fmt.Sprintf("limit must be between 1 and %d", platform.TaskMaxPageSize),
			}
		}
		req.filter.Limit = lim
	} else {
		req.filter.Limit = platform.TaskDefaultPageSize
	}

	if ttype := qp.Get("type"); ttype != "" {
		req.filter.Type = &ttype
	}

	return req, nil
}

func (h *TaskHandler) handlePostTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.logger.Debug("task create request", zap.String("r", fmt.Sprint(r)))

	req, err := decodePostTaskRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.populateTaskCreateOrg(ctx, &req.TaskCreate); err != nil {
		err = &platform.Error{
			Err: err,
			Msg: "could not identify organization",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if !req.TaskCreate.OrganizationID.Valid() {
		err := &platform.Error{
			Code: platform.EInvalid,
			Msg:  "invalid organization id",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	task, err := h.TaskService.CreateTask(ctx, req.TaskCreate)
	if err != nil {
		if e, ok := err.(AuthzError); ok {
			h.logger.Error("failed authentication", zap.Errors("error messages", []error{err, e.AuthzError()}))
		}

		// if the error is not already a platform error then make it into one
		if _, ok := err.(*platform.Error); !ok {
			err = &platform.Error{
				Err:  err,
				Code: platform.EInternal,
				Msg:  "failed to create task",
			}
		}

		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newTaskResponse(*task, []*platform.Label{})); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type postTaskRequest struct {
	TaskCreate platform.TaskCreate
}

func decodePostTaskRequest(ctx context.Context, r *http.Request) (*postTaskRequest, error) {
	var tc platform.TaskCreate
	if err := json.NewDecoder(r.Body).Decode(&tc); err != nil {
		return nil, err
	}

	if err := tc.Validate(); err != nil {
		return nil, err
	}

	return &postTaskRequest{
		TaskCreate: tc,
	}, nil
}

func (h *TaskHandler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.logger.Debug("task retrieve request", zap.String("r", fmt.Sprint(r)))
	req, err := decodeGetTaskRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	task, err := h.TaskService.FindTaskByID(ctx, req.TaskID)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.ENotFound,
			Msg:  "failed to find task",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: task.ID})
	if err != nil {
		err = &platform.Error{
			Err: err,
			Msg: "failed to find resource labels",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.logger.Debug("task retrived", zap.String("tasks", fmt.Sprint(task)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTaskResponse(*task, labels)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type getTaskRequest struct {
	TaskID platform.ID
}

func decodeGetTaskRequest(ctx context.Context, r *http.Request) (*getTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getTaskRequest{
		TaskID: i,
	}

	return req, nil
}

func (h *TaskHandler) handleUpdateTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.logger.Debug("task update request", zap.String("r", fmt.Sprint(r)))
	req, err := decodeUpdateTaskRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	task, err := h.TaskService.UpdateTask(ctx, req.TaskID, req.Update)
	if err != nil {
		err := &platform.Error{
			Err: err,
			Msg: "failed to update task",
		}
		if err.Err == influxdb.ErrTaskNotFound {
			err.Code = platform.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: task.ID})
	if err != nil {
		err = &platform.Error{
			Err: err,
			Msg: "failed to find resource labels",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.logger.Debug("tasks updated", zap.String("task", fmt.Sprint(task)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTaskResponse(*task, labels)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type updateTaskRequest struct {
	Update platform.TaskUpdate
	TaskID platform.ID
}

func decodeUpdateTaskRequest(ctx context.Context, r *http.Request) (*updateTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd platform.TaskUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	if err := upd.Validate(); err != nil {
		return nil, err
	}

	return &updateTaskRequest{
		Update: upd,
		TaskID: i,
	}, nil
}

func (h *TaskHandler) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.logger.Debug("task delete request", zap.String("r", fmt.Sprint(r)))
	req, err := decodeDeleteTaskRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.TaskService.DeleteTask(ctx, req.TaskID); err != nil {
		err := &platform.Error{
			Err: err,
			Msg: "failed to delete task",
		}
		if err.Err == influxdb.ErrTaskNotFound {
			err.Code = platform.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.logger.Debug("tasks deleted", zap.String("taskID", fmt.Sprint(req.TaskID)))
	w.WriteHeader(http.StatusNoContent)
}

type deleteTaskRequest struct {
	TaskID platform.ID
}

func decodeDeleteTaskRequest(ctx context.Context, r *http.Request) (*deleteTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteTaskRequest{
		TaskID: i,
	}, nil
}

func (h *TaskHandler) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetLogsRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EUnauthorized,
			Msg:  "failed to get authorizer",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if k := auth.Kind(); k != platform.AuthorizationKind {
		// Get the authorization for the task, if allowed.
		authz, err := h.getAuthorizationForTask(ctx, auth, req.filter.Task)
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		// We were able to access the authorizer for the task, so reassign that on the context for the rest of this call.
		ctx = pcontext.SetAuthorizer(ctx, authz)
	}

	logs, _, err := h.TaskService.FindLogs(ctx, req.filter)
	if err != nil {
		err := &platform.Error{
			Err: err,
			Msg: "failed to find task logs",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrNoRunsFound {
			err.Code = platform.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, &getLogsResponse{Events: logs}); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type getLogsRequest struct {
	filter platform.LogFilter
}

type getLogsResponse struct {
	Events []*platform.Log `json:"events"`
}

func decodeGetLogsRequest(ctx context.Context, r *http.Request) (*getLogsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	req := &getLogsRequest{}
	taskID, err := platform.IDFromString(id)
	if err != nil {
		return nil, err
	}
	req.filter.Task = *taskID

	if runID := params.ByName("rid"); runID != "" {
		id, err := platform.IDFromString(runID)
		if err != nil {
			return nil, err
		}
		req.filter.Run = id
	}

	return req, nil
}

func (h *TaskHandler) handleGetRuns(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetRunsRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EUnauthorized,
			Msg:  "failed to get authorizer",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if k := auth.Kind(); k != platform.AuthorizationKind {
		// Get the authorization for the task, if allowed.
		authz, err := h.getAuthorizationForTask(ctx, auth, req.filter.Task)
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		// We were able to access the authorizer for the task, so reassign that on the context for the rest of this call.
		ctx = pcontext.SetAuthorizer(ctx, authz)
	}

	runs, _, err := h.TaskService.FindRuns(ctx, req.filter)
	if err != nil {
		err := &platform.Error{
			Err: err,
			Msg: "failed to find runs",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrNoRunsFound {
			err.Code = platform.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newRunsResponse(runs, req.filter.Task)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type getRunsRequest struct {
	filter platform.RunFilter
}

func decodeGetRunsRequest(ctx context.Context, r *http.Request) (*getRunsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	req := &getRunsRequest{}
	taskID, err := platform.IDFromString(id)
	if err != nil {
		return nil, err
	}
	req.filter.Task = *taskID

	qp := r.URL.Query()

	if id := qp.Get("after"); id != "" {
		afterID, err := platform.IDFromString(id)
		if err != nil {
			return nil, err
		}
		req.filter.After = afterID
	}

	if limit := qp.Get("limit"); limit != "" {
		i, err := strconv.Atoi(limit)
		if err != nil {
			return nil, err
		}

		if i < 1 || i > influxdb.TaskMaxPageSize {
			return nil, influxdb.ErrOutOfBoundsLimit
		}
		req.filter.Limit = i
	}

	var at, bt string
	var afterTime, beforeTime time.Time
	if at = qp.Get("afterTime"); at != "" {
		afterTime, err = time.Parse(time.RFC3339, at)
		if err != nil {
			return nil, err
		}
		req.filter.AfterTime = at
	}

	if bt = qp.Get("beforeTime"); bt != "" {
		beforeTime, err = time.Parse(time.RFC3339, bt)
		if err != nil {
			return nil, err
		}
		req.filter.BeforeTime = bt
	}

	if at != "" && bt != "" && !beforeTime.After(afterTime) {
		return nil, &platform.Error{
			Code: platform.EUnprocessableEntity,
			Msg:  "beforeTime must be later than afterTime",
		}
	}

	return req, nil
}

func (h *TaskHandler) handleForceRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeForceRunRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	run, err := h.TaskService.ForceRun(ctx, req.TaskID, req.Timestamp)
	if err != nil {
		err := &platform.Error{
			Err: err,
			Msg: "failed to force run",
		}
		if err.Err == influxdb.ErrTaskNotFound {
			err.Code = platform.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusOK, newRunResponse(*run)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type forceRunRequest struct {
	TaskID    platform.ID
	Timestamp int64
}

func decodeForceRunRequest(ctx context.Context, r *http.Request) (forceRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return forceRunRequest{}, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var ti platform.ID
	if err := ti.DecodeFromString(tid); err != nil {
		return forceRunRequest{}, err
	}

	var req struct {
		ScheduledFor string `json:"scheduledFor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return forceRunRequest{}, err
	}

	var t time.Time
	if req.ScheduledFor == "" {
		t = time.Now()
	} else {
		var err error
		t, err = time.Parse(time.RFC3339, req.ScheduledFor)
		if err != nil {
			return forceRunRequest{}, err
		}
	}

	return forceRunRequest{
		TaskID:    ti,
		Timestamp: t.Unix(),
	}, nil
}

func (h *TaskHandler) handleGetRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetRunRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EUnauthorized,
			Msg:  "failed to get authorizer",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if k := auth.Kind(); k != platform.AuthorizationKind {
		// Get the authorization for the task, if allowed.
		authz, err := h.getAuthorizationForTask(ctx, auth, req.TaskID)
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		// We were able to access the authorizer for the task, so reassign that on the context for the rest of this call.
		ctx = pcontext.SetAuthorizer(ctx, authz)
	}

	run, err := h.TaskService.FindRunByID(ctx, req.TaskID, req.RunID)
	if err != nil {
		err := &platform.Error{
			Err: err,
			Msg: "failed to find run",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrRunNotFound {
			err.Code = platform.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newRunResponse(*run)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type getRunRequest struct {
	TaskID platform.ID
	RunID  platform.ID
}

func decodeGetRunRequest(ctx context.Context, r *http.Request) (*getRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}

	var ti, ri platform.ID
	if err := ti.DecodeFromString(tid); err != nil {
		return nil, err
	}
	if err := ri.DecodeFromString(rid); err != nil {
		return nil, err
	}

	return &getRunRequest{
		RunID:  ri,
		TaskID: ti,
	}, nil
}

type cancelRunRequest struct {
	RunID  platform.ID
	TaskID platform.ID
}

func decodeCancelRunRequest(ctx context.Context, r *http.Request) (*cancelRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}
	tid := params.ByName("id")
	if tid == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(rid); err != nil {
		return nil, err
	}
	var t platform.ID
	if err := t.DecodeFromString(tid); err != nil {
		return nil, err
	}

	return &cancelRunRequest{
		RunID:  i,
		TaskID: t,
	}, nil
}

func (h *TaskHandler) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeCancelRunRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	err = h.TaskService.CancelRun(ctx, req.TaskID, req.RunID)
	if err != nil {
		err := &platform.Error{
			Err: err,
			Msg: "failed to cancel run",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrRunNotFound {
			err.Code = platform.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *TaskHandler) handleRetryRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeRetryRunRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EUnauthorized,
			Msg:  "failed to get authorizer",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if k := auth.Kind(); k != platform.AuthorizationKind {
		// Get the authorization for the task, if allowed.
		authz, err := h.getAuthorizationForTask(ctx, auth, req.TaskID)
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		// We were able to access the authorizer for the task, so reassign that on the context for the rest of this call.
		ctx = pcontext.SetAuthorizer(ctx, authz)
	}

	run, err := h.TaskService.RetryRun(ctx, req.TaskID, req.RunID)
	if err != nil {
		err := &platform.Error{
			Err: err,
			Msg: "failed to retry run",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrRunNotFound {
			err.Code = platform.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusOK, newRunResponse(*run)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type retryRunRequest struct {
	RunID, TaskID platform.ID
}

func decodeRetryRunRequest(ctx context.Context, r *http.Request) (*retryRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}

	var ti, ri platform.ID
	if err := ti.DecodeFromString(tid); err != nil {
		return nil, err
	}
	if err := ri.DecodeFromString(rid); err != nil {
		return nil, err
	}

	return &retryRunRequest{
		RunID:  ri,
		TaskID: ti,
	}, nil
}

func (h *TaskHandler) populateTaskCreateOrg(ctx context.Context, tc *platform.TaskCreate) error {
	if tc.OrganizationID.Valid() && tc.Organization != "" {
		return nil
	}

	if !tc.OrganizationID.Valid() && tc.Organization == "" {
		return errors.New("missing orgID and organization name")
	}

	if tc.OrganizationID.Valid() {
		o, err := h.OrganizationService.FindOrganizationByID(ctx, tc.OrganizationID)
		if err != nil {
			return err
		}
		tc.Organization = o.Name
	} else {
		o, err := h.OrganizationService.FindOrganization(ctx, platform.OrganizationFilter{Name: &tc.Organization})
		if err != nil {
			return err
		}
		tc.OrganizationID = o.ID
	}
	return nil
}

// getAuthorizationForTask looks up the authorization associated with taskID,
// ensuring that the authorizer on ctx is allowed to view the task and the authorization.
//
// This method returns a *platform.Error, suitable for directly passing to h.HandleHTTPError.
func (h *TaskHandler) getAuthorizationForTask(ctx context.Context, auth platform.Authorizer, taskID platform.ID) (*platform.Authorization, *platform.Error) {
	sess, ok := auth.(*platform.Session)
	if !ok {
		return nil, &platform.Error{
			Code: platform.EUnauthorized,
			Msg:  "unable to authorize session",
		}
	}
	// First look up the task, if we're allowed.
	// This assumes h.TaskService validates access.
	t, err := h.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, &platform.Error{
			Err:  err,
			Code: platform.EUnauthorized,
			Msg:  "task ID unknown or unauthorized",
		}
	}

	return sess.EphemeralAuth(t.OrganizationID), nil
}

// TaskService connects to Influx via HTTP using tokens to manage tasks.
type TaskService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// FindTaskByID returns a single task
func (t TaskService) FindTaskByID(ctx context.Context, id platform.ID) (*platform.Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(t.Addr, taskIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		if platform.ErrorCode(err) == platform.ENotFound {
			// ErrTaskNotFound is expected as part of the FindTaskByID contract,
			// so return that actual error instead of a different error that looks like it.
			// TODO cleanup backend task service error implementation
			return nil, influxdb.ErrTaskNotFound
		}
		return nil, err
	}

	var tr taskResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, err
	}

	return &tr.Task, nil
}

// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
// of matching tasks.
func (t TaskService) FindTasks(ctx context.Context, filter platform.TaskFilter) ([]*platform.Task, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(t.Addr, tasksPath)
	if err != nil {
		return nil, 0, err
	}

	val := url.Values{}
	if filter.After != nil {
		val.Add("after", filter.After.String())
	}
	if filter.OrganizationID != nil {
		val.Add("orgID", filter.OrganizationID.String())
	}
	if filter.Organization != "" {
		val.Add("org", filter.Organization)
	}
	if filter.User != nil {
		val.Add("user", filter.User.String())
	}
	if filter.Limit != 0 {
		val.Add("limit", strconv.Itoa(filter.Limit))
	}

	if filter.Type != nil {
		val.Add("type", *filter.Type)
	}

	u.RawQuery = val.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var tr tasksResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, 0, err
	}

	tasks := make([]*platform.Task, len(tr.Tasks))
	for i := range tr.Tasks {
		tasks[i] = &tr.Tasks[i].Task
	}
	return tasks, len(tasks), nil
}

// CreateTask creates a new task.
func (t TaskService) CreateTask(ctx context.Context, tc platform.TaskCreate) (*platform.Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if tc.Token == "" {
		return nil, influxdb.ErrMissingToken
	}

	u, err := NewURL(t.Addr, tasksPath)
	if err != nil {
		return nil, err
	}

	taskBytes, err := json.Marshal(tc)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(taskBytes))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var tr taskResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, err
	}
	return &tr.Task, nil
}

// UpdateTask updates a single task with changeset.
func (t TaskService) UpdateTask(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(t.Addr, taskIDPath(id))
	if err != nil {
		return nil, err
	}

	taskBytes, err := json.Marshal(upd)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(taskBytes))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var tr taskResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, err
	}

	return &tr.Task, nil
}

// DeleteTask removes a task by ID and purges all associated data and scheduled runs.
func (t TaskService) DeleteTask(ctx context.Context, id platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(t.Addr, taskIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckErrorStatus(http.StatusNoContent, resp)
}

// FindLogs returns logs for a run.
func (t TaskService) FindLogs(ctx context.Context, filter platform.LogFilter) ([]*platform.Log, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if !filter.Task.Valid() {
		return nil, 0, errors.New("task ID required")
	}

	var urlPath string
	if filter.Run == nil {
		urlPath = path.Join(taskIDPath(filter.Task), "logs")
	} else {
		urlPath = path.Join(taskIDRunIDPath(filter.Task, *filter.Run), "logs")
	}

	u, err := NewURL(t.Addr, urlPath)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var logs getLogsResponse
	if err := json.NewDecoder(resp.Body).Decode(&logs); err != nil {
		return nil, 0, err
	}

	return logs.Events, len(logs.Events), nil
}

// FindRuns returns a list of runs that match a filter and the total count of returned runs.
func (t TaskService) FindRuns(ctx context.Context, filter platform.RunFilter) ([]*platform.Run, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if !filter.Task.Valid() {
		return nil, 0, errors.New("task ID required")
	}

	u, err := NewURL(t.Addr, taskIDRunsPath(filter.Task))
	if err != nil {
		return nil, 0, err
	}

	val := url.Values{}
	if filter.After != nil {
		val.Set("after", filter.After.String())
	}

	if filter.Limit < 0 || filter.Limit > influxdb.TaskMaxPageSize {
		return nil, 0, influxdb.ErrOutOfBoundsLimit
	}
	val.Set("limit", strconv.Itoa(filter.Limit))

	u.RawQuery = val.Encode()
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var rs runsResponse
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, 0, err
	}

	runs := make([]*platform.Run, len(rs.Runs))
	for i := range rs.Runs {
		runs[i] = &rs.Runs[i].Run
	}

	return runs, len(runs), nil
}

// FindRunByID returns a single run of a specific task.
func (t TaskService) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(t.Addr, taskIDRunIDPath(taskID, runID))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		if platform.ErrorCode(err) == platform.ENotFound {
			// ErrRunNotFound is expected as part of the FindRunByID contract,
			// so return that actual error instead of a different error that looks like it.
			// TODO cleanup backend error implementation
			return nil, platform.ErrRunNotFound
		}

		return nil, err
	}
	var rs = &runResponse{}
	if err := json.NewDecoder(resp.Body).Decode(rs); err != nil {
		return nil, err
	}
	return &rs.Run, nil
}

// RetryRun creates and returns a new run (which is a retry of another run).
func (t TaskService) RetryRun(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	p := path.Join(taskIDRunIDPath(taskID, runID), "retry")
	u, err := NewURL(t.Addr, p)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}

	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		if platform.ErrorCode(err) == platform.ENotFound {
			// ErrRunNotFound is expected as part of the RetryRun contract,
			// so return that actual error instead of a different error that looks like it.
			// TODO cleanup backend task error implementation
			return nil, platform.ErrRunNotFound
		}
		// RequestStillQueuedError is also part of the contract.
		if e := backend.ParseRequestStillQueuedError(err.Error()); e != nil {
			return nil, *e
		}

		return nil, err
	}

	rs := &runResponse{}
	if err := json.NewDecoder(resp.Body).Decode(rs); err != nil {
		return nil, err
	}
	return &rs.Run, nil
}

// ForceRun starts a run manually right now.
func (t TaskService) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*platform.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(t.Addr, taskIDRunsPath(taskID))
	if err != nil {
		return nil, err
	}

	body := fmt.Sprintf(`{"scheduledFor": %q}`, time.Unix(scheduledFor, 0).UTC().Format(time.RFC3339))
	req, err := http.NewRequest("POST", u.String(), strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		if platform.ErrorCode(err) == platform.ENotFound {
			// ErrRunNotFound is expected as part of the RetryRun contract,
			// so return that actual error instead of a different error that looks like it.
			return nil, influxdb.ErrRunNotFound
		}

		// RequestStillQueuedError is also part of the contract.
		if e := backend.ParseRequestStillQueuedError(err.Error()); e != nil {
			return nil, *e
		}

		return nil, err
	}

	rs := &runResponse{}
	if err := json.NewDecoder(resp.Body).Decode(rs); err != nil {
		return nil, err
	}
	return &rs.Run, nil
}

func cancelPath(taskID, runID platform.ID) string {
	return path.Join(taskID.String(), runID.String())
}

// CancelRun stops a longer running run.
func (t TaskService) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(t.Addr, cancelPath(taskID, runID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	SetToken(t.Token, req)

	hc := NewClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return err
	}

	return nil
}

func taskIDPath(id platform.ID) string {
	return path.Join(tasksPath, id.String())
}

func taskIDRunsPath(id platform.ID) string {
	return path.Join(tasksPath, id.String(), "runs")
}

func taskIDRunIDPath(taskID, runID platform.ID) string {
	return path.Join(tasksPath, taskID.String(), "runs", runID.String())
}
