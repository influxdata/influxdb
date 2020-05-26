package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

// TaskBackend is all services and associated parameters required to construct
// the TaskHandler.
type TaskBackend struct {
	influxdb.HTTPErrorHandler
	log *zap.Logger

	AlgoWProxy                 FeatureProxyHandler
	TaskService                influxdb.TaskService
	AuthorizationService       influxdb.AuthorizationService
	OrganizationService        influxdb.OrganizationService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	BucketService              influxdb.BucketService
}

// NewTaskBackend returns a new instance of TaskBackend.
func NewTaskBackend(log *zap.Logger, b *APIBackend) *TaskBackend {
	return &TaskBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        log,
		AlgoWProxy:                 b.AlgoWProxy,
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
	influxdb.HTTPErrorHandler
	log *zap.Logger

	TaskService                influxdb.TaskService
	AuthorizationService       influxdb.AuthorizationService
	OrganizationService        influxdb.OrganizationService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	BucketService              influxdb.BucketService
}

const (
	prefixTasks            = "/api/v2/tasks"
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
func NewTaskHandler(log *zap.Logger, b *TaskBackend) *TaskHandler {
	h := &TaskHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		TaskService:                b.TaskService,
		AuthorizationService:       b.AuthorizationService,
		OrganizationService:        b.OrganizationService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		BucketService:              b.BucketService,
	}

	h.HandlerFunc("GET", prefixTasks, h.handleGetTasks)
	h.Handler("POST", prefixTasks, withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.handlePostTask)))

	h.HandlerFunc("GET", tasksIDPath, h.handleGetTask)
	h.Handler("PATCH", tasksIDPath, withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.handleUpdateTask)))
	h.HandlerFunc("DELETE", tasksIDPath, h.handleDeleteTask)

	h.HandlerFunc("GET", tasksIDLogsPath, h.handleGetLogs)
	h.HandlerFunc("GET", tasksIDRunsIDLogsPath, h.handleGetLogs)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.TasksResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", tasksIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", tasksIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", tasksIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.TasksResourceType,
		UserType:                   influxdb.Owner,
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
		log:              b.log.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.TasksResourceType,
	}
	h.HandlerFunc("GET", tasksIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", tasksIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", tasksIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

// Task is a package-specific Task format that preserves the expected format for the API,
// where time values are represented as strings
type Task struct {
	ID              influxdb.ID            `json:"id"`
	OrganizationID  influxdb.ID            `json:"orgID"`
	Organization    string                 `json:"org"`
	OwnerID         influxdb.ID            `json:"ownerID"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description,omitempty"`
	Status          string                 `json:"status"`
	Flux            string                 `json:"flux"`
	Every           string                 `json:"every,omitempty"`
	Cron            string                 `json:"cron,omitempty"`
	Offset          string                 `json:"offset,omitempty"`
	LatestCompleted string                 `json:"latestCompleted,omitempty"`
	LastRunStatus   string                 `json:"lastRunStatus,omitempty"`
	LastRunError    string                 `json:"lastRunError,omitempty"`
	CreatedAt       string                 `json:"createdAt,omitempty"`
	UpdatedAt       string                 `json:"updatedAt,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

type taskResponse struct {
	Links  map[string]string `json:"links"`
	Labels []influxdb.Label  `json:"labels"`
	Task
}

// NewFrontEndTask converts a internal task type to a task that we want to display to users
func NewFrontEndTask(t influxdb.Task) Task {
	latestCompleted := ""
	if !t.LatestCompleted.IsZero() {
		latestCompleted = t.LatestCompleted.Format(time.RFC3339)
	}
	createdAt := ""
	if !t.CreatedAt.IsZero() {
		createdAt = t.CreatedAt.Format(time.RFC3339)
	}
	updatedAt := ""
	if !t.UpdatedAt.IsZero() {
		updatedAt = t.UpdatedAt.Format(time.RFC3339)
	}
	offset := ""
	if t.Offset != 0*time.Second {
		offset = customParseDuration(t.Offset)
	}

	return Task{
		ID:              t.ID,
		OrganizationID:  t.OrganizationID,
		Organization:    t.Organization,
		OwnerID:         t.OwnerID,
		Name:            t.Name,
		Description:     t.Description,
		Status:          t.Status,
		Flux:            t.Flux,
		Every:           t.Every,
		Cron:            t.Cron,
		Offset:          offset,
		LatestCompleted: latestCompleted,
		LastRunStatus:   t.LastRunStatus,
		LastRunError:    t.LastRunError,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
		Metadata:        t.Metadata,
	}
}

func customParseDuration(d time.Duration) string {
	str := ""
	if d < 0 {
		str = "-"
		d = d * -1
	}

	// parse hours
	hours := d / time.Hour
	if hours != 0 {
		str = fmt.Sprintf("%s%dh", str, hours)
	}
	if d%time.Hour == 0 {
		return str
	}
	// parse minutes
	d = d - (time.Duration(hours) * time.Hour)

	min := d / time.Minute
	if min != 0 {
		str = fmt.Sprintf("%s%dm", str, min)
	}
	if d%time.Minute == 0 {
		return str
	}

	// parse seconds
	d = d - time.Duration(min)*time.Minute
	sec := d / time.Second

	if sec != 0 {
		str = fmt.Sprintf("%s%ds", str, sec)
	}
	return str
}

func newTaskResponse(t influxdb.Task, labels []*influxdb.Label) taskResponse {
	response := taskResponse{
		Links: map[string]string{
			"self":    fmt.Sprintf("/api/v2/tasks/%s", t.ID),
			"members": fmt.Sprintf("/api/v2/tasks/%s/members", t.ID),
			"owners":  fmt.Sprintf("/api/v2/tasks/%s/owners", t.ID),
			"labels":  fmt.Sprintf("/api/v2/tasks/%s/labels", t.ID),
			"runs":    fmt.Sprintf("/api/v2/tasks/%s/runs", t.ID),
			"logs":    fmt.Sprintf("/api/v2/tasks/%s/logs", t.ID),
		},
		Task:   NewFrontEndTask(t),
		Labels: []influxdb.Label{},
	}

	for _, l := range labels {
		response.Labels = append(response.Labels, *l)
	}

	return response
}

func newTasksPagingLinks(basePath string, ts []*influxdb.Task, f influxdb.TaskFilter) *influxdb.PagingLinks {
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

	links := &influxdb.PagingLinks{
		Self: self,
		Next: next,
	}

	return links
}

type tasksResponse struct {
	Links *influxdb.PagingLinks `json:"links"`
	Tasks []taskResponse        `json:"tasks"`
}

func newTasksResponse(ctx context.Context, ts []*influxdb.Task, f influxdb.TaskFilter, labelService influxdb.LabelService) tasksResponse {
	rs := tasksResponse{
		Links: newTasksPagingLinks(prefixTasks, ts, f),
		Tasks: make([]taskResponse, len(ts)),
	}

	for i := range ts {
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: ts[i].ID, ResourceType: influxdb.TasksResourceType})
		rs.Tasks[i] = newTaskResponse(*ts[i], labels)
	}
	return rs
}

type runResponse struct {
	Links map[string]string `json:"links,omitempty"`
	httpRun
}

// httpRun is a version of the Run object used to communicate over the API
// it uses a pointer to a time.Time instead of a time.Time so that we can pass a nil
// value for empty time values
type httpRun struct {
	ID           influxdb.ID    `json:"id,omitempty"`
	TaskID       influxdb.ID    `json:"taskID"`
	Status       string         `json:"status"`
	ScheduledFor *time.Time     `json:"scheduledFor"`
	StartedAt    *time.Time     `json:"startedAt,omitempty"`
	FinishedAt   *time.Time     `json:"finishedAt,omitempty"`
	RequestedAt  *time.Time     `json:"requestedAt,omitempty"`
	Log          []influxdb.Log `json:"log,omitempty"`
}

func newRunResponse(r influxdb.Run) runResponse {
	run := httpRun{
		ID:           r.ID,
		TaskID:       r.TaskID,
		Status:       r.Status,
		Log:          r.Log,
		ScheduledFor: &r.ScheduledFor,
	}

	if !r.StartedAt.IsZero() {
		run.StartedAt = &r.StartedAt
	}
	if !r.FinishedAt.IsZero() {
		run.FinishedAt = &r.FinishedAt
	}
	if !r.RequestedAt.IsZero() {
		run.RequestedAt = &r.RequestedAt
	}

	return runResponse{
		Links: map[string]string{
			"self":  fmt.Sprintf("/api/v2/tasks/%s/runs/%s", r.TaskID, r.ID),
			"task":  fmt.Sprintf("/api/v2/tasks/%s", r.TaskID),
			"logs":  fmt.Sprintf("/api/v2/tasks/%s/runs/%s/logs", r.TaskID, r.ID),
			"retry": fmt.Sprintf("/api/v2/tasks/%s/runs/%s/retry", r.TaskID, r.ID),
		},
		httpRun: run,
	}
}

func convertRun(r httpRun) *influxdb.Run {
	run := &influxdb.Run{
		ID:     r.ID,
		TaskID: r.TaskID,
		Status: r.Status,
		Log:    r.Log,
	}

	if r.StartedAt != nil {
		run.StartedAt = *r.StartedAt
	}

	if r.FinishedAt != nil {
		run.FinishedAt = *r.FinishedAt
	}

	if r.RequestedAt != nil {
		run.RequestedAt = *r.RequestedAt
	}

	if r.ScheduledFor != nil {
		run.ScheduledFor = *r.ScheduledFor
	}

	return run
}

type runsResponse struct {
	Links map[string]string `json:"links"`
	Runs  []*runResponse    `json:"runs"`
}

func newRunsResponse(rs []*influxdb.Run, taskID influxdb.ID) runsResponse {
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
	req, err := decodeGetTasksRequest(ctx, r, h.OrganizationService)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	tasks, _, err := h.TaskService.FindTasks(ctx, req.filter)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Tasks retrived", zap.String("tasks", fmt.Sprint(tasks)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTasksResponse(ctx, tasks, req.filter, h.LabelService)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getTasksRequest struct {
	filter influxdb.TaskFilter
}

func decodeGetTasksRequest(ctx context.Context, r *http.Request, orgs influxdb.OrganizationService) (*getTasksRequest, error) {
	qp := r.URL.Query()
	req := &getTasksRequest{}

	if after := qp.Get("after"); after != "" {
		id, err := influxdb.IDFromString(after)
		if err != nil {
			return nil, err
		}
		req.filter.After = id
	}

	if orgName := qp.Get("org"); orgName != "" {
		o, err := orgs.FindOrganization(ctx, influxdb.OrganizationFilter{Name: &orgName})
		if err != nil {
			if pErr, ok := err.(*influxdb.Error); ok && pErr != nil {
				if kv.IsNotFound(err) || pErr.Code == influxdb.EUnauthorized {
					return nil, &influxdb.Error{
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
		orgID, err := influxdb.IDFromString(oid)
		if err != nil {
			return nil, err
		}
		req.filter.OrganizationID = orgID
	}

	if userID := qp.Get("user"); userID != "" {
		id, err := influxdb.IDFromString(userID)
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
		if lim < 1 || lim > influxdb.TaskMaxPageSize {
			return nil, &influxdb.Error{
				Code: influxdb.EUnprocessableEntity,
				Msg:  fmt.Sprintf("limit must be between 1 and %d", influxdb.TaskMaxPageSize),
			}
		}
		req.filter.Limit = lim
	} else {
		req.filter.Limit = influxdb.TaskDefaultPageSize
	}

	if status := qp.Get("status"); status == "active" {
		req.filter.Status = &status
	} else if status := qp.Get("status"); status == "inactive" {
		req.filter.Status = &status
	}

	// the task api can only create or lookup system tasks.
	req.filter.Type = &influxdb.TaskSystemType

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	return req, nil
}

func (h *TaskHandler) handlePostTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostTaskRequest(ctx, r)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.populateTaskCreateOrg(ctx, &req.TaskCreate); err != nil {
		err = &influxdb.Error{
			Err: err,
			Msg: "could not identify organization",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if !req.TaskCreate.OrganizationID.Valid() {
		err := &influxdb.Error{
			Code: influxdb.EInvalid,
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
			h.log.Error("Failed authentication", zap.Errors("error messages", []error{err, e.AuthzError()}))
		}

		// if the error is not already a influxdb.error then make it into one
		if _, ok := err.(*influxdb.Error); !ok {
			err = &influxdb.Error{
				Err:  err,
				Code: influxdb.EInternal,
				Msg:  "failed to create task",
			}
		}

		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newTaskResponse(*task, []*influxdb.Label{})); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type postTaskRequest struct {
	TaskCreate influxdb.TaskCreate
}

func decodePostTaskRequest(ctx context.Context, r *http.Request) (*postTaskRequest, error) {
	var tc influxdb.TaskCreate
	if err := json.NewDecoder(r.Body).Decode(&tc); err != nil {
		return nil, err
	}

	// pull auth from ctx, populate OwnerID
	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}
	tc.OwnerID = auth.GetUserID()
	// when creating a task we set the type so we can filter later.
	tc.Type = influxdb.TaskSystemType

	if err := tc.Validate(); err != nil {
		return nil, err
	}

	return &postTaskRequest{
		TaskCreate: tc,
	}, nil
}

func (h *TaskHandler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetTaskRequest(ctx, r)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	task, err := h.TaskService.FindTaskByID(ctx, req.TaskID)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.ENotFound,
			Msg:  "failed to find task",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: task.ID, ResourceType: influxdb.TasksResourceType})
	if err != nil {
		err = &influxdb.Error{
			Err: err,
			Msg: "failed to find resource labels",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Task retrieved", zap.String("tasks", fmt.Sprint(task)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTaskResponse(*task, labels)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getTaskRequest struct {
	TaskID influxdb.ID
}

func decodeGetTaskRequest(ctx context.Context, r *http.Request) (*getTaskRequest, error) {
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

	req := &getTaskRequest{
		TaskID: i,
	}

	return req, nil
}

func (h *TaskHandler) handleUpdateTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeUpdateTaskRequest(ctx, r)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	task, err := h.TaskService.UpdateTask(ctx, req.TaskID, req.Update)
	if err != nil {
		err := &influxdb.Error{
			Err: err,
			Msg: "failed to update task",
		}
		if err.Err == influxdb.ErrTaskNotFound {
			err.Code = influxdb.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: task.ID, ResourceType: influxdb.TasksResourceType})
	if err != nil {
		err = &influxdb.Error{
			Err: err,
			Msg: "failed to find resource labels",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Tasks updated", zap.String("task", fmt.Sprint(task)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTaskResponse(*task, labels)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type updateTaskRequest struct {
	Update influxdb.TaskUpdate
	TaskID influxdb.ID
}

func decodeUpdateTaskRequest(ctx context.Context, r *http.Request) (*updateTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd influxdb.TaskUpdate
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
	req, err := decodeDeleteTaskRequest(ctx, r)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.TaskService.DeleteTask(ctx, req.TaskID); err != nil {
		err := &influxdb.Error{
			Err: err,
			Msg: "failed to delete task",
		}
		if err.Err == influxdb.ErrTaskNotFound {
			err.Code = influxdb.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Tasks deleted", zap.String("taskID", fmt.Sprint(req.TaskID)))
	w.WriteHeader(http.StatusNoContent)
}

type deleteTaskRequest struct {
	TaskID influxdb.ID
}

func decodeDeleteTaskRequest(ctx context.Context, r *http.Request) (*deleteTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i influxdb.ID
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
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EUnauthorized,
			Msg:  "failed to get authorizer",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if k := auth.Kind(); k != influxdb.AuthorizationKind {
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
		err := &influxdb.Error{
			Err: err,
			Msg: "failed to find task logs",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrNoRunsFound {
			err.Code = influxdb.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, &getLogsResponse{Events: logs}); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getLogsRequest struct {
	filter influxdb.LogFilter
}

type getLogsResponse struct {
	Events []*influxdb.Log `json:"events"`
}

func decodeGetLogsRequest(ctx context.Context, r *http.Request) (*getLogsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	req := &getLogsRequest{}
	taskID, err := influxdb.IDFromString(id)
	if err != nil {
		return nil, err
	}
	req.filter.Task = *taskID

	if runID := params.ByName("rid"); runID != "" {
		id, err := influxdb.IDFromString(runID)
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
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EUnauthorized,
			Msg:  "failed to get authorizer",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if k := auth.Kind(); k != influxdb.AuthorizationKind {
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
		err := &influxdb.Error{
			Err: err,
			Msg: "failed to find runs",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrNoRunsFound {
			err.Code = influxdb.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newRunsResponse(runs, req.filter.Task)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getRunsRequest struct {
	filter influxdb.RunFilter
}

func decodeGetRunsRequest(ctx context.Context, r *http.Request) (*getRunsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	req := &getRunsRequest{}
	taskID, err := influxdb.IDFromString(id)
	if err != nil {
		return nil, err
	}
	req.filter.Task = *taskID

	qp := r.URL.Query()

	if id := qp.Get("after"); id != "" {
		afterID, err := influxdb.IDFromString(id)
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
		return nil, &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Msg:  "beforeTime must be later than afterTime",
		}
	}

	return req, nil
}

func (h *TaskHandler) handleForceRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeForceRunRequest(ctx, r)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	run, err := h.TaskService.ForceRun(ctx, req.TaskID, req.Timestamp)
	if err != nil {
		err := &influxdb.Error{
			Err: err,
			Msg: "failed to force run",
		}
		if err.Err == influxdb.ErrTaskNotFound {
			err.Code = influxdb.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusCreated, newRunResponse(*run)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type forceRunRequest struct {
	TaskID    influxdb.ID
	Timestamp int64
}

func decodeForceRunRequest(ctx context.Context, r *http.Request) (forceRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return forceRunRequest{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var ti influxdb.ID
	if err := ti.DecodeFromString(tid); err != nil {
		return forceRunRequest{}, err
	}

	var req struct {
		ScheduledFor string `json:"scheduledFor"`
	}

	if r.ContentLength != 0 && r.ContentLength < 1000 { // prevent attempts to use up memory since r.Body should include at most one item (RunManually)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return forceRunRequest{}, err
		}
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
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EUnauthorized,
			Msg:  "failed to get authorizer",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if k := auth.Kind(); k != influxdb.AuthorizationKind {
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
		err := &influxdb.Error{
			Err: err,
			Msg: "failed to find run",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrRunNotFound {
			err.Code = influxdb.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newRunResponse(*run)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getRunRequest struct {
	TaskID influxdb.ID
	RunID  influxdb.ID
}

func decodeGetRunRequest(ctx context.Context, r *http.Request) (*getRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}

	var ti, ri influxdb.ID
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
	RunID  influxdb.ID
	TaskID influxdb.ID
}

func decodeCancelRunRequest(ctx context.Context, r *http.Request) (*cancelRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}
	tid := params.ByName("id")
	if tid == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(rid); err != nil {
		return nil, err
	}
	var t influxdb.ID
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
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	err = h.TaskService.CancelRun(ctx, req.TaskID, req.RunID)
	if err != nil {
		err := &influxdb.Error{
			Err: err,
			Msg: "failed to cancel run",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrRunNotFound {
			err.Code = influxdb.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *TaskHandler) handleRetryRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeRetryRunRequest(ctx, r)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &influxdb.Error{
			Err:  err,
			Code: influxdb.EUnauthorized,
			Msg:  "failed to get authorizer",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if k := auth.Kind(); k != influxdb.AuthorizationKind {
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
		err := &influxdb.Error{
			Err: err,
			Msg: "failed to retry run",
		}
		if err.Err == influxdb.ErrTaskNotFound || err.Err == influxdb.ErrRunNotFound {
			err.Code = influxdb.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusOK, newRunResponse(*run)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type retryRunRequest struct {
	RunID, TaskID influxdb.ID
}

func decodeRetryRunRequest(ctx context.Context, r *http.Request) (*retryRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}

	var ti, ri influxdb.ID
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

func (h *TaskHandler) populateTaskCreateOrg(ctx context.Context, tc *influxdb.TaskCreate) error {
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
		o, err := h.OrganizationService.FindOrganization(ctx, influxdb.OrganizationFilter{Name: &tc.Organization})
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
// This method returns a *influxdb.Error, suitable for directly passing to h.HandleHTTPError.
func (h *TaskHandler) getAuthorizationForTask(ctx context.Context, auth influxdb.Authorizer, taskID influxdb.ID) (*influxdb.Authorization, *influxdb.Error) {
	sess, ok := auth.(*influxdb.Session)
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  "unable to authorize session",
		}
	}
	// First look up the task, if we're allowed.
	// This assumes h.TaskService validates access.
	t, err := h.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, &influxdb.Error{
			Err:  err,
			Code: influxdb.EUnauthorized,
			Msg:  "task ID unknown or unauthorized",
		}
	}

	return sess.EphemeralAuth(t.OrganizationID), nil
}

// TaskService connects to Influx via HTTP using tokens to manage tasks.
type TaskService struct {
	Client *httpc.Client
}

// FindTaskByID returns a single task
func (t TaskService) FindTaskByID(ctx context.Context, id influxdb.ID) (*Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var tr taskResponse
	err := t.Client.Get(taskIDPath(id)).DecodeJSON(&tr).Do(ctx)
	if err != nil {
		return nil, err
	}

	return &tr.Task, nil
}

// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
// of matching tasks.
func (t TaskService) FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]Task, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// slice of 2-capacity string slices for storing parameter key-value pairs
	var params [][2]string

	if filter.After != nil {
		params = append(params, [2]string{"after", filter.After.String()})
	}
	if filter.OrganizationID != nil {
		params = append(params, [2]string{"orgID", filter.OrganizationID.String()})
	}
	if filter.Organization != "" {
		params = append(params, [2]string{"org", filter.Organization})
	}
	if filter.User != nil {
		params = append(params, [2]string{"user", filter.User.String()})
	}
	if filter.Limit != 0 {
		params = append(params, [2]string{"limit", strconv.Itoa(filter.Limit)})
	}

	if filter.Status != nil {
		params = append(params, [2]string{"status", *filter.Status})
	}

	if filter.Type != nil {
		params = append(params, [2]string{"type", *filter.Type})
	}

	var tr tasksResponse
	err := t.Client.
		Get(prefixTasks).
		QueryParams(params...).
		DecodeJSON(&tr).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	tasks := make([]Task, len(tr.Tasks))
	for i := range tr.Tasks {
		tasks[i] = tr.Tasks[i].Task
	}
	return tasks, len(tasks), nil
}

// CreateTask creates a new task.
func (t TaskService) CreateTask(ctx context.Context, tc influxdb.TaskCreate) (*Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	var tr taskResponse

	err := t.Client.
		PostJSON(tc, prefixTasks).
		DecodeJSON(&tr).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &tr.Task, nil
}

// UpdateTask updates a single task with changeset.
func (t TaskService) UpdateTask(ctx context.Context, id influxdb.ID, upd influxdb.TaskUpdate) (*Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var tr taskResponse
	err := t.Client.
		PatchJSON(&upd, taskIDPath(id)).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &tr.Task, nil
}

// DeleteTask removes a task by ID and purges all associated data and scheduled runs.
func (t TaskService) DeleteTask(ctx context.Context, id influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return t.Client.
		Delete(taskIDPath(id)).
		Do(ctx)
}

// FindLogs returns logs for a run.
func (t TaskService) FindLogs(ctx context.Context, filter influxdb.LogFilter) ([]*influxdb.Log, int, error) {
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

	var logs getLogsResponse
	err := t.Client.
		Get(urlPath).
		DecodeJSON(&logs).
		Do(ctx)

	if err != nil {
		return nil, 0, err
	}

	return logs.Events, len(logs.Events), nil
}

// FindRuns returns a list of runs that match a filter and the total count of returned runs.
func (t TaskService) FindRuns(ctx context.Context, filter influxdb.RunFilter) ([]*influxdb.Run, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var params [][2]string

	if !filter.Task.Valid() {
		return nil, 0, errors.New("task ID required")
	}

	if filter.After != nil {
		params = append(params, [2]string{"after", filter.After.String()})
	}

	if filter.Limit < 0 || filter.Limit > influxdb.TaskMaxPageSize {
		return nil, 0, influxdb.ErrOutOfBoundsLimit
	}

	params = append(params, [2]string{"limit", strconv.Itoa(filter.Limit)})

	var rs runsResponse
	err := t.Client.
		Get(taskIDRunsPath(filter.Task)).
		QueryParams(params...).
		DecodeJSON(&rs).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	runs := make([]*influxdb.Run, len(rs.Runs))
	for i := range rs.Runs {
		runs[i] = convertRun(rs.Runs[i].httpRun)
	}

	return runs, len(runs), nil
}

// FindRunByID returns a single run of a specific task.
func (t TaskService) FindRunByID(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var rs = &runResponse{}
	err := t.Client.
		Get(taskIDRunIDPath(taskID, runID)).
		DecodeJSON(rs).
		Do(ctx)

	if err != nil {
		if influxdb.ErrorCode(err) == influxdb.ENotFound {
			// ErrRunNotFound is expected as part of the FindRunByID contract,
			// so return that actual error instead of a different error that looks like it.
			// TODO cleanup backend error implementation
			return nil, influxdb.ErrRunNotFound
		}

		return nil, err
	}

	return convertRun(rs.httpRun), nil
}

// RetryRun creates and returns a new run (which is a retry of another run).
func (t TaskService) RetryRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var rs runResponse
	err := t.Client.
		Post(nil, path.Join(taskIDRunIDPath(taskID, runID), "retry")).
		DecodeJSON(&rs).
		Do(ctx)

	if err != nil {
		if influxdb.ErrorCode(err) == influxdb.ENotFound {
			// ErrRunNotFound is expected as part of the RetryRun contract,
			// so return that actual error instead of a different error that looks like it.
			// TODO cleanup backend task error implementation
			return nil, influxdb.ErrRunNotFound
		}
		// RequestStillQueuedError is also part of the contract.
		if e := influxdb.ParseRequestStillQueuedError(err.Error()); e != nil {
			return nil, *e
		}

		return nil, err
	}

	return convertRun(rs.httpRun), nil
}

// ForceRun starts a run manually right now.
func (t TaskService) ForceRun(ctx context.Context, taskID influxdb.ID, scheduledFor int64) (*influxdb.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	type body struct {
		scheduledFor string
	}
	b := body{scheduledFor: time.Unix(scheduledFor, 0).UTC().Format(time.RFC3339)}

	rs := &runResponse{}
	err := t.Client.
		PostJSON(b, taskIDRunsPath(taskID)).
		DecodeJSON(&rs).
		Do(ctx)

	if err != nil {
		if influxdb.ErrorCode(err) == influxdb.ENotFound {
			// ErrRunNotFound is expected as part of the RetryRun contract,
			// so return that actual error instead of a different error that looks like it.
			return nil, influxdb.ErrRunNotFound
		}

		// RequestStillQueuedError is also part of the contract.
		if e := influxdb.ParseRequestStillQueuedError(err.Error()); e != nil {
			return nil, *e
		}

		return nil, err
	}

	return convertRun(rs.httpRun), nil
}

func cancelPath(taskID, runID influxdb.ID) string {
	return path.Join(taskID.String(), runID.String())
}

// CancelRun stops a longer running run.
func (t TaskService) CancelRun(ctx context.Context, taskID, runID influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	err := t.Client.
		Delete(cancelPath(taskID, runID)).
		Do(ctx)

	if err != nil {
		return err
	}

	return nil
}

func taskIDPath(id influxdb.ID) string {
	return path.Join(prefixTasks, id.String())
}

func taskIDRunsPath(id influxdb.ID) string {
	return path.Join(prefixTasks, id.String(), "runs")
}

func taskIDRunIDPath(taskID, runID influxdb.ID) string {
	return path.Join(prefixTasks, taskID.String(), "runs", runID.String())
}
