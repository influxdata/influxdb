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

	platform "github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	kerrors "github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// TaskHandler represents an HTTP API handler for tasks.
type TaskHandler struct {
	*httprouter.Router
	logger *zap.Logger

	TaskService                platform.TaskService
	AuthorizationService       platform.AuthorizationService
	OrganizationService        platform.OrganizationService
	UserResourceMappingService platform.UserResourceMappingService
	LabelService               platform.LabelService
	UserService                platform.UserService
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
func NewTaskHandler(mappingService platform.UserResourceMappingService, labelService platform.LabelService, logger *zap.Logger, userService platform.UserService) *TaskHandler {
	h := &TaskHandler{
		logger: logger,
		Router: NewRouter(),

		UserResourceMappingService: mappingService,
		LabelService:               labelService,
		UserService:                userService,
	}

	h.HandlerFunc("GET", tasksPath, h.handleGetTasks)
	h.HandlerFunc("POST", tasksPath, h.handlePostTask)

	h.HandlerFunc("GET", tasksIDPath, h.handleGetTask)
	h.HandlerFunc("PATCH", tasksIDPath, h.handleUpdateTask)
	h.HandlerFunc("DELETE", tasksIDPath, h.handleDeleteTask)

	h.HandlerFunc("GET", tasksIDLogsPath, h.handleGetLogs)
	h.HandlerFunc("GET", tasksIDRunsIDLogsPath, h.handleGetLogs)

	h.HandlerFunc("POST", tasksIDMembersPath, newPostMemberHandler(h.UserResourceMappingService, h.UserService, platform.TasksResourceType, platform.Member))
	h.HandlerFunc("GET", tasksIDMembersPath, newGetMembersHandler(h.UserResourceMappingService, h.UserService, platform.TasksResourceType, platform.Member))
	h.HandlerFunc("DELETE", tasksIDMembersIDPath, newDeleteMemberHandler(h.UserResourceMappingService, platform.Member))

	h.HandlerFunc("POST", tasksIDOwnersPath, newPostMemberHandler(h.UserResourceMappingService, h.UserService, platform.TasksResourceType, platform.Owner))
	h.HandlerFunc("GET", tasksIDOwnersPath, newGetMembersHandler(h.UserResourceMappingService, h.UserService, platform.TasksResourceType, platform.Owner))
	h.HandlerFunc("DELETE", tasksIDOwnersIDPath, newDeleteMemberHandler(h.UserResourceMappingService, platform.Owner))

	h.HandlerFunc("GET", tasksIDRunsPath, h.handleGetRuns)
	h.HandlerFunc("POST", tasksIDRunsPath, h.handleForceRun)
	h.HandlerFunc("GET", tasksIDRunsIDPath, h.handleGetRun)
	h.HandlerFunc("POST", tasksIDRunsIDRetryPath, h.handleRetryRun)
	h.HandlerFunc("DELETE", tasksIDRunsIDPath, h.handleCancelRun)

	h.HandlerFunc("GET", tasksIDLabelsPath, newGetLabelsHandler(h.LabelService))
	h.HandlerFunc("POST", tasksIDLabelsPath, newPostLabelHandler(h.LabelService))
	h.HandlerFunc("DELETE", tasksIDLabelsIDPath, newDeleteLabelHandler(h.LabelService))

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

type tasksResponse struct {
	Links map[string]string `json:"links"`
	Tasks []taskResponse    `json:"tasks"`
}

func newTasksResponse(ctx context.Context, ts []*platform.Task, labelService platform.LabelService) tasksResponse {
	// TODO: impl paging links
	/*
	   In swagger, paging links are embedded in a map, like this:
	   "links": {
	       "next": {
	           "href": "string"
	       },
	       "self": {
	           "href": "string"
	       },
	       "prev": {
	           "href": "string"
	       }
	   }

	   But in http services (auth, org, bucket...), links are flat:
	   "links": {
	       "self": "string"
	   }

	   Them need to be unified.
	*/

	rs := tasksResponse{
		Links: map[string]string{
			"self": tasksPath,
		},
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

	req, err := decodeGetTasksRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	tasks, _, err := h.TaskService.FindTasks(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	for _, task := range tasks {
		if err := h.populateOrg(ctx, task); err != nil {
			EncodeError(ctx, err, w)
			return
		}
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newTasksResponse(ctx, tasks, h.LabelService)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type getTasksRequest struct {
	filter platform.TaskFilter
}

func decodeGetTasksRequest(ctx context.Context, r *http.Request) (*getTasksRequest, error) {
	qp := r.URL.Query()
	req := &getTasksRequest{}

	if after := qp.Get("after"); after != "" {
		id, err := platform.IDFromString(after)
		if err != nil {
			return nil, err
		}
		req.filter.After = id
	}

	if orgID := qp.Get("organization"); orgID != "" {
		id, err := platform.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.Organization = id
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
			return nil, kerrors.InvalidDataf("limit must be between 1 and %d", platform.TaskMaxPageSize)
		}
		req.filter.Limit = lim
	} else {
		req.filter.Limit = platform.TaskDefaultPageSize
	}

	return req, nil
}

func (h *TaskHandler) handlePostTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EUnauthorized,
		}
		EncodeError(ctx, err, w)
		return
	}

	req, err := decodePostTaskRequest(ctx, r)
	if err != nil {
		err = &platform.Error{
			Err:  err,
			Code: platform.EInvalid,
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := h.populateOrg(ctx, req.Task); err != nil {
		err = &platform.Error{
			Err: err,
			Msg: "could not identify organization",
		}
		EncodeError(ctx, err, w)
		return
	}

	if !req.Task.OrganizationID.Valid() {
		err := &platform.Error{
			Code: platform.EInvalid,
			Msg:  "invalid organization id",
		}
		EncodeError(ctx, err, w)
		return
	}

	if !req.Task.Owner.ID.Valid() {
		req.Task.Owner.ID = auth.GetUserID()
	}

	if err := h.TaskService.CreateTask(ctx, req.Task); err != nil {
		if e, ok := err.(AuthzError); ok {
			h.logger.Error("failed authentication", zap.Errors("error messages", []error{err, e.AuthzError()}))
		}
		err = &platform.Error{
			Err: err,
			Msg: "failed to create task",
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newTaskResponse(*req.Task, []*platform.Label{})); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type postTaskRequest struct {
	Task *platform.Task
}

func decodePostTaskRequest(ctx context.Context, r *http.Request) (*postTaskRequest, error) {
	task := &platform.Task{}
	if err := json.NewDecoder(r.Body).Decode(task); err != nil {
		return nil, err
	}

	return &postTaskRequest{
		Task: task,
	}, nil
}

func (h *TaskHandler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetTaskRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	task, err := h.TaskService.FindTaskByID(ctx, req.TaskID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.populateOrg(ctx, task); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: task.ID})
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

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
		return nil, kerrors.InvalidDataf("url missing id")
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

	req, err := decodeUpdateTaskRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	task, err := h.TaskService.UpdateTask(ctx, req.TaskID, req.Update)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: task.ID})
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.populateOrg(ctx, task); err != nil {
		EncodeError(ctx, err, w)
		return
	}

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
		return nil, kerrors.InvalidDataf("you must provide a task ID")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd platform.TaskUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
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
		EncodeError(ctx, err, w)
		return
	}

	if err := h.TaskService.DeleteTask(ctx, req.TaskID); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteTaskRequest struct {
	TaskID platform.ID
}

func decodeDeleteTaskRequest(ctx context.Context, r *http.Request) (*deleteTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("you must provide a task ID")
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

	req, err := decodeGetLogsRequest(ctx, r, h.OrganizationService)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	logs, _, err := h.TaskService.FindLogs(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, logs); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type getLogsRequest struct {
	filter platform.LogFilter
}

func decodeGetLogsRequest(ctx context.Context, r *http.Request, orgs platform.OrganizationService) (*getLogsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("you must provide a task ID")
	}

	req := &getLogsRequest{}
	taskID, err := platform.IDFromString(id)
	if err != nil {
		return nil, err
	}
	req.filter.Task = taskID

	qp := r.URL.Query()

	if orgName := qp.Get("org"); orgName != "" {
		o, err := orgs.FindOrganization(ctx, platform.OrganizationFilter{Name: &orgName})
		if err != nil {
			return nil, err
		}

		req.filter.Org = &o.ID
	} else if oid := qp.Get("orgID"); oid != "" {
		orgID, err := platform.IDFromString(oid)
		if err != nil {
			return nil, err
		}
		req.filter.Org = orgID
	}

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

	req, err := decodeGetRunsRequest(ctx, r, h.OrganizationService)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	runs, _, err := h.TaskService.FindRuns(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newRunsResponse(runs, *req.filter.Task)); err != nil {
		logEncodingError(h.logger, r, err)
		return
	}
}

type getRunsRequest struct {
	filter platform.RunFilter
}

func decodeGetRunsRequest(ctx context.Context, r *http.Request, orgs platform.OrganizationService) (*getRunsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("you must provide a task ID")
	}

	req := &getRunsRequest{}
	taskID, err := platform.IDFromString(id)
	if err != nil {
		return nil, err
	}
	req.filter.Task = taskID

	qp := r.URL.Query()

	if orgName := qp.Get("org"); orgName != "" {
		o, err := orgs.FindOrganization(ctx, platform.OrganizationFilter{Name: &orgName})
		if err != nil {
			return nil, err
		}

		req.filter.Org = &o.ID
	} else if orgID := qp.Get("orgID"); orgID != "" {
		oid, err := platform.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.Org = oid
	}

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

		if i < 1 || i > 100 {
			return nil, kerrors.InvalidDataf("limit must be between 1 and 100")
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
		return nil, kerrors.InvalidDataf("beforeTime must be later than afterTime")
	}

	return req, nil
}

func (h *TaskHandler) handleForceRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeForceRunRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	run, err := h.TaskService.ForceRun(ctx, req.TaskID, req.Timestamp)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusOK, newRunResponse(*run)); err != nil {
		EncodeError(ctx, err, w)
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
		return forceRunRequest{}, kerrors.InvalidDataf("you must provide a task ID")
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
		EncodeError(ctx, err, w)
		return
	}

	run, err := h.TaskService.FindRunByID(ctx, req.TaskID, req.RunID)
	if err != nil {
		EncodeError(ctx, err, w)
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
		return nil, kerrors.InvalidDataf("you must provide a task ID")
	}
	rid := params.ByName("rid")
	if rid == "" {
		return nil, kerrors.InvalidDataf("you must provide a run ID")
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
		return nil, kerrors.InvalidDataf("you must provide a run ID")
	}
	tid := params.ByName("id")
	if tid == "" {
		return nil, kerrors.InvalidDataf("you must provide a task ID")
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
		EncodeError(ctx, err, w)
		return
	}

	err = h.TaskService.CancelRun(ctx, req.TaskID, req.RunID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

func (h *TaskHandler) handleRetryRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeRetryRunRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	run, err := h.TaskService.RetryRun(ctx, req.TaskID, req.RunID)
	if err != nil {
		EncodeError(ctx, err, w)
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
		return nil, kerrors.InvalidDataf("you must provide a task ID")
	}
	rid := params.ByName("rid")
	if rid == "" {
		return nil, kerrors.InvalidDataf("you must provide a run ID")
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

// TaskService connects to Influx via HTTP using tokens to manage tasks.
type TaskService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// FindTaskByID returns a single task
func (t TaskService) FindTaskByID(ctx context.Context, id platform.ID) (*platform.Task, error) {
	u, err := newURL(t.Addr, taskIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		if err.Error() == backend.ErrTaskNotFound.Error() {
			// ErrTaskNotFound is expected as part of the FindTaskByID contract,
			// so return that actual error instead of a different error that looks like it.
			return nil, backend.ErrTaskNotFound
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
	u, err := newURL(t.Addr, tasksPath)
	if err != nil {
		return nil, 0, err
	}

	val := url.Values{}
	if filter.After != nil {
		val.Add("after", filter.After.String())
	}
	if filter.Organization != nil {
		val.Add("organization", filter.Organization.String())
	}
	if filter.User != nil {
		val.Add("user", filter.User.String())
	}
	if filter.Limit != 0 {
		val.Add("limit", strconv.Itoa(filter.Limit))
	}

	u.RawQuery = val.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)
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
func (t TaskService) CreateTask(ctx context.Context, tsk *platform.Task) error {
	u, err := newURL(t.Addr, tasksPath)
	if err != nil {
		return err
	}

	taskBytes, err := json.Marshal(tsk)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(taskBytes))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return err
	}

	var tr taskResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return err
	}
	*tsk = tr.Task

	return nil
}

// UpdateTask updates a single task with changeset.
func (t TaskService) UpdateTask(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
	u, err := newURL(t.Addr, taskIDPath(id))
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

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

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
	u, err := newURL(t.Addr, taskIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckErrorStatus(http.StatusNoContent, resp)
}

// FindLogs returns logs for a run.
func (t TaskService) FindLogs(ctx context.Context, filter platform.LogFilter) ([]*platform.Log, int, error) {
	if filter.Task == nil {
		return nil, 0, errors.New("task ID required")
	}

	var urlPath string
	if filter.Run == nil {
		urlPath = path.Join(taskIDPath(*filter.Task), "logs")
	} else {
		urlPath = path.Join(taskIDRunIDPath(*filter.Task, *filter.Run), "logs")
	}

	u, err := newURL(t.Addr, urlPath)
	if err != nil {
		return nil, 0, err
	}
	val := url.Values{}
	if filter.Org != nil {
		val.Set("orgID", filter.Org.String())
	}
	u.RawQuery = val.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var logs []*platform.Log
	if err := json.NewDecoder(resp.Body).Decode(&logs); err != nil {
		return nil, 0, err
	}

	return logs, len(logs), nil
}

// FindRuns returns a list of runs that match a filter and the total count of returned runs.
func (t TaskService) FindRuns(ctx context.Context, filter platform.RunFilter) ([]*platform.Run, int, error) {
	if filter.Task == nil {
		return nil, 0, errors.New("task ID required")
	}

	u, err := newURL(t.Addr, taskIDRunsPath(*filter.Task))
	if err != nil {
		return nil, 0, err
	}

	val := url.Values{}
	if filter.Org != nil {
		val.Set("orgID", filter.Org.String())
	}
	if filter.After != nil {
		val.Set("after", filter.After.String())
	}
	u.RawQuery = val.Encode()
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

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
	u, err := newURL(t.Addr, taskIDRunIDPath(taskID, runID))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		if err.Error() == backend.ErrRunNotFound.Error() {
			// ErrRunNotFound is expected as part of the FindRunByID contract,
			// so return that actual error instead of a different error that looks like it.
			return nil, backend.ErrRunNotFound
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
	p := path.Join(taskIDRunIDPath(taskID, runID), "retry")
	u, err := newURL(t.Addr, p)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}

	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		if err.Error() == backend.ErrRunNotFound.Error() {
			// ErrRunNotFound is expected as part of the RetryRun contract,
			// so return that actual error instead of a different error that looks like it.
			return nil, backend.ErrRunNotFound
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

func (t TaskService) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*platform.Run, error) {
	u, err := newURL(t.Addr, taskIDRunsPath(taskID))
	if err != nil {
		return nil, err
	}

	body := fmt.Sprintf(`{"scheduledFor": %q}`, time.Unix(scheduledFor, 0).UTC().Format(time.RFC3339))
	req, err := http.NewRequest("POST", u.String(), strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		if err.Error() == backend.ErrRunNotFound.Error() {
			// ErrRunNotFound is expected as part of the RetryRun contract,
			// so return that actual error instead of a different error that looks like it.
			return nil, backend.ErrRunNotFound
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

func (t TaskService) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	u, err := newURL(t.Addr, cancelPath(taskID, runID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}

	SetToken(t.Token, req)

	hc := newClient(u.Scheme, t.InsecureSkipVerify)

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

func (h *TaskHandler) populateOrg(ctx context.Context, t *platform.Task) error {
	if t.OrganizationID.Valid() && t.Organization != "" {
		return nil
	}

	if !t.OrganizationID.Valid() && t.Organization == "" {
		return errors.New("missing orgID and organization name")
	}

	if t.OrganizationID.Valid() {
		o, err := h.OrganizationService.FindOrganizationByID(ctx, t.OrganizationID)
		if err != nil {
			return err
		}
		t.Organization = o.Name
	} else {
		o, err := h.OrganizationService.FindOrganization(ctx, platform.OrganizationFilter{Name: &t.Organization})
		if err != nil {
			return err
		}
		t.OrganizationID = o.ID
	}
	return nil
}
