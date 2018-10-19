package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/influxdata/platform"
	pcontext "github.com/influxdata/platform/context"
	kerrors "github.com/influxdata/platform/kit/errors"
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
}

const (
	tasksPath              = "/api/v2/tasks"
	tasksIDPath            = "/api/v2/tasks/:tid"
	tasksIDLogsPath        = "/api/v2/tasks/:tid/logs"
	tasksIDMembersPath     = "/api/v2/tasks/:tid/members"
	tasksIDMembersIDPath   = "/api/v2/tasks/:tid/members/:userID"
	tasksIDOwnersPath      = "/api/v2/tasks/:tid/owners"
	tasksIDOwnersIDPath    = "/api/v2/tasks/:tid/owners/:userID"
	tasksIDRunsPath        = "/api/v2/tasks/:tid/runs"
	tasksIDRunsIDPath      = "/api/v2/tasks/:tid/runs/:rid"
	tasksIDRunsIDLogsPath  = "/api/v2/tasks/:tid/runs/:rid/logs"
	tasksIDRunsIDRetryPath = "/api/v2/tasks/:tid/runs/:rid/retry"
)

// NewTaskHandler returns a new instance of TaskHandler.
func NewTaskHandler(logger *zap.Logger) *TaskHandler {
	h := &TaskHandler{
		logger: logger,
		Router: httprouter.New(),
	}

	h.HandlerFunc("GET", tasksPath, h.handleGetTasks)
	h.HandlerFunc("POST", tasksPath, h.handlePostTask)

	h.HandlerFunc("GET", tasksIDPath, h.handleGetTask)
	h.HandlerFunc("PATCH", tasksIDPath, h.handleUpdateTask)
	h.HandlerFunc("DELETE", tasksIDPath, h.handleDeleteTask)

	h.HandlerFunc("GET", tasksIDLogsPath, h.handleGetLogs)
	h.HandlerFunc("GET", tasksIDRunsIDLogsPath, h.handleGetLogs)

	h.HandlerFunc("POST", tasksIDMembersPath, newPostMemberHandler(h.UserResourceMappingService, platform.TaskResourceType, platform.Member))
	h.HandlerFunc("GET", tasksIDMembersPath, newGetMembersHandler(h.UserResourceMappingService, platform.Member))
	h.HandlerFunc("DELETE", tasksIDMembersIDPath, newDeleteMemberHandler(h.UserResourceMappingService, platform.Member))

	h.HandlerFunc("POST", tasksIDOwnersPath, newPostMemberHandler(h.UserResourceMappingService, platform.TaskResourceType, platform.Owner))
	h.HandlerFunc("GET", tasksIDOwnersPath, newGetMembersHandler(h.UserResourceMappingService, platform.Owner))
	h.HandlerFunc("DELETE", tasksIDOwnersIDPath, newDeleteMemberHandler(h.UserResourceMappingService, platform.Owner))

	h.HandlerFunc("GET", tasksIDRunsPath, h.handleGetRuns)
	h.HandlerFunc("GET", tasksIDRunsIDPath, h.handleGetRun)
	h.HandlerFunc("POST", tasksIDRunsIDRetryPath, h.handleRetryRun)

	return h
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

	if err := encodeResponse(ctx, w, http.StatusOK, tasks); err != nil {
		EncodeError(ctx, err, w)
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

	return req, nil
}

func (h *TaskHandler) handlePostTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostTaskRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.TaskService.CreateTask(ctx, req.Task); err != nil {
		if e, ok := err.(AuthzError); ok {
			h.logger.Error("failed authentication", zap.Errors("error messages", []error{err, e.AuthzError()}))
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Task); err != nil {
		EncodeError(ctx, err, w)
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

	if err := encodeResponse(ctx, w, http.StatusOK, task); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getTaskRequest struct {
	TaskID platform.ID
}

func decodeGetTaskRequest(ctx context.Context, r *http.Request) (*getTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("tid")
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

	if err := encodeResponse(ctx, w, http.StatusOK, task); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type updateTaskRequest struct {
	Update platform.TaskUpdate
	TaskID platform.ID
}

func decodeUpdateTaskRequest(ctx context.Context, r *http.Request) (*updateTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("tid")
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

	w.WriteHeader(http.StatusAccepted)
}

type deleteTaskRequest struct {
	TaskID platform.ID
}

func decodeDeleteTaskRequest(ctx context.Context, r *http.Request) (*deleteTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("tid")
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

	tok, err := GetToken(r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	auth, err := h.AuthorizationService.FindAuthorizationByToken(ctx, tok)
	if err != nil {
		EncodeError(ctx, kerrors.Wrap(err, "invalid token", kerrors.InvalidData), w)
		return
	}
	ctx = pcontext.SetAuthorizer(ctx, auth)

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
		EncodeError(ctx, err, w)
		return
	}
}

type getLogsRequest struct {
	filter platform.LogFilter
}

func decodeGetLogsRequest(ctx context.Context, r *http.Request, orgs platform.OrganizationService) (*getLogsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("tid")
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
	}

	if runID := params.ByName("rid"); id != "" {
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

	tok, err := GetToken(r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	auth, err := h.AuthorizationService.FindAuthorizationByToken(ctx, tok)
	if err != nil {
		EncodeError(ctx, kerrors.Wrap(err, "invalid token", kerrors.InvalidData), w)
		return
	}
	ctx = pcontext.SetAuthorizer(ctx, auth)

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

	if err := encodeResponse(ctx, w, http.StatusOK, runs); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getRunsRequest struct {
	filter platform.RunFilter
}

func decodeGetRunsRequest(ctx context.Context, r *http.Request, orgs platform.OrganizationService) (*getRunsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("tid")
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

	if time := qp.Get("afterTime"); time != "" {
		// TODO (jm): verify valid RFC3339
		req.filter.AfterTime = time
	}

	if time := qp.Get("beforeTime"); time != "" {
		// TODO (jm): verify valid RFC3339
		req.filter.BeforeTime = time
	}

	return req, nil
}

func (h *TaskHandler) handleGetRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tok, err := GetToken(r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	auth, err := h.AuthorizationService.FindAuthorizationByToken(ctx, tok)
	if err != nil {
		EncodeError(ctx, kerrors.Wrap(err, "invalid token", kerrors.InvalidData), w)
		return
	}
	ctx = pcontext.SetAuthorizer(ctx, auth)

	req, err := decodeGetRunRequest(ctx, r, h.OrganizationService)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	run, err := h.TaskService.FindRunByID(ctx, req.OrgID, req.RunID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, run); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getRunRequest struct {
	OrgID platform.ID
	RunID platform.ID
}

func decodeGetRunRequest(ctx context.Context, r *http.Request, orgs platform.OrganizationService) (*getRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("rid")
	if id == "" {
		return nil, kerrors.InvalidDataf("you must provide a run ID")
	}

	qp := r.URL.Query()
	var orgID platform.ID
	if orgName := qp.Get("org"); orgName != "" {
		o, err := orgs.FindOrganization(ctx, platform.OrganizationFilter{Name: &orgName})
		if err != nil {
			return nil, err
		}

		orgID = o.ID
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &getRunRequest{
		RunID: i,
		OrgID: orgID,
	}, nil
}

func (h *TaskHandler) handleRetryRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeRetryRunRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	run, err := h.TaskService.RetryRun(ctx, req.RunID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, run); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type retryRunRequest struct {
	RunID platform.ID
}

func decodeRetryRunRequest(ctx context.Context, r *http.Request) (*retryRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("rid")
	if id == "" {
		return nil, kerrors.InvalidDataf("you must provide a run ID")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &retryRunRequest{
		RunID: i,
	}, nil
}
