package http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// TaskHandler represents an HTTP API handler for tasks.
type TaskHandler struct {
	*httprouter.Router
	TaskService platform.TaskService
}

// NewTaskHandler returns a new instance of TaskHandler.
func NewTaskHandler() *TaskHandler {
	h := &TaskHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("GET", "/v1/tasks", h.handleGetTasks)
	h.HandlerFunc("POST", "/v1/tasks", h.handlePostTask)
	h.HandlerFunc("GET", "/v1/tasks/:id", h.handleGetTask)
	h.HandlerFunc("PATCH", "/v1/tasks/:id", h.handleUpdateTask)
	h.HandlerFunc("DELETE", "/v1/tasks/:id", h.handleDeleteTask)
	return h
}

func (h *TaskHandler) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetTasksRequest(ctx, r)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	tasks, _, err := h.TaskService.FindTasks(ctx, req.filter)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, tasks); err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}
}

type getTasksRequest struct {
	filter platform.TaskFilter
}

func decodeGetTasksRequest(ctx context.Context, r *http.Request) (*getTasksRequest, error) {
	qp := r.URL.Query()
	req := &getTasksRequest{}

	if id := qp.Get("after"); id != "" {
		req.filter.After = &platform.ID{}
		if err := req.filter.After.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	if id := qp.Get("organization"); id != "" {
		req.filter.Organization = &platform.ID{}
		if err := req.filter.Organization.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	if id := qp.Get("user"); id != "" {
		req.filter.User = &platform.ID{}
		if err := req.filter.User.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	return req, nil
}

func (h *TaskHandler) handlePostTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostTaskRequest(ctx, r)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := h.TaskService.CreateTask(ctx, req.Task); err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Task); err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
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
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	task, err := h.TaskService.FindTaskByID(ctx, req.TaskID)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, task); err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
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
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	task, err := h.TaskService.UpdateTask(ctx, req.TaskID, req.Update)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, task); err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
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
		return nil, kerrors.InvalidDataf("url missing id")
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
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := h.TaskService.DeleteTask(ctx, req.TaskID); err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type deleteTaskRequest struct {
	TaskID platform.ID
}

func decodeDeleteTaskRequest(ctx context.Context, r *http.Request) (*deleteTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteTaskRequest{
		TaskID: i,
	}, nil
}
