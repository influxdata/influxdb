package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

const (
	runIDField        = "runID"
	scheduledForField = "scheduledFor"
	startedAtField    = "startedAt"
	finishedAtField   = "finishedAt"
	requestedAtField  = "requestedAt"
	statusField       = "status"
	logField          = "logs"

	taskIDTag = "taskID"

	// Fixed system bucket ID for task and run logs.
	taskSystemBucketID platform.ID = 10
)

// NewAnalyticalStorage creates a new analytical store with access to the necessary systems for storing data and to act as a middleware
func NewAnalyticalStorage(logger *zap.Logger, ts influxdb.TaskService, tcs TaskControlService, pw storage.PointsWriter, qs query.QueryService) *AnalyticalStorage {
	return &AnalyticalStorage{
		logger:             logger,
		TaskService:        ts,
		TaskControlService: tcs,
		pw:                 pw,
		qs:                 qs,
	}
}

type AnalyticalStorage struct {
	influxdb.TaskService
	TaskControlService

	pw     storage.PointsWriter
	qs     query.QueryService
	logger *zap.Logger
}

func (as *AnalyticalStorage) FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	run, err := as.TaskControlService.FinishRun(ctx, taskID, runID)
	if run != nil && run.ID.String() != "" {
		task, err := as.TaskService.FindTaskByID(ctx, run.TaskID)
		if err != nil {
			return run, err
		}

		tags := models.Tags{
			models.NewTag([]byte(taskIDTag), []byte(run.TaskID.String())),
			models.NewTag([]byte(statusField), []byte(run.Status)),
		}

		// log an error if we have incomplete data on finish
		if !run.ID.Valid() ||
			run.ScheduledFor == "" ||
			run.StartedAt == "" ||
			run.FinishedAt == "" ||
			run.Status == "" {
			as.logger.Error("Run missing critical fields", zap.String("run", fmt.Sprintf("%+v", run)), zap.String("runID", run.ID.String()))
		}

		fields := map[string]interface{}{}
		fields[statusField] = run.Status
		fields[runIDField] = run.ID.String()
		fields[startedAtField] = run.StartedAt
		fields[finishedAtField] = run.FinishedAt
		fields[scheduledForField] = run.ScheduledFor
		if run.RequestedAt != "" {
			fields[requestedAtField] = run.RequestedAt
		}

		startedAt, err := run.StartedAtTime()
		if err != nil {
			startedAt = time.Now()
		}

		logBytes, err := json.Marshal(run.Log)
		if err != nil {
			return run, err
		}
		fields[logField] = string(logBytes)

		point, err := models.NewPoint("runs", tags, fields, startedAt)
		if err != nil {
			return run, err
		}

		// use the tsdb explode points to convert to the new style.
		// We could split this on our own but its quite possible this could change.
		points, err := tsdb.ExplodePoints(task.OrganizationID, taskSystemBucketID, models.Points{point})
		if err != nil {
			return run, err
		}
		return run, as.pw.WritePoints(ctx, points)
	}
	return run, err
}

// FindLogs returns logs for a run.
// First attempt to use the TaskService, then append additional analytical's logs to the list
func (as *AnalyticalStorage) FindLogs(ctx context.Context, filter influxdb.LogFilter) ([]*influxdb.Log, int, error) {
	var logs []*influxdb.Log
	if filter.Run != nil {
		run, err := as.FindRunByID(ctx, filter.Task, *filter.Run)
		if err != nil {
			return nil, 0, err
		}
		for i := 0; i < len(run.Log); i++ {
			logs = append(logs, &run.Log[i])
		}
		return logs, len(logs), nil
	}

	// add historical logs to the transactional logs.
	runs, n, err := as.FindRuns(ctx, influxdb.RunFilter{Task: filter.Task})
	if err != nil {
		return nil, 0, err
	}

	for _, run := range runs {
		for i := 0; i < len(run.Log); i++ {
			logs = append(logs, &run.Log[i])
		}
	}

	return logs, n, err
}

// FindRuns returns a list of runs that match a filter and the total count of returned runs.
// First attempt to use the TaskService, then append additional analytical's runs to the list
func (as *AnalyticalStorage) FindRuns(ctx context.Context, filter influxdb.RunFilter) ([]*influxdb.Run, int, error) {
	if filter.Limit == 0 {
		filter.Limit = influxdb.TaskDefaultPageSize
	}

	if filter.Limit < 0 || filter.Limit > influxdb.TaskMaxPageSize {
		return nil, 0, influxdb.ErrOutOfBoundsLimit
	}

	runs, n, err := as.TaskService.FindRuns(ctx, filter)
	if err != nil {
		return runs, n, err
	}

	// if we reached the limit lets stop here
	if len(runs) >= filter.Limit {
		return runs, n, err
	}

	task, err := as.TaskService.FindTaskByID(ctx, filter.Task)
	if err != nil {
		return runs, n, err
	}

	filterPart := ""
	if filter.After != nil {
		filterPart = fmt.Sprintf(`|> filter(fn: (r) => r.runID > %q)`, filter.After.String())
	}

	// the data will be stored for 7 days in the system bucket so pulling 14d's is sufficient.
	runsScript := fmt.Sprintf(`from(bucketID: "000000000000000a")
	  |> range(start: -14d)
	  |> filter(fn: (r) => r._measurement == "runs" and r.taskID == %q)
	  %s
	  |> group(columns: ["taskID"])
	  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")

	  `, filter.Task.String(), filterPart)

	// At this point we are behind authorization
	// so we are faking a read only permission to the org's system bucket
	runSystemBucketID := taskSystemBucketID
	runAuth := &influxdb.Authorization{
		ID:    taskSystemBucketID,
		OrgID: task.OrganizationID,
		Permissions: []influxdb.Permission{
			influxdb.Permission{
				Action: influxdb.ReadAction,
				Resource: influxdb.Resource{
					Type:  influxdb.BucketsResourceType,
					OrgID: &task.OrganizationID,
					ID:    &runSystemBucketID,
				},
			},
		},
	}
	request := &query.Request{Authorization: runAuth, OrganizationID: task.OrganizationID, Compiler: lang.FluxCompiler{Query: runsScript}}

	ittr, err := as.qs.Query(ctx, request)
	if err != nil {
		return nil, 0, err
	}
	defer ittr.Release()

	re := &runReader{logger: as.logger.With(zap.String("component", "run-reader"), zap.String("taskID", filter.Task.String()))}
	for ittr.More() {
		err := ittr.Next().Tables().Do(re.readTable)
		if err != nil {
			return runs, n, err
		}
	}

	if err := ittr.Err(); err != nil {
		return nil, 0, fmt.Errorf("unexpected internal error while decoding run response: %v", err)
	}

	runs = append(runs, re.runs...)

	return runs, n, err
}

// FindRunByID returns a single run.
// First see if it is in the existing TaskService. If not pull it from analytical storage.
func (as *AnalyticalStorage) FindRunByID(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	// check the taskService to see if the run is on its list
	run, err := as.TaskService.FindRunByID(ctx, taskID, runID)
	if err != nil {
		if err, ok := err.(*influxdb.Error); !ok || err.Msg != "run not found" {
			return run, err
		}
	}
	if run != nil {
		return run, err
	}

	task, err := as.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return run, err
	}

	// the data will be stored for 7 days in the system bucket so pulling 14d's is sufficient.
	findRunScript := fmt.Sprintf(`from(bucketID: "000000000000000a")
	|> range(start: -14d)
	|> filter(fn: (r) => r._measurement == "runs" and r.taskID == %q)
	|> group(columns: ["taskID"])
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> filter(fn: (r) => r.runID == %q)
	  `, taskID.String(), runID.String())

	// At this point we are behind authorization
	// so we are faking a read only permission to the org's system bucket
	runSystemBucketID := taskSystemBucketID
	runAuth := &influxdb.Authorization{
		ID:    taskSystemBucketID,
		OrgID: task.OrganizationID,
		Permissions: []influxdb.Permission{
			influxdb.Permission{
				Action: influxdb.ReadAction,
				Resource: influxdb.Resource{
					Type:  influxdb.BucketsResourceType,
					OrgID: &task.OrganizationID,
					ID:    &runSystemBucketID,
				},
			},
		},
	}
	request := &query.Request{Authorization: runAuth, OrganizationID: task.OrganizationID, Compiler: lang.FluxCompiler{Query: findRunScript}}

	ittr, err := as.qs.Query(ctx, request)
	if err != nil {
		return nil, err
	}
	defer ittr.Release()

	re := &runReader{}
	for ittr.More() {
		err := ittr.Next().Tables().Do(re.readTable)
		if err != nil {
			return nil, err
		}
	}

	if err := ittr.Err(); err != nil {
		return nil, fmt.Errorf("unexpected internal error while decoding run response: %v", err)
	}

	if len(re.runs) == 0 {
		return nil, platform.ErrRunNotFound

	}

	if len(re.runs) != 1 {
		return nil, &influxdb.Error{
			Msg:  "found multiple runs with id " + runID.String(),
			Code: influxdb.EInternal,
		}
	}

	return re.runs[0], err
}

func (as *AnalyticalStorage) RetryRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	run, err := as.TaskService.RetryRun(ctx, taskID, runID)
	if err != nil {
		if err, ok := err.(*influxdb.Error); !ok || err.Msg != "run not found" {
			return run, err
		}
	}

	if run != nil {
		return run, err
	}

	// try finding the run (in our system or underlieing)
	run, err = as.FindRunByID(ctx, taskID, runID)
	if err != nil {
		return run, err
	}

	sf, err := run.ScheduledForTime()
	if err != nil {
		return run, err
	}

	return as.ForceRun(ctx, taskID, sf.Unix())
}

type runReader struct {
	runs   []*influxdb.Run
	logger *zap.Logger
}

func (re *runReader) readTable(tbl flux.Table) error {
	return tbl.Do(re.readRuns)
}

func (re *runReader) readRuns(cr flux.ColReader) error {
	for i := 0; i < cr.Len(); i++ {
		var r influxdb.Run
		for j, col := range cr.Cols() {
			switch col.Label {
			case "runID":
				if cr.Strings(j).ValueString(i) != "" {
					id, err := influxdb.IDFromString(cr.Strings(j).ValueString(i))
					if err != nil {
						re.logger.Info("failed to parse runID", zap.Error(err))
						continue
					}
					r.ID = *id
				}
			case "taskID":
				if cr.Strings(j).ValueString(i) != "" {
					id, err := influxdb.IDFromString(cr.Strings(j).ValueString(i))
					if err != nil {
						re.logger.Info("failed to parse taskID", zap.Error(err))
						continue
					}
					r.TaskID = *id
				}
			case startedAtField:
				r.StartedAt = cr.Strings(j).ValueString(i)
			case requestedAtField:
				r.RequestedAt = cr.Strings(j).ValueString(i)
			case scheduledForField:
				r.ScheduledFor = cr.Strings(j).ValueString(i)
			case statusField:
				r.Status = cr.Strings(j).ValueString(i)
			case finishedAtField:
				r.FinishedAt = cr.Strings(j).ValueString(i)
			case logField:
				logBytes := bytes.TrimSpace(cr.Strings(j).Value(i))
				if len(logBytes) != 0 {
					err := json.Unmarshal(logBytes, &r.Log)
					if err != nil {
						re.logger.Info("failed to parse log data", zap.Error(err), zap.ByteString("log_bytes", logBytes))
					}
				}
			}
		}

		// if we dont have a full enough data set we fail here.
		if r.ID.Valid() {
			re.runs = append(re.runs, &r)
		}

	}

	return nil
}
