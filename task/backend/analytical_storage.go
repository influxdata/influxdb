package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	"go.uber.org/zap"
)

const (
	runIDField        = "runID"
	scheduledForField = "scheduledFor"
	startedAtField    = "startedAt"
	finishedAtField   = "finishedAt"
	requestedAtField  = "requestedAt"
	logField          = "logs"

	taskIDTag = "taskID"
	statusTag = "status"
)

// RunRecorder is a type which records runs into an influxdb
// backed storage mechanism
type RunRecorder interface {
	Record(ctx context.Context, orgID influxdb.ID, org string, bucketID influxdb.ID, bucket string, run *influxdb.Run) error
}

// NewAnalyticalRunStorage creates a new analytical store with access to the necessary systems for storing data and to act as a middleware
func NewAnalyticalRunStorage(log *zap.Logger, ts influxdb.TaskService, bs influxdb.BucketService, tcs TaskControlService, rr RunRecorder, qs query.QueryService) *AnalyticalStorage {
	return &AnalyticalStorage{
		log:                log,
		TaskService:        ts,
		BucketService:      bs,
		TaskControlService: tcs,
		rr:                 rr,
		qs:                 qs,
	}
}

// NewAnalyticalStorage creates a new analytical store with access to the necessary systems for storing data and to act as a middleware (deprecated)
func NewAnalyticalStorage(log *zap.Logger, ts influxdb.TaskService, bs influxdb.BucketService, tcs TaskControlService, pw storage.PointsWriter, qs query.QueryService) *AnalyticalStorage {
	return &AnalyticalStorage{
		log:                log,
		TaskService:        ts,
		BucketService:      bs,
		TaskControlService: tcs,
		rr:                 NewStoragePointsWriterRecorder(log, pw),
		qs:                 qs,
	}
}

type AnalyticalStorage struct {
	influxdb.TaskService
	influxdb.BucketService
	TaskControlService

	rr  RunRecorder
	qs  query.QueryService
	log *zap.Logger
}

func (as *AnalyticalStorage) FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	run, err := as.TaskControlService.FinishRun(ctx, taskID, runID)
	if run != nil && run.ID.String() != "" {
		task, err := as.TaskService.FindTaskByID(influxdb.FindTaskWithoutAuth(ctx), run.TaskID)
		if err != nil {
			return run, err
		}

		sb, err := as.BucketService.FindBucketByName(ctx, task.OrganizationID, influxdb.TasksSystemBucketName)
		if err != nil {
			return run, err
		}

		return run, as.rr.Record(ctx, task.OrganizationID, task.Organization, sb.ID, influxdb.TasksSystemBucketName, run)
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

	sb, err := as.BucketService.FindBucketByName(ctx, task.OrganizationID, influxdb.TasksSystemBucketName)
	if err != nil {
		return runs, n, err
	}

	filterPart := ""
	if filter.After != nil {
		filterPart = fmt.Sprintf(`|> filter(fn: (r) => r.runID > %q)`, filter.After.String())
	}

	// the data will be stored for 7 days in the system bucket so pulling 14d's is sufficient.
	runsScript := fmt.Sprintf(`from(bucketID: %q)
	  |> range(start: -14d)
	  |> filter(fn: (r) => r._field != "status")
	  |> filter(fn: (r) => r._measurement == "runs" and r.taskID == %q)
	  %s
	  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	  |> group(columns: ["taskID"])
	  |> sort(columns:["scheduledFor"], desc: true)
	  |> limit(n:%d)

	  `, sb.ID.String(), filter.Task.String(), filterPart, filter.Limit-len(runs))

	// At this point we are behind authorization
	// so we are faking a read only permission to the org's system bucket
	runSystemBucketID := sb.ID
	runAuth := &influxdb.Authorization{
		Status: influxdb.Active,
		ID:     sb.ID,
		OrgID:  task.OrganizationID,
		Permissions: []influxdb.Permission{
			{
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

	re := &runReader{log: as.log.With(zap.String("component", "run-reader"), zap.String("taskID", filter.Task.String()))}
	for ittr.More() {
		err := ittr.Next().Tables().Do(re.readTable)
		if err != nil {
			return runs, n, err
		}
	}

	if err := ittr.Err(); err != nil {
		return nil, 0, fmt.Errorf("unexpected internal error while decoding run response: %v", err)
	}
	runs = as.combineRuns(runs, re.runs)

	return runs, len(runs), err
}

// remove any kv runs that exist in the list of completed runs
func (as *AnalyticalStorage) combineRuns(currentRuns, completeRuns []*influxdb.Run) []*influxdb.Run {
	crMap := map[influxdb.ID]int{}

	// track the current runs
	for i, cr := range currentRuns {
		crMap[cr.ID] = i
	}

	// if we find a complete run that matches a current run the current run is out dated and
	// should be removed.
	for _, completeRun := range completeRuns {
		if i, ok := crMap[completeRun.ID]; ok {
			currentRuns = append(currentRuns[:i], currentRuns[i+1:]...)
		}
	}

	return append(currentRuns, completeRuns...)
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

	sb, err := as.BucketService.FindBucketByName(ctx, task.OrganizationID, influxdb.TasksSystemBucketName)
	if err != nil {
		return run, err
	}

	// the data will be stored for 7 days in the system bucket so pulling 14d's is sufficient.
	findRunScript := fmt.Sprintf(`from(bucketID: %q)
	|> range(start: -14d)
	|> filter(fn: (r) => r._field != "status")
	|> filter(fn: (r) => r._measurement == "runs" and r.taskID == %q)
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> group(columns: ["taskID"])
	|> filter(fn: (r) => r.runID == %q)
	  `, sb.ID.String(), taskID.String(), runID.String())

	// At this point we are behind authorization
	// so we are faking a read only permission to the org's system bucket
	runSystemBucketID := sb.ID
	runAuth := &influxdb.Authorization{
		ID:     sb.ID,
		Status: influxdb.Active,
		OrgID:  task.OrganizationID,
		Permissions: []influxdb.Permission{
			{
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
		return nil, influxdb.ErrRunNotFound

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

	// try finding the run (in our system or underlying)
	run, err = as.FindRunByID(ctx, taskID, runID)
	if err != nil {
		return run, err
	}

	sf := run.ScheduledFor

	return as.ForceRun(ctx, taskID, sf.Unix())
}

type runReader struct {
	runs []*influxdb.Run
	log  *zap.Logger
}

func (re *runReader) readTable(tbl flux.Table) error {
	return tbl.Do(re.readRuns)
}

func (re *runReader) readRuns(cr flux.ColReader) error {
	for i := 0; i < cr.Len(); i++ {
		var r influxdb.Run
		for j, col := range cr.Cols() {
			switch col.Label {
			case runIDField:
				if cr.Strings(j).ValueString(i) != "" {
					id, err := influxdb.IDFromString(cr.Strings(j).ValueString(i))
					if err != nil {
						re.log.Info("Failed to parse runID", zap.Error(err))
						continue
					}
					r.ID = *id
				}
			case taskIDTag:
				if cr.Strings(j).ValueString(i) != "" {
					id, err := influxdb.IDFromString(cr.Strings(j).ValueString(i))
					if err != nil {
						re.log.Info("Failed to parse taskID", zap.Error(err))
						continue
					}
					r.TaskID = *id
				}
			case startedAtField:
				started, err := time.Parse(time.RFC3339Nano, cr.Strings(j).ValueString(i))
				if err != nil {
					re.log.Info("Failed to parse startedAt time", zap.Error(err))
					continue
				}
				r.StartedAt = started.UTC()
			case requestedAtField:
				requested, err := time.Parse(time.RFC3339Nano, cr.Strings(j).ValueString(i))
				if err != nil {
					re.log.Info("Failed to parse requestedAt time", zap.Error(err))
					continue
				}
				r.RequestedAt = requested.UTC()
			case scheduledForField:
				scheduled, err := time.Parse(time.RFC3339, cr.Strings(j).ValueString(i))
				if err != nil {
					re.log.Info("Failed to parse scheduledFor time", zap.Error(err))
					continue
				}
				r.ScheduledFor = scheduled.UTC()
			case statusTag:
				r.Status = cr.Strings(j).ValueString(i)
			case finishedAtField:
				finished, err := time.Parse(time.RFC3339Nano, cr.Strings(j).ValueString(i))
				if err != nil {
					re.log.Info("Failed to parse finishedAt time", zap.Error(err))
					continue
				}
				r.FinishedAt = finished.UTC()
			case logField:
				logBytes := bytes.TrimSpace(cr.Strings(j).Value(i))
				if len(logBytes) != 0 {
					err := json.Unmarshal(logBytes, &r.Log)
					if err != nil {
						re.log.Info("Failed to parse log data", zap.Error(err), zap.ByteString("log_bytes", logBytes))
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
