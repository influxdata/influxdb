package backend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/flux/values"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/semantic"
	platform "github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/query"
)

type QueryLogReader struct {
	queryService query.QueryService
}

var _ LogReader = (*QueryLogReader)(nil)

func NewQueryLogReader(qs query.QueryService) *QueryLogReader {
	return &QueryLogReader{
		queryService: qs,
	}
}

func (qlr *QueryLogReader) ListLogs(ctx context.Context, orgID platform.ID, logFilter platform.LogFilter) ([]platform.Log, error) {
	if !logFilter.Task.Valid() {
		return nil, errors.New("task ID required to list logs")
	}

	filterPart := ""
	if logFilter.Run != nil {
		filterPart = fmt.Sprintf(`|> filter(fn: (r) => r.runID == %q)`, logFilter.Run.String())
	}

	// TODO(lh): Change the range to something more reasonable. Not sure what that range will be.
	listScript := fmt.Sprintf(`from(bucketID: "000000000000000a")
  |> range(start: -100h)
  |> filter(fn: (r) => r._measurement == "logs" and r.taskID == %q)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  %s
  |> group(columns: ["taskID", "runID", "_measurement"])
  `, logFilter.Task.String(), filterPart)

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}
	if auth.Kind() != "authorization" {
		return nil, platform.ErrAuthorizerNotSupported
	}
	request := &query.Request{Authorization: auth.(*platform.Authorization), OrganizationID: orgID, Compiler: lang.FluxCompiler{Query: listScript}}

	ittr, err := qlr.queryService.Query(ctx, request)
	if err != nil {
		return nil, err
	}
	defer ittr.Release()

	re := newRunExtractor()
	for ittr.More() {
		if err := ittr.Next().Tables().Do(re.Extract); err != nil {
			return nil, err
		}
	}

	if err := ittr.Err(); err != nil {
		return nil, err
	}

	runs := re.Runs()

	var logs []platform.Log
	for _, r := range runs {
		logs = append(logs, r.Log...)
	}
	return logs, nil
}

func (qlr *QueryLogReader) ListRuns(ctx context.Context, orgID platform.ID, runFilter platform.RunFilter) ([]*platform.Run, error) {
	if !runFilter.Task.Valid() {
		return nil, errors.New("task required")
	}

	limit := "|> limit(n: 100)\n"
	if runFilter.Limit > 0 {
		limit = fmt.Sprintf("|> limit(n: %d)\n", runFilter.Limit)
	}

	afterID := ""
	if runFilter.After != nil {
		afterID = runFilter.After.String()
	}
	scheduledAfter := runFilter.AfterTime // Fine if this is empty string.
	scheduledBefore := "Z"                // Arbitrary string that occurs after numbers, so it won't reject anything.
	if runFilter.BeforeTime != "" {
		scheduledBefore = runFilter.BeforeTime
	}

	// Because flux doesnt support piviting on a rowkey that might not exist we need first check if we can pivot with "requestedAt"
	// and if that fails we can fall back to pivot without "requestedAt"
	// TODO(lh): After we transition to a separation of transactional and analytical stores this can be simplified.
	pivotWithRequestedAt := `|> pivot(rowKey:["runID", "scheduledFor", "requestedAt"], columnKey: ["status"], valueColumn: "_time")`
	pivotWithOutRequestedAt := `|> pivot(rowKey:["runID", "scheduledFor"], columnKey: ["status"], valueColumn: "_time")`

	listFmtString := `
import "influxdata/influxdb/v1"

from(bucketID: "000000000000000a")
  |> range(start: -24h)
	|> filter(fn: (r) => r._measurement == "records" and r.taskID == %q)
	|> drop(columns: ["_start", "_stop"])
	|> group(columns: ["_measurement", "taskID", "scheduledFor", "status", "runID"])
	|> v1.fieldsAsCols()
	|> filter(fn: (r) => r.scheduledFor < %q and r.scheduledFor > %q and r.runID > %q)
	%s
	%s
	`
	listScript := fmt.Sprintf(listFmtString, runFilter.Task.String(), scheduledBefore, scheduledAfter, afterID, pivotWithRequestedAt, limit)

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}
	if auth.Kind() != "authorization" {
		return nil, platform.ErrAuthorizerNotSupported
	}
	request := &query.Request{Authorization: auth.(*platform.Authorization), OrganizationID: orgID, Compiler: lang.FluxCompiler{Query: listScript}}

	ittr, err := qlr.queryService.Query(ctx, request)
	if err != nil {
		return nil, err
	}

	runs, err := queryIttrToRuns(ittr)
	if err != nil {
		// try re running the script without the requested at
		listScript := fmt.Sprintf(listFmtString, runFilter.Task.String(), scheduledBefore, scheduledAfter, afterID, pivotWithOutRequestedAt, limit)
		request := &query.Request{Authorization: auth.(*platform.Authorization), OrganizationID: orgID, Compiler: lang.FluxCompiler{Query: listScript}}

		ittr, err := qlr.queryService.Query(ctx, request)
		if err != nil {
			return nil, err
		}
		runs, err = queryIttrToRuns(ittr)
		if err != nil {
			return nil, err
		}
	}

	return runs, nil
}

func (qlr *QueryLogReader) FindRunByID(ctx context.Context, orgID, runID platform.ID) (*platform.Run, error) {
	pivotWithRequestedAt := `|> pivot(rowKey:["runID", "scheduledFor", "requestedAt"], columnKey: ["status"], valueColumn: "_time")`
	pivotWithOutRequestedAt := `|> pivot(rowKey:["runID", "scheduledFor"], columnKey: ["status"], valueColumn: "_time")`

	showFmtScript := `
import "influxdata/influxdb/v1"

logs = from(bucketID: "000000000000000a")
	|> range(start: -24h)
	|> filter(fn: (r) => r._measurement == "logs")
	|> drop(columns: ["_start", "_stop"])
	|> v1.fieldsAsCols()
	|> filter(fn: (r) => r.runID == %q)
	|> yield(name: "logs")

from(bucketID: "000000000000000a")
  |> range(start: -24h)
	|> filter(fn: (r) => r._measurement == "records")
	|> drop(columns: ["_start", "_stop"])
	|> group(columns: ["_measurement", "taskID", "scheduledFor", "status", "runID"])
	|> v1.fieldsAsCols()
	|> filter(fn: (r) => r.runID == %q)
	%s
	|> yield(name: "result")
  `
	showScript := fmt.Sprintf(showFmtScript, runID.String(), runID.String(), pivotWithRequestedAt)

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}
	if auth.Kind() != "authorization" {
		return nil, platform.ErrAuthorizerNotSupported
	}
	request := &query.Request{Authorization: auth.(*platform.Authorization), OrganizationID: orgID, Compiler: lang.FluxCompiler{Query: showScript}}

	ittr, err := qlr.queryService.Query(ctx, request)
	if err != nil {
		return nil, err
	}
	runs, err := queryIttrToRuns(ittr)
	if err != nil {
		showScript := fmt.Sprintf(showFmtScript, runID.String(), runID.String(), pivotWithOutRequestedAt)
		request := &query.Request{Authorization: auth.(*platform.Authorization), OrganizationID: orgID, Compiler: lang.FluxCompiler{Query: showScript}}

		ittr, err := qlr.queryService.Query(ctx, request)
		if err != nil {
			return nil, err

		}
		runs, err = queryIttrToRuns(ittr)
		if err != nil {
			return nil, err
		}
	}
	if len(runs) == 0 {
		return nil, ErrRunNotFound
	}
	if len(runs) > 1 {
		return nil, fmt.Errorf("expected one run, got %d", len(runs))
	}

	return runs[0], nil
}

func queryIttrToRuns(results flux.ResultIterator) ([]*platform.Run, error) {
	defer results.Release()

	re := newRunExtractor()

	for results.More() {
		if err := results.Next().Tables().Do(re.Extract); err != nil {
			return nil, err
		}
	}

	if err := results.Err(); err != nil {
		return nil, err
	}

	return re.Runs(), nil
}

// runExtractor is used to decode query results to runs.
type runExtractor struct {
	runs map[platform.ID]platform.Run
}

func newRunExtractor() *runExtractor {
	return &runExtractor{runs: make(map[platform.ID]platform.Run)}
}

// Runs returns the runExtractor's stored runs as a slice.
func (re *runExtractor) Runs() []*platform.Run {
	runs := make([]*platform.Run, 0, len(re.runs))
	for _, r := range re.runs {
		r := r
		runs = append(runs, &r)
	}

	sort.Slice(runs, func(i, j int) bool { return runs[i].ID < runs[j].ID })
	return runs
}

// Extract extracts the run information from the given table.
func (re *runExtractor) Extract(tbl flux.Table) error {
	key := tbl.Key()
	if !key.HasCol("_measurement") {
		return fmt.Errorf("table key missing _measurement: %s", key.String())
	}
	mv := key.LabelValue("_measurement")
	if n := mv.Type().Nature(); n != semantic.String {
		return fmt.Errorf("table key has invalid _measurement type: %s, type = %s", key.String(), n)
	}

	switch mv.Str() {
	case "records":
		return tbl.Do(re.extractRecord)
	case "logs":
		return tbl.Do(re.extractLog)
	default:
		return fmt.Errorf("unknown measurement: %q", mv.Str())
	}
}

func (re *runExtractor) extractRecord(cr flux.ColReader) error {
	for i := 0; i < cr.Len(); i++ {
		var r platform.Run
		for j, col := range cr.Cols() {
			switch col.Label {
			case requestedAtField:
				r.RequestedAt = cr.Strings(j).ValueString(i)
			case scheduledForField:
				r.ScheduledFor = cr.Strings(j).ValueString(i)
			case "runID":
				id, err := platform.IDFromString(cr.Strings(j).ValueString(i))
				if err != nil {
					return err
				}
				r.ID = *id
			case "taskID":
				id, err := platform.IDFromString(cr.Strings(j).ValueString(i))
				if err != nil {
					return err
				}
				r.TaskID = *id
			case RunStarted.String():
				r.StartedAt = values.Time(cr.Times(j).Value(i)).Time().Format(time.RFC3339Nano)
				if r.Status == "" {
					// Only set status if it wasn't already set.
					r.Status = col.Label
				}
			case RunSuccess.String(), RunFail.String(), RunCanceled.String():
				if cr.Times(j).Value(i) != 0 {
					r.FinishedAt = values.Time(cr.Times(j).Value(i)).Time().Format(time.RFC3339Nano)
					// Finished can be set unconditionally;
					// it's fine to overwrite if the status was already set to started.
					r.Status = col.Label
				}
			}
		}

		if !r.ID.Valid() {
			return errors.New("extractRecord: did not find valid run ID in table")
		}

		if ex, ok := re.runs[r.ID]; ok {
			r.Log = ex.Log
		}

		re.runs[r.ID] = r
	}

	return nil
}

func (re *runExtractor) extractLog(cr flux.ColReader) error {
	entries := make(map[platform.ID][]platform.Log)
	for i := 0; i < cr.Len(); i++ {
		var runID platform.ID
		var when, line string
		for j, col := range cr.Cols() {
			switch col.Label {
			case "runID":
				id, err := platform.IDFromString(cr.Strings(j).ValueString(i))
				if err != nil {
					return err
				}
				runID = *id
			case "_time":
				when = values.Time(cr.Times(j).Value(i)).Time().Format(time.RFC3339Nano)
			case "line":
				line = cr.Strings(j).ValueString(i)
			}
		}

		if !runID.Valid() {
			return errors.New("extractLog: did not find valid run ID in table")
		}

		entries[runID] = append(entries[runID], platform.Log{Time: when, Message: line})
	}

	for id, logs := range entries {
		run := re.runs[id]
		run.Log = append(run.Log, logs...)
		re.runs[id] = run
	}

	return nil
}
