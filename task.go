package influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/edit"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb/task/options"
)

const (
	TaskDefaultPageSize = 100
	TaskMaxPageSize     = 500

	TaskStatusActive   = "active"
	TaskStatusInactive = "inactive"
)

var (
	// TaskSystemType is the type set in tasks' for all crud requests
	TaskSystemType = "system"
)

// Task is a task. 🎊
type Task struct {
	ID              ID                     `json:"id"`
	Type            string                 `json:"type,omitempty"`
	OrganizationID  ID                     `json:"orgID"`
	Organization    string                 `json:"org"`
	AuthorizationID ID                     `json:"-"`
	Authorization   *Authorization         `json:"-"`
	OwnerID         ID                     `json:"ownerID"`
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

// EffectiveCron returns the effective cron string of the options.
// If the cron option was specified, it is returned.
// If the every option was specified, it is converted into a cron string using "@every".
// Otherwise, the empty string is returned.
// The value of the offset option is not considered.
func (t *Task) EffectiveCron() string {
	if t.Cron != "" {
		return t.Cron
	}
	if t.Every != "" {
		return "@every " + t.Every
	}
	return ""
}

// TaskEffectiveCron returns the effective cron string of the options
// If the cron option was specified, it is returned.
// If the every option was specified, it is converted into a cron string using by taking the largest duration and setting it to run at that interval
// Otherwise, the empty string is returned.
// The value of the offset option is not considered.
// TODO(docmerlin): when we switch completely to the new scheduler, rename this as EffectiveCron and delete the current
//  EffectiveCron method
func (t *Task) TaskEffectiveCron() (string, error) {
	if t.Cron != "" {
		return t.Cron, nil
	}

	if t.Every != "" {
		dur := &options.Duration{}
		err := dur.Parse(t.Every)
		if err != nil {
			return "", err
		}
		ts := struct {
			hours   int64
			minutes int64
			seconds int64
		}{1, 1, 1}

		for _, x := range dur.Node.Values {
			if x.Magnitude < 0 {
				return "", errors.New("we do not support negative durations")
			}
			switch x.Unit {
			case "h":
				if x.Magnitude > 24 {
					return "", errors.New("we do not support everys with hours units over 24 ")
				}
				ts.hours = x.Magnitude
				return fmt.Sprintf("0 0 */%d * * * *", ts.hours), nil
			case "m":
				if x.Magnitude > 60 {
					return "", errors.New("we do not support everys with seconds units over 60 ")
				}
				ts.minutes = x.Magnitude
				return fmt.Sprintf("0 */%d */%d * * * *", ts.minutes, ts.hours), nil

			case "s":
				if x.Magnitude > 60 {
					return "", errors.New("we do not support everys with seconds units over 60 ")
				}
				ts.seconds = x.Magnitude
				return fmt.Sprintf("*/%d */%d */%d * * * *", ts.seconds, ts.minutes, ts.hours), nil
			}
		}
		return fmt.Sprintf("*/%d */%d */%d * * * *", ts.seconds, ts.minutes, ts.hours), nil
	}
	return "", errors.New("either every or cron must be set")
}

// LatestCompletedTime gives the time.Time that the task was last queued to be run in RFC3339 format.
func (t *Task) LatestCompletedTime() (time.Time, error) {
	tm := t.LatestCompleted
	if tm == "" {
		tm = t.CreatedAt
	}
	return time.Parse(time.RFC3339, tm)
}

// OffsetDuration gives the time.Duration of the Task's Offset property, which represents a delay before execution
func (t *Task) OffsetDuration() (time.Duration, error) {
	if t.Offset == "" {
		return time.Duration(0), nil
	}
	return time.ParseDuration(t.Offset)
}

// Run is a record createId when a run of a task is scheduled.
type Run struct {
	ID           ID        `json:"id,omitempty"`
	TaskID       ID        `json:"taskID"`
	Status       string    `json:"status"`
	ScheduledFor time.Time `json:"scheduledFor"`          // ScheduledFor is the time the task is scheduled to run at
	StartedAt    time.Time `json:"startedAt,omitempty"`   // StartedAt is the time the executor begins running the task
	FinishedAt   time.Time `json:"finishedAt,omitempty"`  // FinishedAt is the time the executor finishes running the task
	RequestedAt  time.Time `json:"requestedAt,omitempty"` // RequestedAt is the time the coordinator told the scheduler to schedule the task
	Log          []Log     `json:"log,omitempty"`
}

// Log represents a link to a log resource
type Log struct {
	RunID   ID     `json:"runID,omitempty"`
	Time    string `json:"time"`
	Message string `json:"message"`
}

func (l Log) String() string {
	return l.Time + ": " + l.Message
}

// TaskService represents a service for managing one-off and recurring tasks.
type TaskService interface {
	// FindTaskByID returns a single task
	FindTaskByID(ctx context.Context, id ID) (*Task, error)

	// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
	// of matching tasks.
	FindTasks(ctx context.Context, filter TaskFilter) ([]*Task, int, error)

	// CreateTask creates a new task.
	// The owner of the task is inferred from the authorizer associated with ctx.
	CreateTask(ctx context.Context, t TaskCreate) (*Task, error)

	// UpdateTask updates a single task with changeset.
	UpdateTask(ctx context.Context, id ID, upd TaskUpdate) (*Task, error)

	// DeleteTask removes a task by ID and purges all associated data and scheduled runs.
	DeleteTask(ctx context.Context, id ID) error

	// FindLogs returns logs for a run.
	FindLogs(ctx context.Context, filter LogFilter) ([]*Log, int, error)

	// FindRuns returns a list of runs that match a filter and the total count of returned runs.
	FindRuns(ctx context.Context, filter RunFilter) ([]*Run, int, error)

	// FindRunByID returns a single run.
	FindRunByID(ctx context.Context, taskID, runID ID) (*Run, error)

	// CancelRun cancels a currently running run.
	CancelRun(ctx context.Context, taskID, runID ID) error

	// RetryRun creates and returns a new run (which is a retry of another run).
	RetryRun(ctx context.Context, taskID, runID ID) (*Run, error)

	// ForceRun forces a run to occur with unix timestamp scheduledFor, to be executed as soon as possible.
	// The value of scheduledFor may or may not align with the task's schedule.
	ForceRun(ctx context.Context, taskID ID, scheduledFor int64) (*Run, error)
}

// TaskCreate is the set of values to create a task.
type TaskCreate struct {
	Type           string                 `json:"type,omitempty"`
	Flux           string                 `json:"flux"`
	Description    string                 `json:"description,omitempty"`
	Status         string                 `json:"status,omitempty"`
	OrganizationID ID                     `json:"orgID,omitempty"`
	Organization   string                 `json:"org,omitempty"`
	OwnerID        ID                     `json:"-"`
	Metadata       map[string]interface{} `json:"-"` // not to be set through a web request but rather used by a http service using tasks backend.
}

func (t TaskCreate) Validate() error {
	switch {
	case t.Flux == "":
		return errors.New("missing flux")
	case !t.OrganizationID.Valid() && t.Organization == "":
		return errors.New("missing orgID and org")
	case t.Status != "" && t.Status != TaskStatusActive && t.Status != TaskStatusInactive:
		return fmt.Errorf("invalid task status: %q", t.Status)
	}
	return nil
}

// TaskUpdate represents updates to a task. Options updates override any options set in the Flux field.
type TaskUpdate struct {
	Flux        *string `json:"flux,omitempty"`
	Status      *string `json:"status,omitempty"`
	Description *string `json:"description,omitempty"`

	// LatestCompleted us to set latest completed on startup to skip task catchup
	LatestCompleted *string                `json:"-"`
	LastRunStatus   *string                `json:"-"`
	LastRunError    *string                `json:"-"`
	Metadata        map[string]interface{} `json:"-"` // not to be set through a web request but rather used by a http service using tasks backend.

	// Options gets unmarshalled from json as if it was flat, with the same level as Flux and Status.
	Options options.Options // when we unmarshal this gets unmarshalled from flat key-values

}

func (t *TaskUpdate) UnmarshalJSON(data []byte) error {
	// this is a type so we can marshal string into durations nicely
	jo := struct {
		Flux        *string `json:"flux,omitempty"`
		Status      *string `json:"status,omitempty"`
		Name        string  `json:"name,omitempty"`
		Description *string `json:"description,omitempty"`

		// Cron is a cron style time schedule that can be used in place of Every.
		Cron string `json:"cron,omitempty"`

		// Every represents a fixed period to repeat execution.
		// It gets marshalled from a string duration, i.e.: "10s" is 10 seconds
		Every options.Duration `json:"every,omitempty"`

		// Offset represents a delay before execution.
		// It gets marshalled from a string duration, i.e.: "10s" is 10 seconds
		Offset *options.Duration `json:"offset,omitempty"`

		Concurrency *int64 `json:"concurrency,omitempty"`

		Retry *int64 `json:"retry,omitempty"`
	}{}

	if err := json.Unmarshal(data, &jo); err != nil {
		return err
	}
	t.Options.Name = jo.Name
	t.Description = jo.Description
	t.Options.Cron = jo.Cron
	t.Options.Every = jo.Every
	if jo.Offset != nil {
		offset := *jo.Offset
		t.Options.Offset = &offset
	}
	t.Options.Concurrency = jo.Concurrency
	t.Options.Retry = jo.Retry
	t.Flux = jo.Flux
	t.Status = jo.Status
	return nil
}

func (t TaskUpdate) MarshalJSON() ([]byte, error) {
	jo := struct {
		Flux        *string `json:"flux,omitempty"`
		Status      *string `json:"status,omitempty"`
		Name        string  `json:"name,omitempty"`
		Description *string `json:"description,omitempty"`

		// Cron is a cron style time schedule that can be used in place of Every.
		Cron string `json:"cron,omitempty"`

		// Every represents a fixed period to repeat execution.
		Every options.Duration `json:"every,omitempty"`

		// Offset represents a delay before execution.
		Offset *options.Duration `json:"offset,omitempty"`

		Concurrency *int64 `json:"concurrency,omitempty"`

		Retry *int64 `json:"retry,omitempty"`
	}{}
	jo.Name = t.Options.Name
	jo.Cron = t.Options.Cron
	jo.Every = t.Options.Every
	jo.Description = t.Description
	if t.Options.Offset != nil {
		offset := *t.Options.Offset
		jo.Offset = &offset
	}
	jo.Concurrency = t.Options.Concurrency
	jo.Retry = t.Options.Retry
	jo.Flux = t.Flux
	jo.Status = t.Status
	return json.Marshal(jo)
}

func (t TaskUpdate) Validate() error {
	switch {
	case !t.Options.Every.IsZero() && t.Options.Cron != "":
		return errors.New("cannot specify both every and cron")
	case !t.Options.Every.IsZero():
		if _, err := time.ParseDuration(t.Options.Every.String()); err != nil {
			return fmt.Errorf("every: %s is invalid", err)
		}
	case t.Options.Offset != nil && !t.Options.Offset.IsZero():
		if _, err := time.ParseDuration(t.Options.Offset.String()); err != nil {
			return fmt.Errorf("offset: %s, %s is invalid", t.Options.Offset.String(), err)
		}
	case t.Flux == nil && t.Status == nil && t.Options.IsZero():
		return errors.New("cannot update task without content")
	case t.Status != nil && *t.Status != TaskStatusActive && *t.Status != TaskStatusInactive:
		return fmt.Errorf("invalid task status: %q", *t.Status)
	}
	return nil
}

// safeParseSource calls the Flux parser.ParseSource function
// and is guaranteed not to panic.
func safeParseSource(f string) (pkg *ast.Package, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &Error{
				Code: EInternal,
				Msg:  "internal error in flux engine; unable to parse",
			}
		}
	}()

	pkg = parser.ParseSource(f)
	return pkg, err
}

// UpdateFlux updates the TaskUpdate to go from updating options to updating a flux string, that now has those updated options in it
// It zeros the options in the TaskUpdate.
func (t *TaskUpdate) UpdateFlux(oldFlux string) (err error) {
	if t.Flux != nil && *t.Flux != "" {
		oldFlux = *t.Flux
	}
	toDelete := map[string]struct{}{}
	parsedPKG, err := safeParseSource(oldFlux)
	if err != nil {
		return err
	}

	if ast.Check(parsedPKG) > 0 {
		return ast.GetError(parsedPKG)
	}
	parsed := parsedPKG.Files[0]
	if !t.Options.Every.IsZero() && t.Options.Cron != "" {
		return errors.New("cannot specify both cron and every")
	}
	op := make(map[string]ast.Expression, 4)

	if t.Options.Name != "" {
		op["name"] = &ast.StringLiteral{Value: t.Options.Name}
	}
	if !t.Options.Every.IsZero() {
		op["every"] = &t.Options.Every.Node
	}
	if t.Options.Cron != "" {
		op["cron"] = &ast.StringLiteral{Value: t.Options.Cron}
	}
	if t.Options.Offset != nil {
		if !t.Options.Offset.IsZero() {
			op["offset"] = &t.Options.Offset.Node
		} else {
			toDelete["offset"] = struct{}{}
		}
	}
	if len(op) > 0 || len(toDelete) > 0 {
		editFunc := func(opt *ast.OptionStatement) (ast.Expression, error) {
			a, ok := opt.Assignment.(*ast.VariableAssignment)
			if !ok {
				return nil, errors.New("option assignment must be variable assignment")
			}
			obj, ok := a.Init.(*ast.ObjectExpression)
			if !ok {
				return nil, fmt.Errorf("value is is %s, not an object expression", a.Init.Type())
			}
			// modify in the keys and values that already are in the ast
			for i, p := range obj.Properties {
				k := p.Key.Key()
				if _, ok := toDelete[k]; ok {
					obj.Properties = append(obj.Properties[:i], obj.Properties[i+1:]...)
				}
				switch k {
				case "name":
					if name, ok := op["name"]; ok && t.Options.Name != "" {
						delete(op, "name")
						p.Value = name
					}
				case "offset":
					if offset, ok := op["offset"]; ok && t.Options.Offset != nil {
						delete(op, "offset")
						p.Value = offset.Copy().(*ast.DurationLiteral)
					}
				case "every":
					if every, ok := op["every"]; ok && !t.Options.Every.IsZero() {
						p.Value = every.Copy().(*ast.DurationLiteral)
						delete(op, "every")
					} else if cron, ok := op["cron"]; ok && t.Options.Cron != "" {
						delete(op, "cron")
						p.Value = cron
						p.Key = &ast.Identifier{Name: "cron"}
					}
				case "cron":
					if cron, ok := op["cron"]; ok && t.Options.Cron != "" {
						delete(op, "cron")
						p.Value = cron
					} else if every, ok := op["every"]; ok && !t.Options.Every.IsZero() {
						delete(op, "every")
						p.Key = &ast.Identifier{Name: "every"}
						p.Value = every.Copy().(*ast.DurationLiteral)
					}
				}
			}
			// add in new keys and values to the ast
			for k := range op {
				obj.Properties = append(obj.Properties, &ast.Property{
					Key:   &ast.Identifier{Name: k},
					Value: op[k],
				})
			}
			return nil, nil
		}

		ok, err := edit.Option(parsed, "task", editFunc)

		if err != nil {
			return err
		}
		if !ok {
			return errors.New("unable to edit option")
		}

		t.Options.Clear()
		s := ast.Format(parsed)
		t.Flux = &s
	}
	return nil
}

// TaskFilter represents a set of filters that restrict the returned results
type TaskFilter struct {
	Type           *string
	Name           *string
	After          *ID
	OrganizationID *ID
	Organization   string
	User           *ID
	Limit          int
}

// QueryParams Converts TaskFilter fields to url query params.
func (f TaskFilter) QueryParams() map[string][]string {
	qp := map[string][]string{}
	if f.After != nil {
		qp["after"] = []string{f.After.String()}
	}

	if f.OrganizationID != nil {
		qp["orgID"] = []string{f.OrganizationID.String()}
	}

	if f.Organization != "" {
		qp["org"] = []string{f.Organization}
	}

	if f.User != nil {
		qp["user"] = []string{f.User.String()}
	}

	if f.Limit > 0 {
		qp["limit"] = []string{strconv.Itoa(f.Limit)}
	}

	return qp
}

// RunFilter represents a set of filters that restrict the returned results
type RunFilter struct {
	// Task ID is required for listing runs.
	Task ID

	After      *ID
	Limit      int
	AfterTime  string
	BeforeTime string
}

// LogFilter represents a set of filters that restrict the returned log results.
type LogFilter struct {
	// Task ID is required.
	Task ID

	// The optional Run ID limits logs to a single run.
	Run *ID
}
