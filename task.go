package platform

import "context"

// Task is a task. 🎊
type Task struct {
	ID           ID     `json:"id,omitempty"`
	Organization ID     `json:"organizationId"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	Owner        User   `json:"owner"`
	Flux         string `json:"flux"`
	Every        string `json:"every,omitempty"`
	Cron         string `json:"cron,omitempty"`
	Last         Run    `json:"last,omitempty"`
}

// Run is a record created when a run of a task is scheduled.
type Run struct {
	ID           ID     `json:"id,omitempty"`
	TaskID       ID     `json:"taskId"`
	Status       string `json:"status"`
	ScheduledFor string `json:"scheduledFor"`
	StartedAt    string `json:"startedAt,omitempty"`
	FinishedAt   string `json:"finishedAt,omitempty"`
	RequestedAt  string `json:"requestedAt,omitempty"`
	Log          Log    `json:"log"`
}

// Log represents a link to a log resource
type Log string

// TaskService represents a service for managing one-off and recurring tasks.
type TaskService interface {
	// Returns a single task
	FindTaskByID(ctx context.Context, id ID) (*Task, error)

	// Returns a list of tasks that match a filter (limit 100) and the total count
	// of matching tasks.
	FindTasks(ctx context.Context, filter TaskFilter) ([]*Task, int, error)

	// Creates a new task
	CreateTask(ctx context.Context, t *Task) error

	// Updates a single task with changeset
	UpdateTask(ctx context.Context, id ID, upd TaskUpdate) (*Task, error)

	// Removes a task by ID and purges all associated data and scheduled runs
	DeleteTask(ctx context.Context, id ID) error

	// Returns logs for a run.
	FindLogs(ctx context.Context, filter LogFilter) ([]*Log, int, error)

	// Returns a list of runs that match a filter and the total count of returned runs.
	FindRuns(ctx context.Context, filter RunFilter) ([]*Run, int, error)

	// Returns a single run
	FindRunByID(ctx context.Context, id ID) (*Run, error)

	// Creates and returns a new run (which is a retry of another run)
	RetryRun(ctx context.Context, id ID) (*Run, error)
}

// TaskUpdate represents updates to a task
type TaskUpdate struct {
	Flux   *string `json:"flux,omitempty"`
	Status *string `json:"status,omitempty"`
}

// TaskFilter represents a set of filters that restrict the returned results
type TaskFilter struct {
	After        *ID
	Organization *ID
	User         *ID
}

// RunFilter represents a set of filters that restrict the returned results
type RunFilter struct {
	Task       *ID
	After      *ID
	Limit      int
	AfterTime  string
	BeforeTime string
}

// LogFilter represents a set of filters that restrict the returned results
type LogFilter struct {
	Task *ID
	Run  *ID
}
