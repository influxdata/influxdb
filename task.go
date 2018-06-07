package platform

import "context"

// Task is a task. 🎊
type Task struct {
	ID     ID     `json:"id,omitempty"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Owner  User   `json:"owner"`
	IFQL   string `json:"ifql"`
	Every  string `json:"every,omitempty"`
	Cron   string `json:"cron,omitempty"`
}

// TaskService represents a service for managing one-off and recurring tasks.
type TaskService interface {
	FindTaskByID(ctx context.Context, id ID) (*Task, error)

	// Returns a task that matches filter.
	FindTask(ctx context.Context, filter TaskFilter) (*Task, error)

	// Returns a list of tasks that match a filter (limit 100) and the total count
	// of matching tasks.
	FindTasks(ctx context.Context, filter TaskFilter) ([]*Task, int, error)

	// Creates a new task
	CreateTask(ctx context.Context, t *Task) error

	// Updates a single task with changeset
	UpdateTask(ctx context.Context, id ID, upd TaskUpdate) (*Task, error)

	// Removes a task by ID and purges all associated data and scheduled runs
	DeleteTask(ctx context.Context, id ID) error
}

// TaskUpdate represents updates to a task
type TaskUpdate struct {
	Name *string `json:"name"`
}

// TaskFilter represents a set of filters that restrict the returned results
type TaskFilter struct {
	After        *ID
	Organization *ID
	User         *ID
}
