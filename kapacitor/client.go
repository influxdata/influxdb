package kapacitor

import (
	"context"
	"fmt"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/uuid"
	client "github.com/influxdata/kapacitor/client/v1"
)

const (
	// Prefix is prepended to the ID of all alerts
	Prefix = "chronograf-v1-"

	// FetchRate is the rate Paginating Kapacitor Clients will consume responses
	FetchRate = 100
)

// Client communicates to kapacitor
type Client struct {
	URL        string
	Username   string
	Password   string
	ID         chronograf.ID
	Ticker     chronograf.Ticker
	kapaClient func(url, username, password string) (KapaClient, error)
}

// KapaClient represents a connection to a kapacitor instance
type KapaClient interface {
	CreateTask(opt client.CreateTaskOptions) (client.Task, error)
	Task(link client.Link, opt *client.TaskOptions) (client.Task, error)
	ListTasks(opt *client.ListTasksOptions) ([]client.Task, error)
	UpdateTask(link client.Link, opt client.UpdateTaskOptions) (client.Task, error)
	DeleteTask(link client.Link) error
}

// NewClient creates a client that interfaces with Kapacitor tasks
func NewClient(url, username, password string) *Client {
	return &Client{
		URL:        url,
		Username:   username,
		Password:   password,
		ID:         &uuid.V4{},
		Ticker:     &Alert{},
		kapaClient: NewKapaClient,
	}
}

// Task represents a running kapacitor task
type Task struct {
	ID         string                // Kapacitor ID
	Type       string                // Kapacitor type (stream or batch)
	DB         string                // DB this task is associated with
	RP         string                // RP this task is associated with
	Status     string                // Status is the current state of the task
	Href       string                // Kapacitor relative URI
	HrefOutput string                // Kapacitor relative URI to HTTPOutNode
	Rule       chronograf.AlertRule  // Rule is the rule that represents this Task
	TICKScript chronograf.TICKScript // TICKScript is the running script
}

// NewTask creates a task from a kapacitor client task
func NewTask(task *client.Task) *Task {
	db, rp := "", ""
	if len(task.DBRPs) > 0 {
		dbrp := task.DBRPs[0]
		db, rp = dbrp.Database, dbrp.RetentionPolicy
	}

	script := chronograf.TICKScript(task.TICKscript)
	rule, err := Reverse(script)
	if err != nil {
		rule = chronograf.AlertRule{
			ID:         task.ID,
			Name:       task.ID,
			Query:      nil,
			TICKScript: script,
		}
	}

	return &Task{
		ID:         task.ID,
		Type:       task.Type.String(),
		DB:         db,
		RP:         rp,
		Status:     task.Status.String(),
		Href:       task.Link.Href,
		HrefOutput: HrefOutput(task.ID),
		Rule:       rule,
		TICKScript: script,
	}
}

// HrefOutput returns the link to a kapacitor task httpOut Node given an id
func HrefOutput(ID string) string {
	return fmt.Sprintf("/kapacitor/v1/tasks/%s/%s", ID, HTTPEndpoint)
}

// Href returns the link to a kapacitor task given an id
func (c *Client) Href(ID string) string {
	return fmt.Sprintf("/kapacitor/v1/tasks/%s", ID)
}

// HrefOutput returns the link to a kapacitor task httpOut Node given an id
func (c *Client) HrefOutput(ID string) string {
	return HrefOutput(ID)
}

// Create builds and POSTs a tickscript to kapacitor
func (c *Client) Create(ctx context.Context, rule chronograf.AlertRule) (*Task, error) {
	kapa, err := c.kapaClient(c.URL, c.Username, c.Password)
	if err != nil {
		return nil, err
	}

	id, err := c.ID.Generate()
	if err != nil {
		return nil, err
	}

	script, err := c.Ticker.Generate(rule)
	if err != nil {
		return nil, err
	}

	kapaID := Prefix + id
	rule.ID = kapaID
	task, err := kapa.CreateTask(client.CreateTaskOptions{
		ID:         kapaID,
		Type:       toTask(rule.Query),
		DBRPs:      []client.DBRP{{Database: rule.Query.Database, RetentionPolicy: rule.Query.RetentionPolicy}},
		TICKscript: string(script),
		Status:     client.Enabled,
	})
	if err != nil {
		return nil, err
	}

	return NewTask(&task), nil
}

// Delete removes tickscript task from kapacitor
func (c *Client) Delete(ctx context.Context, href string) error {
	kapa, err := c.kapaClient(c.URL, c.Username, c.Password)
	if err != nil {
		return err
	}
	return kapa.DeleteTask(client.Link{Href: href})
}

func (c *Client) updateStatus(ctx context.Context, href string, status client.TaskStatus) (*Task, error) {
	kapa, err := c.kapaClient(c.URL, c.Username, c.Password)
	if err != nil {
		return nil, err
	}

	opts := client.UpdateTaskOptions{
		Status: status,
	}

	task, err := kapa.UpdateTask(client.Link{Href: href}, opts)
	if err != nil {
		return nil, err
	}

	return NewTask(&task), nil
}

// Disable changes the tickscript status to disabled for a given href.
func (c *Client) Disable(ctx context.Context, href string) (*Task, error) {
	return c.updateStatus(ctx, href, client.Disabled)
}

// Enable changes the tickscript status to disabled for a given href.
func (c *Client) Enable(ctx context.Context, href string) (*Task, error) {
	return c.updateStatus(ctx, href, client.Enabled)
}

// AllStatus returns the status of all tasks in kapacitor
func (c *Client) AllStatus(ctx context.Context) (map[string]string, error) {
	kapa, err := c.kapaClient(c.URL, c.Username, c.Password)
	if err != nil {
		return nil, err
	}

	// Only get the status, id and link section back
	opts := &client.ListTasksOptions{
		Fields: []string{"status"},
	}
	tasks, err := kapa.ListTasks(opts)
	if err != nil {
		return nil, err
	}

	taskStatuses := map[string]string{}
	for _, task := range tasks {
		taskStatuses[task.ID] = task.Status.String()
	}

	return taskStatuses, nil
}

// Status returns the status of a task in kapacitor
func (c *Client) Status(ctx context.Context, href string) (string, error) {
	s, err := c.status(ctx, href)
	if err != nil {
		return "", err
	}

	return s.String(), nil
}

func (c *Client) status(ctx context.Context, href string) (client.TaskStatus, error) {
	kapa, err := c.kapaClient(c.URL, c.Username, c.Password)
	if err != nil {
		return 0, err
	}
	task, err := kapa.Task(client.Link{Href: href}, nil)
	if err != nil {
		return 0, err
	}

	return task.Status, nil
}

// All returns all tasks in kapacitor
func (c *Client) All(ctx context.Context) (map[string]*Task, error) {
	kapa, err := c.kapaClient(c.URL, c.Username, c.Password)
	if err != nil {
		return nil, err
	}

	// Only get the status, id and link section back
	opts := &client.ListTasksOptions{}
	tasks, err := kapa.ListTasks(opts)
	if err != nil {
		return nil, err
	}

	all := map[string]*Task{}
	for _, task := range tasks {
		all[task.ID] = NewTask(&task)
	}
	return all, nil
}

// Reverse builds a chronograf.AlertRule and its QueryConfig from a tickscript
func (c *Client) Reverse(id string, script chronograf.TICKScript) chronograf.AlertRule {
	rule, err := Reverse(script)
	if err != nil {
		return chronograf.AlertRule{
			ID:         id,
			Name:       id,
			Query:      nil,
			TICKScript: script,
		}
	}
	rule.ID = id
	rule.TICKScript = script
	return rule
}

// Get returns a single alert in kapacitor
func (c *Client) Get(ctx context.Context, id string) (*Task, error) {
	kapa, err := c.kapaClient(c.URL, c.Username, c.Password)
	if err != nil {
		return nil, err
	}
	href := c.Href(id)
	task, err := kapa.Task(client.Link{Href: href}, nil)
	if err != nil {
		return nil, chronograf.ErrAlertNotFound
	}

	return NewTask(&task), nil
}

// Update changes the tickscript of a given id.
func (c *Client) Update(ctx context.Context, href string, rule chronograf.AlertRule) (*Task, error) {
	kapa, err := c.kapaClient(c.URL, c.Username, c.Password)
	if err != nil {
		return nil, err
	}

	script, err := c.Ticker.Generate(rule)
	if err != nil {
		return nil, err
	}

	prevStatus, err := c.status(ctx, href)
	if err != nil {
		return nil, err
	}

	// We need to disable the kapacitor task followed by enabling it during update.
	opts := client.UpdateTaskOptions{
		TICKscript: string(script),
		Status:     client.Disabled,
		Type:       toTask(rule.Query),
		DBRPs: []client.DBRP{
			{
				Database:        rule.Query.Database,
				RetentionPolicy: rule.Query.RetentionPolicy,
			},
		},
	}

	task, err := kapa.UpdateTask(client.Link{Href: href}, opts)
	if err != nil {
		return nil, err
	}

	// Now enable the task if previously enabled
	if prevStatus == client.Enabled {
		if _, err := c.Enable(ctx, href); err != nil {
			return nil, err
		}
	}

	return NewTask(&task), nil
}

func toTask(q *chronograf.QueryConfig) client.TaskType {
	if q == nil || q.RawText == nil || *q.RawText == "" {
		return client.StreamTask
	}
	return client.BatchTask
}

// NewKapaClient creates a Kapacitor client connection
func NewKapaClient(url, username, password string) (KapaClient, error) {
	var creds *client.Credentials
	if username != "" {
		creds = &client.Credentials{
			Method:   client.UserAuthentication,
			Username: username,
			Password: password,
		}
	}

	clnt, err := client.New(client.Config{
		URL:         url,
		Credentials: creds,
	})

	if err != nil {
		return clnt, err
	}

	return &PaginatingKapaClient{clnt, FetchRate}, nil
}
