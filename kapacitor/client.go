package kapacitor

import (
	"context"
	"fmt"

	"github.com/influxdata/chronograf"
	client "github.com/influxdata/kapacitor/client/v1"
)

type Client struct {
	URL      string
	Username string
	Password string
	ID       chronograf.ID
	Ticker   chronograf.Ticker
}

const (
	Prefix  = "chronograf-v1-"
	Pattern = "chronograf-v1-*"
)

// Task represents a running kapacitor task
type Task struct {
	ID         string                // Kapacitor ID
	Href       string                // Kapacitor relative URI
	TICKScript chronograf.TICKScript // TICKScript is the running script
}

func (c *Client) Href(ID string) string {
	return fmt.Sprintf("/kapacitor/v1/tasks/%s", ID)
}

func (c *Client) Create(ctx context.Context, rule chronograf.AlertRule) (*Task, error) {
	kapa, err := c.kapaClient(ctx)
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

	taskType, err := toTask(rule.Type)
	if err != nil {
		return nil, err
	}

	kapaID := Prefix + id
	task, err := kapa.CreateTask(client.CreateTaskOptions{
		ID:         kapaID,
		Type:       taskType,
		DBRPs:      []client.DBRP{{Database: rule.Query.Database, RetentionPolicy: rule.Query.RetentionPolicy}},
		TICKscript: string(script),
		Status:     client.Enabled,
	})
	if err != nil {
		return nil, err
	}

	return &Task{
		ID:         kapaID,
		Href:       task.Link.Href,
		TICKScript: script,
	}, nil
}

func (c *Client) Delete(ctx context.Context, href string) error {
	kapa, err := c.kapaClient(ctx)
	if err != nil {
		return err
	}
	return kapa.DeleteTask(client.Link{Href: href})
}

func (c *Client) Update(ctx context.Context, href string, rule chronograf.AlertRule) (*Task, error) {
	kapa, err := c.kapaClient(ctx)
	if err != nil {
		return nil, err
	}

	script, err := c.Ticker.Generate(rule)
	if err != nil {
		return nil, err
	}

	taskType, err := toTask(rule.Type)
	if err != nil {
		return nil, err
	}

	opts := client.UpdateTaskOptions{
		TICKscript: string(script),
		Status:     client.Enabled,
		Type:       taskType,
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

	return &Task{
		ID:         task.ID,
		Href:       task.Link.Href,
		TICKScript: script,
	}, nil
}

func (c *Client) kapaClient(ctx context.Context) (*client.Client, error) {
	var creds *client.Credentials
	if c.Username != "" {
		creds = &client.Credentials{
			Method:   client.UserAuthentication,
			Username: c.Username,
			Password: c.Password,
		}
	}

	return client.New(client.Config{
		URL:         c.URL,
		Credentials: creds,
	})
}
func toTask(taskType string) (client.TaskType, error) {
	if taskType == "stream" {
		return client.StreamTask, nil
	} else if taskType == "batch" {
		return client.BatchTask, nil
	} else {
		return 0, fmt.Errorf("Unknown alert type %s", taskType)
	}
}
