package kapacitor

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
	client "github.com/influxdata/kapacitor/client/v1"
)

type MockKapa struct {
	ResTask     client.Task
	ResTasks    []client.Task
	TaskError   error
	UpdateError error
	CreateError error
	ListError   error
	DeleteError error
	LastStatus  client.TaskStatus

	*client.CreateTaskOptions
	client.Link
	*client.TaskOptions
	*client.ListTasksOptions
	*client.UpdateTaskOptions
}

func (m *MockKapa) CreateTask(opt client.CreateTaskOptions) (client.Task, error) {
	m.CreateTaskOptions = &opt
	return m.ResTask, m.CreateError
}

func (m *MockKapa) Task(link client.Link, opt *client.TaskOptions) (client.Task, error) {
	m.Link = link
	m.TaskOptions = opt
	return m.ResTask, m.TaskError
}

func (m *MockKapa) ListTasks(opt *client.ListTasksOptions) ([]client.Task, error) {
	m.ListTasksOptions = opt
	return m.ResTasks, m.ListError
}

func (m *MockKapa) UpdateTask(link client.Link, opt client.UpdateTaskOptions) (client.Task, error) {
	m.Link = link
	m.LastStatus = opt.Status

	if m.UpdateTaskOptions == nil {
		m.UpdateTaskOptions = &opt
	}

	return m.ResTask, m.UpdateError
}

func (m *MockKapa) DeleteTask(link client.Link) error {
	m.Link = link
	return m.DeleteError
}

type MockID struct {
	ID string
}

func (m *MockID) Generate() (string, error) {
	return m.ID, nil
}
func TestClient_AllStatus(t *testing.T) {
	type fields struct {
		URL        string
		Username   string
		Password   string
		ID         chronograf.ID
		Ticker     chronograf.Ticker
		kapaClient func(url, username, password string) (KapaClient, error)
	}
	type args struct {
		ctx context.Context
	}
	kapa := &MockKapa{}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     map[string]string
		wantErr  bool
		resTask  client.Task
		resTasks []client.Task
		resError error

		createTaskOptions client.CreateTaskOptions
		link              client.Link
		taskOptions       *client.TaskOptions
		listTasksOptions  *client.ListTasksOptions
		updateTaskOptions client.UpdateTaskOptions
	}{
		{
			name: "return no tasks",
			fields: fields{
				URL:      "http://hill-valley-preservation-society.org",
				Username: "ElsaRaven",
				Password: "save the clock tower",
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			listTasksOptions: &client.ListTasksOptions{
				Fields: []string{"status"},
			},
			want: map[string]string{},
		},
		{
			name: "return two tasks",
			fields: fields{
				URL:      "http://hill-valley-preservation-society.org",
				Username: "ElsaRaven",
				Password: "save the clock tower",
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			listTasksOptions: &client.ListTasksOptions{
				Fields: []string{"status"},
			},
			resTasks: []client.Task{
				client.Task{
					ID:     "howdy",
					Status: client.Enabled,
				},
				client.Task{
					ID:     "doody",
					Status: client.Disabled,
				},
			},
			want: map[string]string{
				"howdy": "enabled",
				"doody": "disabled",
			},
		},
		{
			name: "list task error",
			fields: fields{
				URL:      "http://hill-valley-preservation-society.org",
				Username: "ElsaRaven",
				Password: "save the clock tower",
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			listTasksOptions: &client.ListTasksOptions{
				Fields: []string{"status"},
			},
			resError: fmt.Errorf("this is an error"),
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		kapa.ResTask = tt.resTask
		kapa.ResTasks = tt.resTasks
		kapa.ListError = tt.resError

		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				URL:        tt.fields.URL,
				Username:   tt.fields.Username,
				Password:   tt.fields.Password,
				ID:         tt.fields.ID,
				Ticker:     tt.fields.Ticker,
				kapaClient: tt.fields.kapaClient,
			}
			got, err := c.AllStatus(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.AllStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.AllStatus() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(kapa.ListTasksOptions, tt.listTasksOptions) {
				t.Errorf("Client.AllStatus() = listTasksOptions  %v, want %v", kapa.ListTasksOptions, tt.listTasksOptions)
			}
			if !reflect.DeepEqual(kapa.TaskOptions, tt.taskOptions) {
				t.Errorf("Client.AllStatus() = taskOptions  %v, want %v", kapa.TaskOptions, tt.taskOptions)
			}
			if !reflect.DeepEqual(kapa.ListTasksOptions, tt.listTasksOptions) {
				t.Errorf("Client.AllStatus() = listTasksOptions  %v, want %v", kapa.ListTasksOptions, tt.listTasksOptions)
			}
			if !reflect.DeepEqual(kapa.Link, tt.link) {
				t.Errorf("Client.AllStatus() = Link  %v, want %v", kapa.Link, tt.link)
			}
		})
	}
}

func TestClient_All(t *testing.T) {
	type fields struct {
		URL        string
		Username   string
		Password   string
		ID         chronograf.ID
		Ticker     chronograf.Ticker
		kapaClient func(url, username, password string) (KapaClient, error)
	}
	type args struct {
		ctx context.Context
	}
	kapa := &MockKapa{}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     map[string]chronograf.AlertRule
		wantErr  bool
		resTask  client.Task
		resTasks []client.Task
		resError error

		createTaskOptions client.CreateTaskOptions
		link              client.Link
		taskOptions       *client.TaskOptions
		listTasksOptions  *client.ListTasksOptions
		updateTaskOptions client.UpdateTaskOptions
	}{
		{
			name: "return no tasks",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			listTasksOptions: &client.ListTasksOptions{},
			want:             map[string]chronograf.AlertRule{},
		},
		{
			name: "return a non-reversible task",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			listTasksOptions: &client.ListTasksOptions{},
			resTasks: []client.Task{
				client.Task{
					ID:     "howdy",
					Status: client.Enabled,
				},
			},
			want: map[string]chronograf.AlertRule{
				"howdy": chronograf.AlertRule{
					ID:         "howdy",
					Name:       "howdy",
					TICKScript: "",
				},
			},
		},
		{
			name: "return a reversible task",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			listTasksOptions: &client.ListTasksOptions{},
			resTasks: []client.Task{
				client.Task{
					ID:     "rule 1",
					Status: client.Enabled,
					TICKscript: `var db = '_internal'

var rp = 'monitor'

var measurement = 'cq'

var groupBy = []

var whereFilter = lambda: TRUE

var name = 'rule 1'

var idVar = name + ':{{.Group}}'

var message = ''

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var crit = 90000

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |eval(lambda: "queryOk")
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
`,
				},
			},
			want: map[string]chronograf.AlertRule{
				"rule 1": chronograf.AlertRule{
					ID:   "rule 1",
					Name: "rule 1",
					TICKScript: `var db = '_internal'

var rp = 'monitor'

var measurement = 'cq'

var groupBy = []

var whereFilter = lambda: TRUE

var name = 'rule 1'

var idVar = name + ':{{.Group}}'

var message = ''

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var crit = 90000

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |eval(lambda: "queryOk")
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
`,
					Trigger: "threshold",
					Alerts:  []string{},
					TriggerValues: chronograf.TriggerValues{
						Operator: "greater than",
						Value:    "90000",
					},
					Query: &chronograf.QueryConfig{
						Database:        "_internal",
						RetentionPolicy: "monitor",
						Measurement:     "cq",
						Fields: []chronograf.Field{
							{
								Field: "queryOk",
								Funcs: []string{},
							},
						},
						GroupBy: chronograf.GroupBy{
							Tags: []string{},
						},
						AreTagsAccepted: false,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		kapa.ResTask = tt.resTask
		kapa.ResTasks = tt.resTasks
		kapa.ListError = tt.resError
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				URL:        tt.fields.URL,
				Username:   tt.fields.Username,
				Password:   tt.fields.Password,
				ID:         tt.fields.ID,
				Ticker:     tt.fields.Ticker,
				kapaClient: tt.fields.kapaClient,
			}
			got, err := c.All(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.All() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.All() = %#v, want %#v", got, tt.want)
			}
			if !reflect.DeepEqual(kapa.ListTasksOptions, tt.listTasksOptions) {
				t.Errorf("Client.All() = listTasksOptions  %v, want %v", kapa.ListTasksOptions, tt.listTasksOptions)
			}
			if !reflect.DeepEqual(kapa.TaskOptions, tt.taskOptions) {
				t.Errorf("Client.All() = taskOptions  %v, want %v", kapa.TaskOptions, tt.taskOptions)
			}
			if !reflect.DeepEqual(kapa.ListTasksOptions, tt.listTasksOptions) {
				t.Errorf("Client.All() = listTasksOptions  %v, want %v", kapa.ListTasksOptions, tt.listTasksOptions)
			}
			if !reflect.DeepEqual(kapa.Link, tt.link) {
				t.Errorf("Client.All() = Link  %v, want %v", kapa.Link, tt.link)
			}
		})
	}
}

func TestClient_Get(t *testing.T) {
	type fields struct {
		URL        string
		Username   string
		Password   string
		ID         chronograf.ID
		Ticker     chronograf.Ticker
		kapaClient func(url, username, password string) (KapaClient, error)
	}
	type args struct {
		ctx context.Context
		id  string
	}
	kapa := &MockKapa{}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     chronograf.AlertRule
		wantErr  bool
		resTask  client.Task
		resTasks []client.Task
		resError error

		createTaskOptions client.CreateTaskOptions
		link              client.Link
		taskOptions       *client.TaskOptions
		listTasksOptions  *client.ListTasksOptions
		updateTaskOptions client.UpdateTaskOptions
	}{
		{
			name: "return no task",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			args: args{
				id: "myid",
			},
			taskOptions: nil,
			wantErr:     true,
			resError:    fmt.Errorf("No such task"),
			link: client.Link{
				Href: "/kapacitor/v1/tasks/myid",
			},
		},
		{
			name: "return non-reversible task",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			args: args{
				id: "myid",
			},
			taskOptions: nil,
			resTask: client.Task{
				ID: "myid",
			},
			want: chronograf.AlertRule{
				ID:   "myid",
				Name: "myid",
			},
			link: client.Link{
				Href: "/kapacitor/v1/tasks/myid",
			},
		},
		{
			name: "return reversible task",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
			},
			args: args{
				id: "rule 1",
			},
			taskOptions: nil,
			resTask: client.Task{
				ID: "rule 1",
				TICKscript: `var db = '_internal'

var rp = 'monitor'

var measurement = 'cq'

var groupBy = []

var whereFilter = lambda: TRUE

var name = 'rule 1'

var idVar = name + ':{{.Group}}'

var message = ''

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var crit = 90000

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |eval(lambda: "queryOk")
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
`,
			},
			want: chronograf.AlertRule{
				ID:   "rule 1",
				Name: "rule 1",
				TICKScript: `var db = '_internal'

var rp = 'monitor'

var measurement = 'cq'

var groupBy = []

var whereFilter = lambda: TRUE

var name = 'rule 1'

var idVar = name + ':{{.Group}}'

var message = ''

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var crit = 90000

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |eval(lambda: "queryOk")
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
`,
				Trigger: "threshold",
				Alerts:  []string{},
				TriggerValues: chronograf.TriggerValues{
					Operator: "greater than",
					Value:    "90000",
				},
				Query: &chronograf.QueryConfig{
					Database:        "_internal",
					RetentionPolicy: "monitor",
					Measurement:     "cq",
					Fields: []chronograf.Field{
						{
							Field: "queryOk",
							Funcs: []string{},
						},
					},
					GroupBy: chronograf.GroupBy{
						Tags: []string{},
					},
					AreTagsAccepted: false,
				},
			},
			link: client.Link{
				Href: "/kapacitor/v1/tasks/rule 1",
			},
		},
	}
	for _, tt := range tests {
		kapa.ResTask = tt.resTask
		kapa.ResTasks = tt.resTasks
		kapa.TaskError = tt.resError
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				URL:        tt.fields.URL,
				Username:   tt.fields.Username,
				Password:   tt.fields.Password,
				ID:         tt.fields.ID,
				Ticker:     tt.fields.Ticker,
				kapaClient: tt.fields.kapaClient,
			}
			got, err := c.Get(tt.args.ctx, tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.Get() =\n%#v\nwant\n%#v", got, tt.want)
			}
			if !reflect.DeepEqual(kapa.ListTasksOptions, tt.listTasksOptions) {
				t.Errorf("Client.Get() = listTasksOptions  %v, want %v", kapa.ListTasksOptions, tt.listTasksOptions)
			}
			if !reflect.DeepEqual(kapa.TaskOptions, tt.taskOptions) {
				t.Errorf("Client.Get() = taskOptions  %v, want %v", kapa.TaskOptions, tt.taskOptions)
			}
			if !reflect.DeepEqual(kapa.ListTasksOptions, tt.listTasksOptions) {
				t.Errorf("Client.Get() = listTasksOptions  %v, want %v", kapa.ListTasksOptions, tt.listTasksOptions)
			}
			if !reflect.DeepEqual(kapa.Link, tt.link) {
				t.Errorf("Client.Get() = Link  %v, want %v", kapa.Link, tt.link)
			}
		})
	}
}

func TestClient_updateStatus(t *testing.T) {
	type fields struct {
		URL        string
		Username   string
		Password   string
		ID         chronograf.ID
		Ticker     chronograf.Ticker
		kapaClient func(url, username, password string) (KapaClient, error)
	}
	type args struct {
		ctx    context.Context
		href   string
		status client.TaskStatus
	}
	kapa := &MockKapa{}
	tests := []struct {
		name              string
		fields            fields
		args              args
		resTask           client.Task
		want              *Task
		resError          error
		wantErr           bool
		updateTaskOptions *client.UpdateTaskOptions
	}{
		{
			name: "disable alert rule",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
				Ticker: &Alert{},
			},
			args: args{
				ctx:    context.Background(),
				href:   "/kapacitor/v1/tasks/howdy",
				status: client.Disabled,
			},
			resTask: client.Task{
				ID:     "howdy",
				Status: client.Disabled,
				Type:   client.StreamTask,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
				Link: client.Link{
					Href: "/kapacitor/v1/tasks/howdy",
				},
			},
			updateTaskOptions: &client.UpdateTaskOptions{
				TICKscript: "",
				Status:     client.Disabled,
			},
			want: &Task{
				ID:         "howdy",
				Type:       "stream",
				DB:         "db",
				RP:         "rp",
				Status:     "disabled",
				Href:       "/kapacitor/v1/tasks/howdy",
				HrefOutput: "/kapacitor/v1/tasks/howdy/output",
				Rule:       chronograf.AlertRule{},
			},
		},
		{
			name: "fail to enable alert rule",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
				Ticker: &Alert{},
			},
			args: args{
				ctx:    context.Background(),
				href:   "/kapacitor/v1/tasks/howdy",
				status: client.Enabled,
			},
			updateTaskOptions: &client.UpdateTaskOptions{
				TICKscript: "",
				Status:     client.Enabled,
			},
			resError: fmt.Errorf("error"),
			wantErr:  true,
		},
		{
			name: "enable alert rule",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
				Ticker: &Alert{},
			},
			args: args{
				ctx:    context.Background(),
				href:   "/kapacitor/v1/tasks/howdy",
				status: client.Enabled,
			},
			resTask: client.Task{
				ID:   "howdy",
				Type: client.StreamTask,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
				Status: client.Enabled,
				Link: client.Link{
					Href: "/kapacitor/v1/tasks/howdy",
				},
			},
			updateTaskOptions: &client.UpdateTaskOptions{
				TICKscript: "",
				Status:     client.Enabled,
			},
			want: &Task{
				ID:         "howdy",
				Type:       "stream",
				DB:         "db",
				RP:         "rp",
				Status:     "enabled",
				Href:       "/kapacitor/v1/tasks/howdy",
				HrefOutput: "/kapacitor/v1/tasks/howdy/output",
				Rule:       chronograf.AlertRule{},
			},
		},
	}
	for _, tt := range tests {
		kapa.ResTask = tt.resTask
		kapa.UpdateError = tt.resError
		kapa.UpdateTaskOptions = nil
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				URL:        tt.fields.URL,
				Username:   tt.fields.Username,
				Password:   tt.fields.Password,
				ID:         tt.fields.ID,
				Ticker:     tt.fields.Ticker,
				kapaClient: tt.fields.kapaClient,
			}
			got, err := c.updateStatus(tt.args.ctx, tt.args.href, tt.args.status)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.updateStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.updateStatus() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(kapa.UpdateTaskOptions, tt.updateTaskOptions) {
				t.Errorf("Client.updateStatus() = %v, want %v", kapa.UpdateTaskOptions, tt.updateTaskOptions)
			}
		})
	}
}

func TestClient_Update(t *testing.T) {
	type fields struct {
		URL        string
		Username   string
		Password   string
		ID         chronograf.ID
		Ticker     chronograf.Ticker
		kapaClient func(url, username, password string) (KapaClient, error)
	}
	type args struct {
		ctx  context.Context
		href string
		rule chronograf.AlertRule
	}
	kapa := &MockKapa{}
	tests := []struct {
		name              string
		fields            fields
		args              args
		resTask           client.Task
		want              *Task
		resError          error
		wantErr           bool
		updateTaskOptions *client.UpdateTaskOptions
		wantStatus        client.TaskStatus
	}{
		{
			name: "update alert rule error",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
				Ticker: &Alert{},
			},
			args: args{
				ctx:  context.Background(),
				href: "/kapacitor/v1/tasks/howdy",
				rule: chronograf.AlertRule{
					ID: "howdy",
					Query: &chronograf.QueryConfig{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			resError: fmt.Errorf("error"),
			updateTaskOptions: &client.UpdateTaskOptions{
				TICKscript: "",
				Type:       client.StreamTask,
				Status:     client.Disabled,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			wantErr:    true,
			wantStatus: client.Disabled,
		},
		{
			name: "update alert rule",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
				Ticker: &Alert{},
			},
			args: args{
				ctx:  context.Background(),
				href: "/kapacitor/v1/tasks/howdy",
				rule: chronograf.AlertRule{
					ID: "howdy",
					Query: &chronograf.QueryConfig{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			resTask: client.Task{
				ID:   "howdy",
				Type: client.StreamTask,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
				Status: client.Enabled,
				Link: client.Link{
					Href: "/kapacitor/v1/tasks/howdy",
				},
			},
			updateTaskOptions: &client.UpdateTaskOptions{
				TICKscript: "",
				Type:       client.StreamTask,
				Status:     client.Disabled,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			want: &Task{
				ID:         "howdy",
				Href:       "/kapacitor/v1/tasks/howdy",
				HrefOutput: "/kapacitor/v1/tasks/howdy/output",
				Rule: chronograf.AlertRule{
					ID:   "howdy",
					Name: "howdy",
				},
			},
			wantStatus: client.Enabled,
		},
		{
			name: "stays disabled when already disabled",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
				Ticker: &Alert{},
			},
			args: args{
				ctx:  context.Background(),
				href: "/kapacitor/v1/tasks/howdy",
				rule: chronograf.AlertRule{
					ID: "howdy",
					Query: &chronograf.QueryConfig{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			resTask: client.Task{
				ID:   "howdy",
				Type: client.StreamTask,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
				Status: client.Disabled,
				Link: client.Link{
					Href: "/kapacitor/v1/tasks/howdy",
				},
			},
			updateTaskOptions: &client.UpdateTaskOptions{
				TICKscript: "",
				Type:       client.StreamTask,
				Status:     client.Disabled,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			want: &Task{
				ID:         "howdy",
				Href:       "/kapacitor/v1/tasks/howdy",
				HrefOutput: "/kapacitor/v1/tasks/howdy/output",
				Rule: chronograf.AlertRule{
					ID:   "howdy",
					Name: "howdy",
				},
			},
			wantStatus: client.Disabled,
		},
	}
	for _, tt := range tests {
		kapa.ResTask = tt.resTask
		kapa.UpdateError = tt.resError
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				URL:        tt.fields.URL,
				Username:   tt.fields.Username,
				Password:   tt.fields.Password,
				ID:         tt.fields.ID,
				Ticker:     tt.fields.Ticker,
				kapaClient: tt.fields.kapaClient,
			}
			got, err := c.Update(tt.args.ctx, tt.args.href, tt.args.rule)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Update() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.Update() =\n%#+v\n, want\n%#+v\n", got, tt.want)
			}
			if !reflect.DeepEqual(kapa.UpdateTaskOptions, tt.updateTaskOptions) {
				t.Errorf("Client.Update() = %v, want %v", kapa.UpdateTaskOptions, tt.updateTaskOptions)
			}
			if tt.wantStatus != kapa.LastStatus {
				t.Errorf("Client.Update() = %v, want %v", kapa.LastStatus, tt.wantStatus)
			}
		})
	}
}

func TestClient_Create(t *testing.T) {
	type fields struct {
		URL        string
		Username   string
		Password   string
		ID         chronograf.ID
		Ticker     chronograf.Ticker
		kapaClient func(url, username, password string) (KapaClient, error)
	}
	type args struct {
		ctx  context.Context
		rule chronograf.AlertRule
	}
	kapa := &MockKapa{}
	tests := []struct {
		name              string
		fields            fields
		args              args
		resTask           client.Task
		want              *Task
		resError          error
		wantErr           bool
		createTaskOptions *client.CreateTaskOptions
	}{
		{
			name: "create alert rule",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
				Ticker: &Alert{},
				ID: &MockID{
					ID: "howdy",
				},
			},
			args: args{
				ctx: context.Background(),
				rule: chronograf.AlertRule{
					ID: "howdy",
					Query: &chronograf.QueryConfig{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			resTask: client.Task{
				ID:     "chronograf-v1-howdy",
				Status: client.Enabled,
				Type:   client.StreamTask,
				Link: client.Link{
					Href: "/kapacitor/v1/tasks/chronograf-v1-howdy",
				},
			},
			createTaskOptions: &client.CreateTaskOptions{
				TICKscript: "",
				ID:         "chronograf-v1-howdy",
				Type:       client.StreamTask,
				Status:     client.Enabled,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			want: &Task{
				ID:         "chronograf-v1-howdy",
				Type:       "stream",
				DB:         "db",
				RP:         "rp",
				Status:     "enabled",
				Href:       "/kapacitor/v1/tasks/chronograf-v1-howdy",
				HrefOutput: "/kapacitor/v1/tasks/chronograf-v1-howdy/output",
				Rule: chronograf.AlertRule{
					ID:   "chronograf-v1-howdy",
					Name: "chronograf-v1-howdy",
				},
			},
		},
		{
			name: "create alert rule",
			fields: fields{
				kapaClient: func(url, username, password string) (KapaClient, error) {
					return kapa, nil
				},
				Ticker: &Alert{},
				ID: &MockID{
					ID: "howdy",
				},
			},
			args: args{
				ctx: context.Background(),
				rule: chronograf.AlertRule{
					ID: "howdy",
					Query: &chronograf.QueryConfig{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			resError: fmt.Errorf("error"),
			createTaskOptions: &client.CreateTaskOptions{
				TICKscript: "",
				ID:         "chronograf-v1-howdy",
				Type:       client.StreamTask,
				Status:     client.Enabled,
				DBRPs: []client.DBRP{
					{
						Database:        "db",
						RetentionPolicy: "rp",
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		kapa.ResTask = tt.resTask
		kapa.CreateError = tt.resError
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				URL:        tt.fields.URL,
				Username:   tt.fields.Username,
				Password:   tt.fields.Password,
				ID:         tt.fields.ID,
				Ticker:     tt.fields.Ticker,
				kapaClient: tt.fields.kapaClient,
			}
			got, err := c.Create(tt.args.ctx, tt.args.rule)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.Create() =\n%v\n, want\n%v\n", got, tt.want)
			}
			if !reflect.DeepEqual(kapa.CreateTaskOptions, tt.createTaskOptions) {
				t.Errorf("Client.Create() =  %v, want %v", kapa.CreateTaskOptions, tt.createTaskOptions)
			}
		})
	}
}
