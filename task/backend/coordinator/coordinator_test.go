package coordinator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/task/backend"
	"github.com/influxdata/platform/task/backend/coordinator"
	"github.com/influxdata/platform/task/mock"
	platformtesting "github.com/influxdata/platform/testing"
)

func timeoutSelector(ch <-chan *mock.Task) (*mock.Task, error) {
	select {
	case task := <-ch:
		return task, nil
	case <-time.After(time.Second):
		return nil, errors.New("timeout on select")
	}
}

func TestCoordinator(t *testing.T) {
	st := backend.NewInMemStore()
	sched := mock.NewScheduler()

	coord := coordinator.New(sched, st)
	createChan := sched.TaskCreateChan()
	releaseChan := sched.TaskReleaseChan()

	orgID := platformtesting.MustIDBase16("69746f7175650d0a")
	usrID := platformtesting.MustIDBase16("6c61757320657420")
	script := `option task = {name: "a task",cron: "* * * * *"} from(bucket:"test") |> range(start:-1h)`
	id, err := coord.CreateTask(context.Background(), orgID, usrID, script, 0)
	if err != nil {
		t.Fatal(err)
	}

	task, err := timeoutSelector(createChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Script != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	deleted, err := coord.DeleteTask(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	if !deleted {
		t.Fatal("no error and not deleted")
	}

	task, err = timeoutSelector(releaseChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Script != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	id, err = coord.CreateTask(context.Background(), orgID, usrID, script, 0)
	if err != nil {
		t.Fatal(err)
	}

	task, err = timeoutSelector(createChan)
	if err != nil {
		t.Fatal(err)
	}

	err = coord.DisableTask(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	task, err = timeoutSelector(releaseChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Script != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	err = coord.EnableTask(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	task, err = timeoutSelector(createChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Script != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	newScript := `option task = {name: "a task",cron: "1 * * * *"} from(bucket:"test") |> range(start:-2h)`
	err = coord.ModifyTask(context.Background(), id, newScript)
	if err != nil {
		t.Fatal(err)
	}

	task, err = timeoutSelector(releaseChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Script != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	task, err = timeoutSelector(createChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Script != newScript {
		t.Fatal("task sent to scheduler doesnt match task created")
	}
}
