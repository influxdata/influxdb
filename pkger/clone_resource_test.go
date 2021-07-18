package pkger

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestTaskToObject just checks that if we convert a task to an object, and then
// parse it back again, we don't lose anything useful.
func TestTaskToObject(t *testing.T) {

	tasks := map[string]influxdb.Task{
		"Active Task": {
			ID:             1234,
			OrganizationID: 5678,
			Organization:   "Test Org",
			OwnerID:        9012,
			Name:           "Test Task 1",
			Description:    "An active task",
			Status:         "active",
			Flux:           `from(bucket: "apps")\n  |> to(bucket: "more_apps")`,
			Every:          "5m0s",
			Offset:         0,
		},
		"Inactive Task": {
			ID:             1234,
			OrganizationID: 5678,
			Organization:   "Test Org",
			OwnerID:        9012,
			Name:           "Test Task 2",
			Description:    "An inactive task",
			Status:         "inactive",
			Flux:           `from(bucket: "apps")\n  |> to(bucket: "more_apps")`,
			Cron:           "*/5 * * * *",
		},
	}

	for name, task := range tasks {
		t.Run(name, func(t *testing.T) {

			obj := TaskToObject(task.Name, task)
			b, err := yaml.Marshal(obj)
			require.NoError(t, err, "unexpected error marshalling object")

			tmpl, err := Parse(EncodingYAML, FromString(string(b)))
			require.NoError(t, err, "unexpected error parsing template")

			parsedTasks := tmpl.tasks()
			require.Len(t, parsedTasks, 1, "expected 1 task to be returned")

			assert.Equal(t, task.Name, parsedTasks[0].Name())
			assert.Equal(t, task.Cron, parsedTasks[0].cron)
			assert.Equal(t, task.Description, parsedTasks[0].description)
			assert.Equal(t, task.Offset, parsedTasks[0].offset)
			assert.Equal(t, task.Flux, parsedTasks[0].query.Query)
			assert.Equal(t, task.Status, parsedTasks[0].status)

			// The .String() on the parsed task will return 0s if the duration is
			// empty, which shouldn't match, so
			if task.Every != "" {
				assert.Equal(t, task.Every, parsedTasks[0].every.String())
			} else {
				assert.Empty(t, parsedTasks[0].every)
			}
		})
	}
}
