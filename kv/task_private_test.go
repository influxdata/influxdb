package kv

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

func Test_newTaskMatchFN(t *testing.T) {
	ct := func(typ string, name string) *basicKvTask {
		return &basicKvTask{
			Type:           typ,
			OrganizationID: 1,
			Name:           name,
		}
	}

	const (
		NoOrg = platform.ID(0)
		NoTyp = "-"
		NoNam = "-"
	)

	newMatch := func(orgID platform.ID, typ string, name string) taskMatchFn {
		var (
			org *influxdb.Organization
			fil taskmodel.TaskFilter
		)

		if orgID != NoOrg {
			org = &influxdb.Organization{ID: orgID}
		}

		if typ != NoTyp {
			fil.Type = &typ
		}

		if name != NoNam {
			fil.Name = &name
		}

		return newTaskMatchFn(fil, org)
	}

	type test struct {
		name string
		task matchableTask
		fn   taskMatchFn
		exp  bool
	}

	tests := []struct {
		name  string
		tests []test
	}{
		{
			"match org",
			[]test{
				{
					name: "equal",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(1, NoTyp, NoNam),
					exp:  true,
				},
				{
					name: "not org",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(2, NoTyp, NoNam),
					exp:  false,
				},
			},
		},
		{
			"match type",
			[]test{
				{
					name: "empty with system type",
					task: ct("", "Foo"),
					fn:   newMatch(NoOrg, taskmodel.TaskSystemType, NoNam),
					exp:  true,
				},
				{
					name: "system with system type",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(NoOrg, taskmodel.TaskSystemType, NoNam),
					exp:  true,
				},
				{
					name: "equal",
					task: ct("other type", "Foo"),
					fn:   newMatch(NoOrg, "other type", NoNam),
					exp:  true,
				},
				{
					name: "not type",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(NoOrg, "other type", NoNam),
					exp:  false,
				},
			},
		},
		{
			"match name",
			[]test{
				{
					name: "equal",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(NoOrg, NoTyp, "Foo"),
					exp:  true,
				},
				{
					name: "not name",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(NoOrg, NoTyp, "Bar"),
					exp:  false,
				},
			},
		},
		{
			"match org type",
			[]test{
				{
					name: "equal",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(1, taskmodel.TaskSystemType, NoNam),
					exp:  true,
				},
				{
					name: "not type",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(1, "wrong type", NoNam),
					exp:  false,
				},
				{
					name: "not org",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(2, taskmodel.TaskSystemType, NoNam),
					exp:  false,
				},
				{
					name: "not org and type",
					task: ct("check", "Foo"),
					fn:   newMatch(2, taskmodel.TaskSystemType, NoNam),
					exp:  false,
				},
			},
		},
		{
			"match org name",
			[]test{
				{
					name: "equal",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(1, NoTyp, "Foo"),
					exp:  true,
				},
				{
					name: "not org",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(2, NoTyp, "Foo"),
					exp:  false,
				},
			},
		},
		{
			"match org name type",
			[]test{
				{
					name: "equal",
					task: ct("check", "Foo"),
					fn:   newMatch(1, "check", "Foo"),
					exp:  true,
				},
				{
					name: "not org",
					task: ct("check", "Foo"),
					fn:   newMatch(2, "check", "Foo"),
					exp:  false,
				},
				{
					name: "not name",
					task: ct("check", "Foo"),
					fn:   newMatch(1, "check", "Bar"),
					exp:  false,
				},
				{
					name: "not type",
					task: ct("check", "Foo"),
					fn:   newMatch(1, "other", "Foo"),
					exp:  false,
				},
			},
		},
	}
	for _, group := range tests {
		t.Run(group.name, func(t *testing.T) {
			for _, test := range group.tests {
				t.Run(test.name, func(t *testing.T) {
					if got, exp := test.fn(test.task), test.exp; got != exp {
						t.Errorf("unxpected match result: -got/+exp\n%v", cmp.Diff(got, exp))
					}
				})
			}
		})
	}

	t.Run("match returns nil for no filter", func(t *testing.T) {
		fn := newTaskMatchFn(taskmodel.TaskFilter{}, nil)
		if fn != nil {
			t.Error("expected nil")
		}
	})
}
