package label_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/mock"
	"go.uber.org/zap/zaptest"
)

func TestLabels(t *testing.T) {
	setup := func(t *testing.T, store *label.Store, tx kv.Tx) {
		for i := 1; i <= 10; i++ {
			mock.SetIDForFunc(&store.IDGenerator, influxdb.ID(i), func() {
				err := store.CreateLabel(context.Background(), tx, &influxdb.Label{
					Name:  fmt.Sprintf("labelname%d", i),
					OrgID: influxdb.ID(i),
				})

				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}

	setupForList := func(t *testing.T, store *label.Store, tx kv.Tx) {
		setup(t, store, tx)

		mock.SetIDForFunc(&store.IDGenerator, influxdb.ID(11), func() {
			err := store.CreateLabel(context.Background(), tx, &influxdb.Label{
				Name:  fmt.Sprintf("labelname%d", 11),
				OrgID: influxdb.ID(5),
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}

	tt := []struct {
		name    string
		setup   func(*testing.T, *label.Store, kv.Tx)
		update  func(*testing.T, *label.Store, kv.Tx)
		results func(*testing.T, *label.Store, kv.Tx)
	}{
		{
			name:  "create",
			setup: setup,
			results: func(t *testing.T, store *label.Store, tx kv.Tx) {
				labels, err := store.ListLabels(context.Background(), tx, influxdb.LabelFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(labels) != 10 {
					t.Fatalf("expected 10 labels, got: %d", len(labels))
				}

				expected := []*influxdb.Label{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Label{
						ID:    influxdb.ID(i),
						Name:  fmt.Sprintf("labelname%d", i),
						OrgID: influxdb.ID(i),
					})
				}
				if !reflect.DeepEqual(labels, expected) {
					t.Fatalf("expected identical labels: \n%+v\n%+v", labels, expected)
				}
			},
		},
		{
			name:  "get",
			setup: setup,
			results: func(t *testing.T, store *label.Store, tx kv.Tx) {
				label, err := store.GetLabel(context.Background(), tx, influxdb.ID(1))
				if err != nil {
					t.Fatal(err)
				}

				expected := &influxdb.Label{
					ID:    influxdb.ID(1),
					Name:  "labelname1",
					OrgID: influxdb.ID(1),
				}

				if !reflect.DeepEqual(label, expected) {
					t.Fatalf("expected identical label: \n%+v\n%+v", label, expected)
				}
			},
		},
		{
			name:  "list",
			setup: setupForList,
			results: func(t *testing.T, store *label.Store, tx kv.Tx) {
				// list all
				labels, err := store.ListLabels(context.Background(), tx, influxdb.LabelFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(labels) != 11 {
					t.Fatalf("expected 11 labels, got: %d", len(labels))
				}

				expected := []*influxdb.Label{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Label{
						ID:    influxdb.ID(i),
						Name:  fmt.Sprintf("labelname%d", i),
						OrgID: influxdb.ID(i),
					})
				}
				expected = append(expected, &influxdb.Label{
					ID:    influxdb.ID(11),
					Name:  fmt.Sprintf("labelname%d", 11),
					OrgID: influxdb.ID(5),
				})

				if !reflect.DeepEqual(labels, expected) {
					t.Fatalf("expected identical labels: \n%+v\n%+v", labels, expected)
				}

				// filter by name
				l, err := store.ListLabels(context.Background(), tx, influxdb.LabelFilter{Name: "labelname5"})
				if err != nil {
					t.Fatal(err)
				}

				if len(l) != 1 {
					t.Fatalf("expected 1 label, got: %d", len(l))
				}

				expectedLabel := []*influxdb.Label{&influxdb.Label{
					ID:    influxdb.ID(5),
					Name:  "labelname5",
					OrgID: influxdb.ID(5),
				}}
				if !reflect.DeepEqual(l, expectedLabel) {
					t.Fatalf("label returned by list did not match expected: \n%+v\n%+v", l, expectedLabel)
				}

				// filter by org id
				id := influxdb.ID(5)
				l, err = store.ListLabels(context.Background(), tx, influxdb.LabelFilter{OrgID: &id})
				if err != nil {
					t.Fatal(err)
				}

				if len(l) != 2 {
					t.Fatalf("expected 2 labels, got: %d", len(l))
				}

				expectedLabel = []*influxdb.Label{
					&influxdb.Label{
						ID:    influxdb.ID(5),
						Name:  "labelname5",
						OrgID: influxdb.ID(5)},
					{
						ID:    influxdb.ID(11),
						Name:  "labelname11",
						OrgID: influxdb.ID(5),
					}}
				if !reflect.DeepEqual(l, expectedLabel) {
					t.Fatalf("label returned by list did not match expected: \n%+v\n%+v", l, expectedLabel)
				}
			},
		},
		{
			name:  "update",
			setup: setup,
			update: func(t *testing.T, store *label.Store, tx kv.Tx) {
				upd := influxdb.LabelUpdate{Name: "newName"}
				updated, err := store.UpdateLabel(context.Background(), tx, influxdb.ID(1), upd)
				if err != nil {
					t.Fatal(err)
				}

				if updated.Name != upd.Name {
					t.Fatalf("expected updated name %s, got: %s", upd.Name, updated.Name)
				}
			},
			results: func(t *testing.T, store *label.Store, tx kv.Tx) {
				la, err := store.GetLabel(context.Background(), tx, influxdb.ID(1))
				if err != nil {
					t.Fatal(err)
				}

				if la.Name != "newName" {
					t.Fatalf("expected update name to be %s, got: %s", "newName", la.Name)
				}
			},
		},
		{
			name:  "delete",
			setup: setup,
			update: func(t *testing.T, store *label.Store, tx kv.Tx) {
				err := store.DeleteLabel(context.Background(), tx, influxdb.ID(5))
				if err != nil {
					t.Fatal(err)
				}

				err = store.DeleteLabel(context.Background(), tx, influxdb.ID(5))
				if err != label.ErrLabelNotFound {
					t.Fatal("expected label not found error when deleting bucket that has already been deleted, got: ", err)
				}
			},
			results: func(t *testing.T, store *label.Store, tx kv.Tx) {
				l, err := store.ListLabels(context.Background(), tx, influxdb.LabelFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(l) != 9 {
					t.Fatalf("expected 2 labels, got: %d", len(l))
				}
			},
		},
	}

	for _, testScenario := range tt {
		t.Run(testScenario.name, func(t *testing.T) {
			t.Parallel()

			store := inmem.NewKVStore()
			if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
				t.Fatal(err)
			}

			ts, err := label.NewStore(store)
			if err != nil {
				t.Fatal(err)
			}

			// setup
			if testScenario.setup != nil {
				err := ts.Update(context.Background(), func(tx kv.Tx) error {
					testScenario.setup(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// update
			if testScenario.update != nil {
				err := ts.Update(context.Background(), func(tx kv.Tx) error {
					testScenario.update(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// results
			if testScenario.results != nil {
				err := ts.View(context.Background(), func(tx kv.Tx) error {
					testScenario.results(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
