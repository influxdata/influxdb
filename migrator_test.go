package influxdb_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

type oldStruct struct {
	Version string `json:"version,omitempty"`
	ID      int    `json:"id"`
	OldFld1 string `json:"old_fld1,omitempty"`
	NewFld1 string `json:"new_fld1,omitempty"`
}

type newStruct struct {
	Version string `json:"version,omitempty"`
	ID      int    `json:"id"`
	NewFld1 string `json:"new_fld1"`
}

type mockMigrator struct {
	needMigration bool
}

func (m mockMigrator) Bucket() []byte {
	return []byte("mockMigratorv1")
}

func (m mockMigrator) NeedMigration() bool {
	return m.needMigration
}

func (m mockMigrator) Up(src []byte) (dst []byte, err error) {
	old := new(oldStruct)
	if err := json.Unmarshal(src, old); err != nil {
		return nil, influxdb.ErrBadSourceType
	}
	if old.Version == "new-version-id" {
		return nil, influxdb.ErrNoNeedToConvert
	}
	new := newStruct{
		Version: "new-version-id",
		ID:      old.ID,
		NewFld1: old.OldFld1,
	}
	return json.Marshal(new)
}

func (m mockMigrator) Down(src []byte) (dst []byte, err error) {
	new := new(newStruct)
	if err := json.Unmarshal(src, new); err != nil {
		return nil, influxdb.ErrBadSourceType
	}
	old := oldStruct{
		Version: "old-version-id",
		ID:      new.ID,
		OldFld1: new.NewFld1,
	}
	return json.Marshal(old)
}

func TestMigrationManager(t *testing.T) {
	oldData := map[string][]byte{
		"1": []byte(`{"id":1,"old_fld1":"f1"}`),
		"2": []byte(`{"id":2,"old_fld1":"f2"}`),
		"3": []byte(`{"id":3,"old_fld1":"f3"}`),
	}
	newData := map[string][]byte{
		"1": []byte(`{"version":"new-version-id","id":1,"new_fld1":"f1"}`),
		"2": []byte(`{"version":"new-version-id","id":2,"new_fld1":"f2"}`),
		"3": []byte(`{"version":"new-version-id","id":3,"new_fld1":"f3"}`),
	}
	store := &mock.MigratorService{
		Data: map[string]map[string][]byte{
			"mockMigratorv1": oldData,
		},
	}
	m := mockMigrator{
		needMigration: true,
	}
	mg := influxdb.NewMigrationManager(store, 0)
	mg.Register(m)
	ctx := context.Background()
	// Channel buffer size equals total migrators.
	errCh := make(chan error)
	done := make(chan struct{})
	// up dry run
	go mg.DryRun(ctx, errCh, done)
updry:
	for {
		select {
		case <-done:
			break updry
		case <-errCh:
			t.Fail()
		}
	}
	resultData := store.GetAll([]byte("mockMigratorv1"))
	if diff := cmp.Diff(resultData, oldData); diff != "" {
		t.Errorf("migration ups are different -got/+want\ndiff %s", diff)
	}
	// up run
	_ = mg.Migrate(ctx, true)

	resultData = store.GetAll([]byte("mockMigratorv1"))

	if diff := cmp.Diff(resultData, newData); diff != "" {
		t.Errorf("migration ups are different -got/+want\ndiff %s", diff)
	}
	// down run
	_ = mg.Migrate(ctx, false)
	resultData = store.GetAll([]byte("mockMigratorv1"))

	if diff := cmp.Diff(resultData, oldData); diff != "" {
		t.Errorf("migration ups are different -got/+want\ndiff %s", diff)
	}
}
