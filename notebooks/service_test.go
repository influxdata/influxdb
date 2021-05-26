package notebooks

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/sqlite/migrations"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	idGen = snowflake.NewIDGenerator()
)

func TestCreateAndGetNotebook(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)
	ctx := context.Background()

	// getting an invalid id should return an error
	got, err := svc.GetNotebook(ctx, idGen.ID())
	require.Nil(t, got)
	require.ErrorIs(t, influxdb.ErrNotebookNotFound, err)

	testCreate := &influxdb.NotebookReqBody{
		OrgID: idGen.ID(),
		Name:  "some name",
		Spec:  map[string]interface{}{"hello": "goodbye"},
	}

	// create a notebook and assert the results
	gotCreate, err := svc.CreateNotebook(ctx, testCreate)
	require.NoError(t, err)
	gotCreateBody := &influxdb.NotebookReqBody{
		OrgID: gotCreate.OrgID,
		Name:  gotCreate.Name,
		Spec:  gotCreate.Spec,
	}
	require.Equal(t, testCreate, gotCreateBody)

	// get the notebook with the ID that was created and assert the results
	gotGet, err := svc.GetNotebook(ctx, gotCreate.ID)
	require.NoError(t, err)
	gotGetBody := &influxdb.NotebookReqBody{
		OrgID: gotGet.OrgID,
		Name:  gotGet.Name,
		Spec:  gotGet.Spec,
	}
	require.Equal(t, testCreate, gotGetBody)
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)
	ctx := context.Background()

	testCreate := &influxdb.NotebookReqBody{
		OrgID: idGen.ID(),
		Name:  "some name",
		Spec:  map[string]interface{}{"hello": "goodbye"},
	}

	testUpdate := &influxdb.NotebookReqBody{
		OrgID: testCreate.OrgID,
		Name:  "a new name",
		Spec:  map[string]interface{}{"aloha": "aloha"},
	}

	// attempting to update a non-existant notebook should return an error
	got, err := svc.UpdateNotebook(ctx, idGen.ID(), testUpdate)
	require.Nil(t, got)
	require.ErrorIs(t, influxdb.ErrNotebookNotFound, err)

	// create the notebook so updating it can be tested
	gotCreate, err := svc.CreateNotebook(ctx, testCreate)
	require.NoError(t, err)
	gotCreateBody := &influxdb.NotebookReqBody{
		OrgID: gotCreate.OrgID,
		Name:  gotCreate.Name,
		Spec:  gotCreate.Spec,
	}
	require.Equal(t, testCreate, gotCreateBody)

	// try to update the notebook and assert the results
	gotUpdate, err := svc.UpdateNotebook(ctx, gotCreate.ID, testUpdate)
	require.NoError(t, err)
	gotUpdateBody := &influxdb.NotebookReqBody{
		OrgID: gotUpdate.OrgID,
		Name:  gotUpdate.Name,
		Spec:  gotUpdate.Spec,
	}

	require.Equal(t, testUpdate, gotUpdateBody)
	require.Equal(t, gotCreate.ID, gotUpdate.ID)
	require.Equal(t, gotCreate.CreatedAt, gotUpdate.CreatedAt)
	require.NotEqual(t, gotUpdate.CreatedAt, gotUpdate.UpdatedAt)
}

func TestDelete(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)
	ctx := context.Background()

	// attempting to delete a non-existant notebook should return an error
	err := svc.DeleteNotebook(ctx, idGen.ID())
	fmt.Println(err)
	require.ErrorIs(t, influxdb.ErrNotebookNotFound, err)

	testCreate := &influxdb.NotebookReqBody{
		OrgID: idGen.ID(),
		Name:  "some name",
		Spec:  map[string]interface{}{"hello": "goodbye"},
	}

	// create the notebook that we are going to try to delete
	gotCreate, err := svc.CreateNotebook(ctx, testCreate)
	require.NoError(t, err)
	gotCreateBody := &influxdb.NotebookReqBody{
		OrgID: gotCreate.OrgID,
		Name:  gotCreate.Name,
		Spec:  gotCreate.Spec,
	}
	require.Equal(t, testCreate, gotCreateBody)

	// should be able to successfully delete the notebook now
	err = svc.DeleteNotebook(ctx, gotCreate.ID)
	require.NoError(t, err)

	// ensure the notebook no longer exists
	_, err = svc.GetNotebook(ctx, gotCreate.ID)
	require.ErrorIs(t, influxdb.ErrNotebookNotFound, err)
}

func TestList(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)
	ctx := context.Background()

	orgID := idGen.ID()

	// selecting with no matches for org_id should return an empty list and no error
	got, err := svc.ListNotebooks(ctx, influxdb.NotebookListFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Equal(t, 0, len(got))

	// create some notebooks to test the list operation with
	creates := []*influxdb.NotebookReqBody{
		{
			OrgID: orgID,
			Name:  "some name",
			Spec:  map[string]interface{}{"hello": "goodbye"},
		},
		{
			OrgID: orgID,
			Name:  "another name",
			Spec:  map[string]interface{}{"aloha": "aloha"},
		},
		{
			OrgID: orgID,
			Name:  "some name",
			Spec:  map[string]interface{}{"hola": "adios"},
		},
	}

	for _, c := range creates {
		_, err := svc.CreateNotebook(ctx, c)
		require.NoError(t, err)
	}

	// there should now be notebooks returned from ListNotebooks
	got, err = svc.ListNotebooks(ctx, influxdb.NotebookListFilter{OrgID: orgID})
	require.NoError(t, err)
	require.Equal(t, len(creates), len(got))

	// make sure the elements from the returned list were from the list of notebooks to create
	for _, n := range got {
		require.Contains(t, creates, &influxdb.NotebookReqBody{
			OrgID: n.OrgID,
			Name:  n.Name,
			Spec:  n.Spec,
		})
	}
}

func newTestService(t *testing.T) (*Service, func(t *testing.T)) {
	store, clean := sqlite.NewTestStore(t)
	ctx := context.Background()

	sqliteMigrator := sqlite.NewMigrator(store, zap.NewNop())
	err := sqliteMigrator.Up(ctx, &migrations.All{})
	require.NoError(t, err)

	svc := NewService(zap.NewNop(), store)

	return svc, clean
}
