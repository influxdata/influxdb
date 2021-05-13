package service

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/snowflake"
)

type DemoStore struct {
	list map[string][]*Notebook
}

func NewDemoStore() (*DemoStore, error) {
	return &DemoStore{}, nil
}

func (s *DemoStore) GetNotebook(ctx context.Context, orgID platform.ID, id platform.ID) (*Notebook, error) {
	ns, ok := s.list[orgID.String()]
	if !ok {
		return nil, nil
	}

	for _, n := range ns {
		if n.ID == id {
			return n, nil
		}
	}

	return nil, nil
}

func (s *DemoStore) GetNotebooks(ctx context.Context, filter NotebookListFilter) ([]*Notebook, error) {
	o := filter.OrgID

	ns, ok := s.list[o.String()]
	if !ok {
		return nil, nil
	}

	return ns, nil
}

func (s *DemoStore) CreateNotebook(ctx context.Context, create NotebookCreate) (*Notebook, error) {
	n := &Notebook{
		OrgID:     create.OrgID,
		Name:      create.Name,
		Spec:      create.Spec,
		ID:        snowflake.NewDefaultIDGenerator().ID(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	return n, nil
}

func (s *DemoStore) DeleteNotebook(ctx context.Context, orgID platform.ID, id platform.ID, update NotebookUpdate) error {
	ns, err := s.GetNotebooks(ctx, NotebookListFilter{OrgID: orgID})
	if err != nil {
		return err
	}

	newNs := []*Notebook{}

	for _, n := range ns {
		if n.ID != id {
			newNs = append(newNs, n)
		}
	}

	s.list[orgID.String()] = newNs

	return nil
}

func (s *DemoStore) UpdateNotebook(ctx context.Context, orgID platform.ID, id platform.ID, update NotebookUpdate) (*Notebook, error) {
	n, err := s.GetNotebook(ctx, orgID, id)
	if err != nil {
		return nil, nil
	}

	if update.Name != "" {
		n.Name = update.Name
	}

	if len(update.Spec) > 0 {
		n.Spec = update.Spec
	}

	return n, nil
}
