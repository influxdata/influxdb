// This file is a placeholder for an actual notebooks service implementation.
// For now it enables user experimentation with the UI in front of the notebooks
// backend server.

package service

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var _ NotebookService = (*FakeStore)(nil)

type FakeStore struct {
	list map[string][]*Notebook
}

func NewFakeStore() (*FakeStore, error) {
	return &FakeStore{
		list: make(map[string][]*Notebook),
	}, nil
}

func (s *FakeStore) GetNotebook(ctx context.Context, id platform.ID) (*Notebook, error) {
	ns := []*Notebook{}

	for _, nList := range s.list {
		ns = append(ns, nList...)
	}

	for _, n := range ns {
		if n.ID == id {
			return n, nil
		}
	}

	return nil, ErrNotebookNotFound
}

func (s *FakeStore) ListNotebooks(ctx context.Context, filter NotebookListFilter) ([]*Notebook, error) {
	o := filter.OrgID

	ns, ok := s.list[o.String()]
	if !ok {
		return []*Notebook{}, nil
	}

	return ns, nil
}

func (s *FakeStore) CreateNotebook(ctx context.Context, create *NotebookReqBody) (*Notebook, error) {
	n := &Notebook{
		OrgID:     create.OrgID,
		Name:      create.Name,
		Spec:      create.Spec,
		ID:        snowflake.NewDefaultIDGenerator().ID(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	idStr := create.OrgID.String()
	c := s.list[idStr]

	ns := append(c, n)
	s.list[idStr] = ns

	return n, nil
}

func (s *FakeStore) DeleteNotebook(ctx context.Context, id platform.ID) error {
	var foundOrg string
	for org, nList := range s.list {
		for _, b := range nList {
			if b.ID == id {
				foundOrg = org
			}
		}
	}

	if foundOrg == "" {
		return ErrNotebookNotFound
	}

	newNs := []*Notebook{}

	for _, b := range s.list[foundOrg] {
		if b.ID != id {
			newNs = append(newNs, b)
		}
	}

	s.list[foundOrg] = newNs
	return nil
}

func (s *FakeStore) UpdateNotebook(ctx context.Context, id platform.ID, update *NotebookReqBody) (*Notebook, error) {
	n, err := s.GetNotebook(ctx, id)
	if err != nil {
		return nil, err
	}

	if update.Name != "" {
		n.Name = update.Name
	}

	if len(update.Spec) > 0 {
		n.Spec = update.Spec
	}

	return n, nil
}
