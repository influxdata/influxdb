package label

import (
	"context"
	"path"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

var _ influxdb.LabelService = (*LabelClientService)(nil)

type LabelClientService struct {
	Client *httpc.Client
}

func labelIDPath(id influxdb.ID) string {
	return path.Join(prefixLabels, id.String())
}

func resourceIDPath(resourceType influxdb.ResourceType, resourceID influxdb.ID, p string) string {
	return path.Join("/api/v2/", string(resourceType), resourceID.String(), p)
}

// CreateLabel creates a new label.
func (s *LabelClientService) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	var lr labelResponse
	err := s.Client.
		PostJSON(l, prefixLabels).
		DecodeJSON(&lr).
		Do(ctx)
	if err != nil {
		return err
	}

	*l = lr.Label
	return nil
}

// FindLabelByID returns a single label by ID.
func (s *LabelClientService) FindLabelByID(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
	var lr labelResponse
	err := s.Client.
		Get(labelIDPath(id)).
		DecodeJSON(&lr).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &lr.Label, nil
}

// FindLabels is a client for the find labels response from the server.
func (s *LabelClientService) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) ([]*influxdb.Label, error) {
	params := influxdb.FindOptionParams(opt...)
	if filter.OrgID != nil {
		params = append(params, [2]string{"orgID", filter.OrgID.String()})
	}
	if filter.Name != "" {
		params = append(params, [2]string{"name", filter.Name})
	}

	var lr labelsResponse
	err := s.Client.
		Get(prefixLabels).
		QueryParams(params...).
		DecodeJSON(&lr).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return lr.Labels, nil
}

// FindResourceLabels returns a list of labels, derived from a label mapping filter.
func (s *LabelClientService) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	if err := filter.Valid(); err != nil {
		return nil, err
	}

	var r labelsResponse
	err := s.Client.
		Get(resourceIDPath(filter.ResourceType, filter.ResourceID, "labels")).
		DecodeJSON(&r).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return r.Labels, nil
}

// UpdateLabel updates a label and returns the updated label.
func (s *LabelClientService) UpdateLabel(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	var lr labelResponse
	err := s.Client.
		PatchJSON(upd, labelIDPath(id)).
		DecodeJSON(&lr).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &lr.Label, nil
}

// DeleteLabel removes a label by ID.
func (s *LabelClientService) DeleteLabel(ctx context.Context, id influxdb.ID) error {
	return s.Client.
		Delete(labelIDPath(id)).
		Do(ctx)
}

// ******* Label Mappings ******* //

// CreateLabelMapping will create a labbel mapping
func (s *LabelClientService) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	urlPath := resourceIDPath(m.ResourceType, m.ResourceID, "labels")
	return s.Client.
		PostJSON(m, urlPath).
		DecodeJSON(m).
		Do(ctx)
}

func (s *LabelClientService) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	return s.Client.
		Delete(resourceIDPath(m.ResourceType, m.ResourceID, "labels")).
		Do(ctx)
}
