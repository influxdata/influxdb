package platform

import (
	"context"
	"fmt"
)

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ErrSourceNotFound = Error("source not found")
)

// SourceType is a string for types of sources.
type SourceType string

const (
	V2SourceType   = "v2"
	V1SourceType   = "v1"
	SelfSourceType = "self"
)

// Source is an external Influx with time series data.
// TODO(desa): do we still need default?
// TODO(desa): do sources belong
type Source struct {
	ID                 ID         `json:"id,string"`                    // ID is the unique ID of the source
	OrganizationID     ID         `json:"organizationID"`               // OrganizationID is the organization ID that resource belongs to
	Default            bool       `json:"default"`                      // Default specifies the default source for the application
	Name               string     `json:"name"`                         // Name is the user-defined name for the source
	Type               SourceType `json:"type,omitempty"`               // Type specifies which kinds of source (enterprise vs oss vs 2.0)
	URL                string     `json:"url"`                          // URL are the connections to the source
	InsecureSkipVerify bool       `json:"insecureSkipVerify,omitempty"` // InsecureSkipVerify as true means any certificate presented by the source is accepted
	Telegraf           string     `json:"telegraf"`                     // Telegraf is the db telegraf is written to.  By default it is "telegraf"
	SourceFields
	V1SourceFields

	BucketService BucketService `json:"-"`
	// TODO(desa): is this a good idea?
	SourceQuerier SourceQuerier `json:"-"`
}

// V1SourceFields are the fields for connecting to a 1.0 source (oss or enterprise)
type V1SourceFields struct {
	Username     string `json:"username,omitempty"`     // Username is the username to connect to the source
	Password     string `json:"password,omitempty"`     // Password is in CLEARTEXT
	SharedSecret string `json:"sharedSecret,omitempty"` // ShareSecret is the optional signing secret for Influx JWT authorization
	MetaURL      string `json:"metaUrl,omitempty"`      // MetaURL is the url for the meta node
	DefaultRP    string `json:"defaultRP"`              // DefaultRP is the default retention policy used in database queries to this source
	FluxURL      string `json:"fluxURL,omitempty"`      // FluxURL is the url for a flux connected to a 1x source
}

// SourceFields
type SourceFields struct {
	Token string `json:"token"` // Token is the 2.0 authorization token associated with a source
}

// SourceService is a service for managing sources.
type SourceService interface {
	// DefaultSource retrieves the default source.
	DefaultSource(ctx context.Context) (*Source, error)
	// FindSourceByID retrieves a source by its ID.
	FindSourceByID(ctx context.Context, id ID) (*Source, error)
	// FindSources returns a list of all sources.
	FindSources(ctx context.Context, opts FindOptions) ([]*Source, int, error)
	// CreateSource sets the sources ID and stores it.
	CreateSource(ctx context.Context, s *Source) error
	// UpdateSource updates the source.
	UpdateSource(ctx context.Context, id ID, upd SourceUpdate) (*Source, error)
	// DeleteSource removes the source.
	DeleteSource(ctx context.Context, id ID) error
}

// SourceUpdate represents updates to a source.
type SourceUpdate struct {
	Name               *string     `json:"name"`
	Type               *SourceType `json:"type,omitempty"`
	Token              *string     `json:"token"`
	URL                *string     `json:"url"`
	InsecureSkipVerify *bool       `json:"insecureSkipVerify,omitempty"`
	Telegraf           *string     `json:"telegraf"`
	Username           *string     `json:"username,omitempty"`
	Password           *string     `json:"password,omitempty"`
	SharedSecret       *string     `json:"sharedSecret,omitempty"`
	MetaURL            *string     `json:"metaURL,omitempty"`
	FluxURL            *string     `json:"fluxURL,omitempty"`
	Role               *string     `json:"role,omitempty"`
	DefaultRP          *string     `json:"defaultRP"`
}

// Apply applies an update to a source.
func (u SourceUpdate) Apply(s *Source) error {
	if u.Name != nil {
		s.Name = *u.Name
	}
	if u.Type != nil {
		s.Type = *u.Type
	}
	if u.Token != nil {
		s.Token = *u.Token
	}
	if u.URL != nil {
		s.URL = *u.URL
	}
	if u.InsecureSkipVerify != nil {
		s.InsecureSkipVerify = *u.InsecureSkipVerify
	}
	if u.Telegraf != nil {
		s.Telegraf = *u.Telegraf
	}
	if u.Username != nil {
		s.Username = *u.Username
	}
	if u.Password != nil {
		s.Password = *u.Password
	}
	if u.SharedSecret != nil {
		s.SharedSecret = *u.SharedSecret
	}
	if u.MetaURL != nil {
		s.MetaURL = *u.MetaURL
	}
	if u.FluxURL != nil {
		s.FluxURL = *u.FluxURL
	}
	if u.DefaultRP != nil {
		s.DefaultRP = *u.DefaultRP
	}

	return nil
}
func (s *Source) FindBucketByID(ctx context.Context, id ID) (*Bucket, error) {
	if s.BucketService == nil {
		return nil, fmt.Errorf("not supported")
	}
	return s.BucketService.FindBucketByID(ctx, id)
}

func (s *Source) FindBucket(ctx context.Context, filter BucketFilter) (*Bucket, error) {
	if s.BucketService == nil {
		return nil, fmt.Errorf("not supported")
	}
	return s.BucketService.FindBucket(ctx, filter)
}

func (s *Source) FindBuckets(ctx context.Context, filter BucketFilter, opt ...FindOptions) ([]*Bucket, int, error) {
	if s.BucketService == nil {
		return nil, 0, fmt.Errorf("not supported")
	}
	return s.BucketService.FindBuckets(ctx, filter, opt...)
}

func (s *Source) CreateBucket(ctx context.Context, b *Bucket) error {
	if s.BucketService == nil {
		return fmt.Errorf("not supported")
	}
	return s.BucketService.CreateBucket(ctx, b)
}

func (s *Source) UpdateBucket(ctx context.Context, id ID, upd BucketUpdate) (*Bucket, error) {
	if s.BucketService == nil {
		return nil, fmt.Errorf("not supported")
	}
	return s.BucketService.UpdateBucket(ctx, id, upd)
}

func (s *Source) DeleteBucket(ctx context.Context, id ID) error {
	if s.BucketService == nil {
		return fmt.Errorf("not supported")
	}
	return s.BucketService.DeleteBucket(ctx, id)
}

func (s *Source) Query(ctx context.Context, q *SourceQuery) (*SourceQueryResult, error) {
	if s.SourceQuerier == nil {
		return nil, fmt.Errorf("not supported")
	}
	return s.SourceQuerier.Query(ctx, q)
}
