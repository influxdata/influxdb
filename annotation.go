package influxdb

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	errEmptySummary = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "summary cannot be empty",
	}
	errSummaryTooLong = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "summary must be less than 255 characters",
	}
	errStreamTagTooLong = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "stream tag must be less than 255 characters",
	}
	errStreamNameTooLong = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "stream name must be less than 255 characters",
	}
	errStreamDescTooLong = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "stream description must be less than 1024 characters",
	}
	errStickerTooLong = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "stickers must be less than 255 characters",
	}
	errMsgTooLong = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "message must be less than 4096 characters",
	}
	errReversedTimes = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "start time must come before end time",
	}
	errMissingStreamName = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "stream name must be set",
	}
	errMissingStreamTagOrId = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "stream tag or id must be set",
	}
	errMissingEndTime = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "end time must be set",
	}
	errMissingStartTime = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "start time must be set",
	}
)

func invalidStickerError(s string) error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("invalid sticker: %q", s),
	}
}

func stickerSliceToMap(stickers []string) (map[string]string, error) {
	stickerMap := map[string]string{}

	for i := range stickers {
		if stick0, stick1, found := strings.Cut(stickers[i], "="); found {
			stickerMap[stick0] = stick1
		} else {
			return nil, invalidStickerError(stickers[i])
		}
	}

	return stickerMap, nil
}

// AnnotationService is the service contract for Annotations
type AnnotationService interface {
	// CreateAnnotations creates annotations.
	CreateAnnotations(ctx context.Context, orgID platform.ID, create []AnnotationCreate) ([]AnnotationEvent, error)
	// ListAnnotations lists all annotations matching the filter.
	ListAnnotations(ctx context.Context, orgID platform.ID, filter AnnotationListFilter) ([]StoredAnnotation, error)
	// GetAnnotation gets an annotation by id.
	GetAnnotation(ctx context.Context, id platform.ID) (*StoredAnnotation, error)
	// DeleteAnnotations deletes annotations matching the filter.
	DeleteAnnotations(ctx context.Context, orgID platform.ID, delete AnnotationDeleteFilter) error
	// DeleteAnnotation deletes an annotation by id.
	DeleteAnnotation(ctx context.Context, id platform.ID) error
	// UpdateAnnotation updates an annotation.
	UpdateAnnotation(ctx context.Context, id platform.ID, update AnnotationCreate) (*AnnotationEvent, error)

	// ListStreams lists all streams matching the filter.
	ListStreams(ctx context.Context, orgID platform.ID, filter StreamListFilter) ([]StoredStream, error)
	// CreateOrUpdateStream creates or updates the matching stream by name.
	CreateOrUpdateStream(ctx context.Context, orgID platform.ID, stream Stream) (*ReadStream, error)
	// GetStream gets a stream by id. Currently this is only used for authorization, and there are no
	// API routes for getting a single stream by ID.
	GetStream(ctx context.Context, id platform.ID) (*StoredStream, error)
	// UpdateStream updates the stream by the ID.
	UpdateStream(ctx context.Context, id platform.ID, stream Stream) (*ReadStream, error)
	// DeleteStreams deletes one or more streams by name.
	DeleteStreams(ctx context.Context, orgID platform.ID, delete BasicStream) error
	// DeleteStreamByID deletes the stream metadata by id.
	DeleteStreamByID(ctx context.Context, id platform.ID) error
}

// AnnotationEvent contains fields for annotating an event.
type AnnotationEvent struct {
	ID               platform.ID `json:"id,omitempty"` // ID is the annotation ID.
	AnnotationCreate             // AnnotationCreate defines the common input/output bits of an annotation.
}

// AnnotationCreate contains user providable fields for annotating an event.
type AnnotationCreate struct {
	StreamTag string             `json:"stream,omitempty"`    // StreamTag provides a means to logically group a set of annotated events.
	Summary   string             `json:"summary"`             // Summary is the only field required to annotate an event.
	Message   string             `json:"message,omitempty"`   // Message provides more details about the event being annotated.
	Stickers  AnnotationStickers `json:"stickers,omitempty"`  // Stickers are like tags, but named something obscure to differentiate them from influx tags. They are there to differentiate an annotated event.
	EndTime   *time.Time         `json:"endTime,omitempty"`   // EndTime is the time of the event being annotated. Defaults to now if not set.
	StartTime *time.Time         `json:"startTime,omitempty"` // StartTime is the start time of the event being annotated. Defaults to EndTime if not set.
}

// StoredAnnotation represents annotation data to be stored in the database.
type StoredAnnotation struct {
	ID        platform.ID        `db:"id"`        // ID is the annotation's id.
	OrgID     platform.ID        `db:"org_id"`    // OrgID is the annotations's owning organization.
	StreamID  platform.ID        `db:"stream_id"` // StreamID is the id of a stream.
	StreamTag string             `db:"stream"`    // StreamTag is the name of a stream (when selecting with join of streams).
	Summary   string             `db:"summary"`   // Summary is the summary of the annotated event.
	Message   string             `db:"message"`   // Message is a longer description of the annotated event.
	Stickers  AnnotationStickers `db:"stickers"`  // Stickers are additional labels to group annotations by.
	Duration  string             `db:"duration"`  // Duration is the time range (with zone) of an annotated event.
	Lower     string             `db:"lower"`     // Lower is the time an annotated event begins.
	Upper     string             `db:"upper"`     // Upper is the time an annotated event ends.
}

// ToCreate is a utility method for converting a StoredAnnotation to an AnnotationCreate type
func (s StoredAnnotation) ToCreate() (*AnnotationCreate, error) {
	et, err := time.Parse(time.RFC3339Nano, s.Upper)
	if err != nil {
		return nil, err
	}

	st, err := time.Parse(time.RFC3339Nano, s.Lower)
	if err != nil {
		return nil, err
	}

	return &AnnotationCreate{
		StreamTag: s.StreamTag,
		Summary:   s.Summary,
		Message:   s.Message,
		Stickers:  s.Stickers,
		EndTime:   &et,
		StartTime: &st,
	}, nil
}

// ToEvent is a utility method for converting a StoredAnnotation to an AnnotationEvent type
func (s StoredAnnotation) ToEvent() (*AnnotationEvent, error) {
	c, err := s.ToCreate()
	if err != nil {
		return nil, err
	}

	return &AnnotationEvent{
		ID:               s.ID,
		AnnotationCreate: *c,
	}, nil
}

type AnnotationStickers map[string]string

// Value implements the database/sql Valuer interface for adding AnnotationStickers to the database
// Stickers are stored in the database as a slice of strings like "[key=val]"
// They are encoded into a JSON string for storing into the database, and the JSON sqlite extension is
// able to manipulate them like an object.
func (a AnnotationStickers) Value() (driver.Value, error) {
	stickSlice := make([]string, 0, len(a))

	for k, v := range a {
		stickSlice = append(stickSlice, fmt.Sprintf("%s=%s", k, v))
	}

	sticks, err := json.Marshal(stickSlice)
	if err != nil {
		return nil, err
	}

	return string(sticks), nil
}

// Scan implements the database/sql Scanner interface for retrieving AnnotationStickers from the database
// The string is decoded into a slice of strings, which are then converted back into a map
func (a *AnnotationStickers) Scan(value interface{}) error {
	vString, ok := value.(string)
	if !ok {
		return &errors.Error{
			Code: errors.EInternal,
			Msg:  "could not load stickers from sqlite",
		}
	}

	var stickSlice []string
	if err := json.NewDecoder(strings.NewReader(vString)).Decode(&stickSlice); err != nil {
		return err
	}

	stickMap, err := stickerSliceToMap(stickSlice)
	if err != nil {
		return nil
	}

	*a = stickMap
	return nil
}

// Validate validates the creation object.
func (a *AnnotationCreate) Validate(nowFunc func() time.Time) error {
	switch s := utf8.RuneCountInString(a.Summary); {
	case s <= 0:
		return errEmptySummary
	case s > 255:
		return errSummaryTooLong
	}

	switch t := utf8.RuneCountInString(a.StreamTag); {
	case t == 0:
		a.StreamTag = "default"
	case t > 255:
		return errStreamTagTooLong
	}

	if utf8.RuneCountInString(a.Message) > 4096 {
		return errMsgTooLong
	}

	for k, v := range a.Stickers {
		if utf8.RuneCountInString(k) > 255 || utf8.RuneCountInString(v) > 255 {
			return errStickerTooLong
		}
	}

	now := nowFunc()
	if a.EndTime == nil {
		a.EndTime = &now
	}

	if a.StartTime == nil {
		a.StartTime = a.EndTime
	}

	if a.EndTime.Before(*(a.StartTime)) {
		return errReversedTimes
	}

	return nil
}

// AnnotationDeleteFilter contains fields for deleting an annotated event.
type AnnotationDeleteFilter struct {
	StreamTag string            `json:"stream,omitempty"`    // StreamTag provides a means to logically group a set of annotated events.
	StreamID  platform.ID       `json:"streamID,omitempty"`  // StreamID provides a means to logically group a set of annotated events.
	Stickers  map[string]string `json:"stickers,omitempty"`  // Stickers are like tags, but named something obscure to differentiate them from influx tags. They are there to differentiate an annotated event.
	EndTime   *time.Time        `json:"endTime,omitempty"`   // EndTime is the time of the event being annotated. Defaults to now if not set.
	StartTime *time.Time        `json:"startTime,omitempty"` // StartTime is the start time of the event being annotated. Defaults to EndTime if not set.
}

// Validate validates the deletion object.
func (a *AnnotationDeleteFilter) Validate() error {
	var errs []string

	if len(a.StreamTag) == 0 && !a.StreamID.Valid() {
		errs = append(errs, errMissingStreamTagOrId.Error())
	}

	if a.EndTime == nil {
		errs = append(errs, errMissingEndTime.Error())
	}

	if a.StartTime == nil {
		errs = append(errs, errMissingStartTime.Error())
	}

	if len(errs) > 0 {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  strings.Join(errs, "; "),
		}
	}

	if a.EndTime.Before(*(a.StartTime)) {
		return errReversedTimes
	}

	return nil
}

var dre = regexp.MustCompile(`stickers\[(.*)\]`)

// SetStickers sets the stickers from the query parameters.
func (a *AnnotationDeleteFilter) SetStickers(vals map[string][]string) {
	if a.Stickers == nil {
		a.Stickers = map[string]string{}
	}

	for k, v := range vals {
		if ss := dre.FindStringSubmatch(k); len(ss) == 2 && len(v) > 0 {
			a.Stickers[ss[1]] = v[0]
		}
	}
}

// AnnotationList defines the structure of the response when listing annotations.
type AnnotationList struct {
	StreamTag   string           `json:"stream"`
	Annotations []ReadAnnotation `json:"annotations"`
}

// ReadAnnotations allows annotations to be assigned to a stream.
type ReadAnnotations map[string][]ReadAnnotation

// MarshalJSON allows us to marshal the annotations belonging to a stream properly.
func (s ReadAnnotations) MarshalJSON() ([]byte, error) {
	annotationList := []AnnotationList{}

	for k, v := range s {
		annotationList = append(annotationList, AnnotationList{
			StreamTag:   k,
			Annotations: v,
		})
	}

	return json.Marshal(annotationList)
}

// ReadAnnotation defines the simplest form of an annotation to be returned. Essentially, it's AnnotationEvent without stream info.
type ReadAnnotation struct {
	ID        platform.ID       `json:"id"`                  // ID is the annotation's generated id.
	Summary   string            `json:"summary"`             // Summary is the only field required to annotate an event.
	Message   string            `json:"message,omitempty"`   // Message provides more details about the event being annotated.
	Stickers  map[string]string `json:"stickers,omitempty"`  // Stickers are like tags, but named something obscure to differentiate them from influx tags. They are there to differentiate an annotated event.
	EndTime   string            `json:"endTime"`             // EndTime is the time of the event being annotated.
	StartTime string            `json:"startTime,omitempty"` // StartTime is the start time of the event being annotated.
}

// AnnotationListFilter is a selection filter for listing annotations.
type AnnotationListFilter struct {
	StickerIncludes AnnotationStickers `json:"stickerIncludes,omitempty"` // StickerIncludes allows the user to filter annotated events based on it's sticker.
	StreamIncludes  []string           `json:"streamIncludes,omitempty"`  // StreamIncludes allows the user to filter annotated events by stream.
	BasicFilter
}

// Validate validates the filter.
func (f *AnnotationListFilter) Validate(nowFunc func() time.Time) error {
	return f.BasicFilter.Validate(nowFunc)
}

var re = regexp.MustCompile(`stickerIncludes\[(.*)\]`)

// SetStickerIncludes sets the stickerIncludes from the query parameters.
func (f *AnnotationListFilter) SetStickerIncludes(vals map[string][]string) {
	if f.StickerIncludes == nil {
		f.StickerIncludes = map[string]string{}
	}

	for k, v := range vals {
		if ss := re.FindStringSubmatch(k); len(ss) == 2 && len(v) > 0 {
			f.StickerIncludes[ss[1]] = v[0]
		}
	}
}

// StreamListFilter is a selection filter for listing streams. Streams are not considered first class resources, but depend on an annotation using them.
type StreamListFilter struct {
	StreamIncludes []string `json:"streamIncludes,omitempty"` // StreamIncludes allows the user to filter streams returned.
	BasicFilter
}

// Validate validates the filter.
func (f *StreamListFilter) Validate(nowFunc func() time.Time) error {
	return f.BasicFilter.Validate(nowFunc)
}

// Stream defines the stream metadata. Used in create and update requests/responses. Delete requests will only require stream name.
type Stream struct {
	Name        string `json:"stream"`                // Name is the name of a stream.
	Description string `json:"description,omitempty"` // Description is more information about a stream.
}

// ReadStream defines the returned stream.
type ReadStream struct {
	ID          platform.ID `json:"id" db:"id"`                             // ID is the id of a stream.
	Name        string      `json:"stream" db:"name"`                       // Name is the name of a stream.
	Description string      `json:"description,omitempty" db:"description"` // Description is more information about a stream.
	CreatedAt   time.Time   `json:"createdAt" db:"created_at"`              // CreatedAt is a timestamp.
	UpdatedAt   time.Time   `json:"updatedAt" db:"updated_at"`              // UpdatedAt is a timestamp.
}

// IsValid validates the stream.
func (s *Stream) Validate(strict bool) error {
	switch nameChars := utf8.RuneCountInString(s.Name); {
	case nameChars <= 0:
		if strict {
			return errMissingStreamName
		}
		s.Name = "default"
	case nameChars > 255:
		return errStreamNameTooLong
	}

	if utf8.RuneCountInString(s.Description) > 1024 {
		return errStreamDescTooLong
	}

	return nil
}

// StoredStream represents stream data to be stored in the metadata database.
type StoredStream struct {
	ID          platform.ID `db:"id"`          // ID is the stream's id.
	OrgID       platform.ID `db:"org_id"`      // OrgID is the stream's owning organization.
	Name        string      `db:"name"`        // Name is the name of a stream.
	Description string      `db:"description"` // Description is more information about a stream.
	CreatedAt   time.Time   `db:"created_at"`  // CreatedAt is a timestamp.
	UpdatedAt   time.Time   `db:"updated_at"`  // UpdatedAt is a timestamp.
}

// BasicStream defines a stream by name. Used for stream deletes.
type BasicStream struct {
	Names []string `json:"stream"`
}

// IsValid validates the stream is not empty.
func (s BasicStream) IsValid() bool {
	if len(s.Names) <= 0 {
		return false
	}

	for i := range s.Names {
		if len(s.Names[i]) <= 0 {
			return false
		}
	}

	return true
}

// BasicFilter defines common filter options.
type BasicFilter struct {
	StartTime *time.Time `json:"startTime,omitempty"` // StartTime is the time the event being annotated started.
	EndTime   *time.Time `json:"endTime,omitempty"`   // EndTime is the time the event being annotated ended.
}

// Validate validates the basic filter options, setting sane defaults where appropriate.
func (f *BasicFilter) Validate(nowFunc func() time.Time) error {
	now := nowFunc().UTC().Truncate(time.Second)
	if f.EndTime == nil || f.EndTime.IsZero() {
		f.EndTime = &now
	}

	if f.StartTime == nil {
		f.StartTime = &time.Time{}
	}

	if f.EndTime.Before(*(f.StartTime)) {
		return errReversedTimes
	}

	return nil
}
