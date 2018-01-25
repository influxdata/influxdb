package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/influx"
)

const (
	since           = "since"
	until           = "until"
	timeMilliFormat = "2006-01-02T15:04:05.999Z07:00"
)

type annotationLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type annotationResponse struct {
	ID        string          `json:"id"`        // ID is the unique annotation identifier
	StartTime string          `json:"startTime"` // StartTime in RFC3339 of the start of the annotation
	EndTime   string          `json:"endTime"`   // EndTime in RFC3339 of the end of the annotation
	Text      string          `json:"text"`      // Text is the associated user-facing text describing the annotation
	Type      string          `json:"type"`      // Type describes the kind of annotation
	Links     annotationLinks `json:"links"`
}

func newAnnotationResponse(src chronograf.Source, a *chronograf.Annotation) annotationResponse {
	base := "/chronograf/v1/sources"
	return annotationResponse{
		ID:        a.ID,
		StartTime: a.StartTime.UTC().Format(timeMilliFormat),
		EndTime:   a.EndTime.UTC().Format(timeMilliFormat),
		Text:      a.Text,
		Type:      a.Type,
		Links: annotationLinks{
			Self: fmt.Sprintf("%s/%d/annotations/%s", base, src.ID, a.ID),
		},
	}
}

type annotationsResponse struct {
	Annotations []annotationResponse `json:"annotations"`
}

func newAnnotationsResponse(src chronograf.Source, as []chronograf.Annotation) annotationsResponse {
	annotations := make([]annotationResponse, len(as))
	for i, a := range as {
		annotations[i] = newAnnotationResponse(src, &a)
	}
	return annotationsResponse{
		Annotations: annotations,
	}
}

// TODO: check that start time is before stop time
func validAnnotationQuery(query url.Values) (startTime, stopTime time.Time, err error) {
	start := query.Get(since)
	if start == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("since parameter is required")
	}

	startTime, err = time.Parse(timeMilliFormat, start)
	if err != nil {
		return
	}

	// if until isn't stated, the default time is now
	stop := query.Get(until)
	if stop != "" {
		stopTime, err := time.Parse(timeMilliFormat, stop)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		return startTime, stopTime, nil
	}
	return startTime, time.Now().UTC(), nil
}

// Annotations returns all annotations within the annotations store
func (s *Service) Annotations(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	start, stop, err := validAnnotationQuery(r.URL.Query())
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	src, err := s.Store.Sources(ctx).Get(ctx, id)
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	ts, err := s.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	store := influx.NewAnnotationStore(ts)
	annotations, err := store.All(ctx, start, stop)
	if err != nil {
		msg := fmt.Errorf("Error loading annotations: %v", err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	res := newAnnotationsResponse(src, annotations)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// Annotation returns a specified annotation id within the annotations store
func (s *Service) Annotation(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}
	annoID, err := paramStr("aid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	src, err := s.Store.Sources(ctx).Get(ctx, id)
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	ts, err := s.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	store := influx.NewAnnotationStore(ts)
	anno, err := store.Get(ctx, annoID)
	if err != nil {
		if err != chronograf.ErrAnnotationNotFound {
			msg := fmt.Errorf("Error loading annotation: %v", err)
			unknownErrorWithMessage(w, msg, s.Logger)
			return
		}
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newAnnotationResponse(src, anno)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

type newAnnotationRequest struct {
	StartTime time.Time `json:"startTime"`      // StartTime is the time in rfc3339 milliseconds
	EndTime   time.Time `json:"endTime"`        // EndTime is the time in rfc3339 milliseconds
	Text      string    `json:"text,omitempty"` // Text is the associated user-facing text describing the annotation
	Type      string    `json:"type,omitempty"` // Type describes the kind of annotation
}

// TODO: check that the endtime is after the starttime
func (ar *newAnnotationRequest) UnmarshalJSON(data []byte) error {
	type Alias newAnnotationRequest
	aux := &struct {
		StartTime string `json:"startTime"`
		EndTime   string `json:"endTime"`
		*Alias
	}{
		Alias: (*Alias)(ar),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	ar.StartTime, err = time.Parse(timeMilliFormat, aux.StartTime)
	if err != nil {
		return err
	}

	ar.EndTime, err = time.Parse(timeMilliFormat, aux.EndTime)
	if err != nil {
		return err
	}

	return nil
}

func (ar *newAnnotationRequest) Annotation() *chronograf.Annotation {
	return &chronograf.Annotation{
		StartTime: ar.StartTime,
		EndTime:   ar.EndTime,
		Text:      ar.Text,
		Type:      ar.Type,
	}
}

// NewAnnotation adds the annotation from a POST body to the annotations store
func (s *Service) NewAnnotation(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	src, err := s.Store.Sources(ctx).Get(ctx, id)
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	ts, err := s.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	var req newAnnotationRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	store := influx.NewAnnotationStore(ts)
	anno, err := store.Add(ctx, req.Annotation())
	if err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for response"
			Error(w, http.StatusRequestTimeout, msg, s.Logger)
			return
		}
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newAnnotationResponse(src, anno)
	location(w, res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, s.Logger)
}

// RemoveAnnotation removes the annotation from the time series source
func (s *Service) RemoveAnnotation(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	annoID, err := paramStr("aid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	src, err := s.Store.Sources(ctx).Get(ctx, id)
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	ts, err := s.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	store := influx.NewAnnotationStore(ts)
	if err = store.Delete(ctx, annoID); err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for  response"
			Error(w, http.StatusRequestTimeout, msg, s.Logger)
			return
		}
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type updateAnnotationRequest struct {
	StartTime *time.Time `json:"startTime,omitempty"` // StartTime is the time in rfc3339 milliseconds
	EndTime   *time.Time `json:"endTime,omitempty"`   // EndTime is the time in rfc3339 milliseconds
	Text      *string    `json:"text,omitempty"`      // Text is the associated user-facing text describing the annotation
	Type      *string    `json:"type,omitempty"`      // Type describes the kind of annotation
}

// TODO: make sure that endtime is after starttime
func (u *updateAnnotationRequest) UnmarshalJSON(data []byte) error {
	type Alias updateAnnotationRequest
	aux := &struct {
		StartTime *string `json:"startTime,omitempty"`
		EndTime   *string `json:"endTime,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(u),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.StartTime != nil {
		tm, err := time.Parse(timeMilliFormat, *aux.StartTime)
		if err != nil {
			return err
		}
		u.StartTime = &tm
	}

	if aux.EndTime != nil {
		tm, err := time.Parse(timeMilliFormat, *aux.EndTime)
		if err != nil {
			return err
		}
		u.EndTime = &tm
	}

	// Update must have at least one field set
	if u.StartTime == nil && u.EndTime == nil && u.Text == nil && u.Type == nil {
		return fmt.Errorf("update request must have at least one field")
	}

	return nil
}

// UpdateAnnotation overwrite an existing annotation
func (s *Service) UpdateAnnotation(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	annoID, err := paramStr("aid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	src, err := s.Store.Sources(ctx).Get(ctx, id)
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	ts, err := s.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	store := influx.NewAnnotationStore(ts)
	cur, err := store.Get(ctx, annoID)
	if err != nil {
		notFound(w, annoID, s.Logger)
		return
	}

	var req updateAnnotationRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if req.StartTime != nil {
		cur.StartTime = *req.StartTime
	}
	if req.EndTime != nil {
		cur.EndTime = *req.EndTime
	}
	if req.Text != nil {
		cur.Text = *req.Text
	}
	if req.Type != nil {
		cur.Type = *req.Type
	}

	if err = store.Update(ctx, cur); err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for response"
			Error(w, http.StatusRequestTimeout, msg, s.Logger)
			return
		}
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newAnnotationResponse(src, cur)
	location(w, res.Links.Self)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}
