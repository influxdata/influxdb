package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/influx"
)

type annotationLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type annotationResponse struct {
	chronograf.Annotation
	Links annotationLinks `json:"links"`
}

func newAnnotationResponse(src chronograf.Source, a chronograf.Annotation) annotationResponse {
	base := "/chronograf/v1/sources"
	return annotationResponse{
		Annotation: a,
		Links: annotationLinks{
			Self: fmt.Sprintf("%s/%d/annotations/%s_%s_%d", base, src.ID, a.Type, a.Name, a.Duration),
		},
	}
}

type annotationsResponse struct {
	Annotations []annotationResponse `json:"annotations"`
}

func newAnnotationsResponse(src chronograf.Source, as []chronograf.Annotation) annotationsResponse {
	annotations := make([]annotationResponse, len(as))
	for i, a := range as {
		annotations[i] = newAnnotationResponse(src, a)
	}
	return annotationsResponse{
		Annotations: annotations,
	}
}

// ValidAnnotation check if the annotation contains all required data
func ValidAnnotation(a *chronograf.Annotation) error {
	if a.Name == "" {
		return fmt.Errorf("annotation requires name")
	}

	if a.Type == "" {
		return fmt.Errorf("annotation requires type")
	}
	return nil
}

// Annotations returns all annotations within the annotations store
func (s *Service) Annotations(w http.ResponseWriter, r *http.Request) {
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

	store := influx.NewAnnotationStore(ts)
	annotations, err := store.All(ctx)
	if err != nil {
		msg := fmt.Errorf("Error loading annotations: %v", err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	res := newAnnotationsResponse(src, annotations)
	encodeJSON(w, http.StatusOK, res, s.Logger)
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

	var req chronograf.Annotation
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err = ValidAnnotation(&req); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	store := influx.NewAnnotationStore(ts)
	anno, err := store.Add(ctx, req)
	if err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for  response"
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

	var req chronograf.Annotation
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err = ValidAnnotation(&req); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	store := influx.NewAnnotationStore(ts)
	if err = store.Delete(ctx, req); err != nil {
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

// UpdateAnnotation overwrite an existing annotation
func (s *Service) UpdateAnnotation(w http.ResponseWriter, r *http.Request) {
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

	var req chronograf.Annotation
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err = ValidAnnotation(&req); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	store := influx.NewAnnotationStore(ts)
	if err = store.Update(ctx, req); err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for  response"
			Error(w, http.StatusRequestTimeout, msg, s.Logger)
			return
		}
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newAnnotationResponse(src, req)
	location(w, res.Links.Self)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}
