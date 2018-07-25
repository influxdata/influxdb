package server

import (
	"fmt"
	"net/http"

	"github.com/influxdata/platform/chronograf"
)

// Permissions returns all possible permissions for this source.
func (s *Service) Permissions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	src, err := s.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, s.Logger)
		return
	}

	ts, err := s.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	perms := ts.Permissions(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	httpAPISrcs := "/chronograf/v1/sources"
	res := struct {
		Permissions chronograf.Permissions `json:"permissions"`
		Links       map[string]string      `json:"links"` // Links are URI locations related to user
	}{
		Permissions: perms,
		Links: map[string]string{
			"self":   fmt.Sprintf("%s/%d/permissions", httpAPISrcs, srcID),
			"source": fmt.Sprintf("%s/%d", httpAPISrcs, srcID),
		},
	}
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

func validPermissions(perms *chronograf.Permissions) error {
	if perms == nil {
		return nil
	}
	for _, perm := range *perms {
		if perm.Scope != chronograf.AllScope && perm.Scope != chronograf.DBScope {
			return fmt.Errorf("Invalid permission scope")
		}
		if perm.Scope == chronograf.DBScope && perm.Name == "" {
			return fmt.Errorf("Database scoped permission requires a name")
		}
	}
	return nil
}
