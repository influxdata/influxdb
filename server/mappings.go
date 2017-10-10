package server

import "net/http"

type getMappingsResponse struct {
	Mappings []mapping `json:"mappings"`
}

type mapping struct {
	Measurement string `json:"measurement"` // The measurement where data for this mapping is found
	Name        string `json:"name"`        // The application name which will be assigned to the corresponding measurement
}

// GetMappings returns the known mappings of measurements to applications
func (s *Service) GetMappings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	layouts, err := s.LayoutStore.All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading layouts", s.Logger)
		return
	}

	mp := getMappingsResponse{
		Mappings: []mapping{},
	}

	seen := make(map[string]bool)

	for _, layout := range layouts {
		if seen[layout.Measurement+layout.ID] {
			continue
		}
		mp.Mappings = append(mp.Mappings, mapping{layout.Measurement, layout.Application})
		seen[layout.Measurement+layout.ID] = true
	}

	encodeJSON(w, http.StatusOK, mp, s.Logger)
}
