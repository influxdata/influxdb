package server

import "net/http"

type getMappingsResponse struct {
	Mappings []mapping `json:"mappings"`
}

type mapping struct {
	Measurement string `json:"measurement"` // The measurement where data for this mapping is found
	Name        string `json:"name"`        // The application name which will be assigned to the corresponding measurement
}

func (h *Store) GetMappings(w http.ResponseWriter, r *http.Request) {
	cpu := "cpu"
	system := "System"
	mp := getMappingsResponse{
		Mappings: []mapping{
			mapping{
				Measurement: cpu,
				Name:        system,
			},
		},
	}
	encodeJSON(w, http.StatusOK, mp, h.Logger)
}
