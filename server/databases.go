package server

import (
  "net/http"
  "fmt"
)

type jsonResponse struct{
  id int
}

// Databases queries the list of all databases for a source
func (h *Service) Databases (w http.ResponseWriter, r *http.Request) {
  fmt.Print("database endpoint")

  srcID, err := paramID("id", r)
  if err != nil {
    Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
    return
  }

  res := jsonResponse{id: srcID}

  encodeJSON(w, http.StatusOK, res, h.Logger)
}
