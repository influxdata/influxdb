package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/platform/chronograf/v2"
)

type cellV2Links struct {
	Self string `json:"self"`
}

type cellV2Response struct {
	platform.Cell
	Links cellV2Links `json:"links"`
}

func (r cellV2Response) MarshalJSON() ([]byte, error) {
	vis, err := platform.MarshalVisualizationJSON(r.Visualization)
	if err != nil {
		return nil, err
	}

	return json.Marshal(struct {
		platform.CellContents
		Links         cellV2Links     `json:"links"`
		Visualization json.RawMessage `json:"visualization"`
	}{
		CellContents:  r.CellContents,
		Links:         r.Links,
		Visualization: vis,
	})
}

func newCellV2Response(c *platform.Cell) cellV2Response {
	return cellV2Response{
		Links: cellV2Links{
			Self: fmt.Sprintf("/chronograf/v2/cells/%s", c.ID),
		},
		Cell: *c,
	}
}

// CellsV2 returns all cells within the store.
func (s *Service) CellsV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// TODO: support filtering via query params
	cells, _, err := s.Store.Cells(ctx).FindCells(ctx, platform.CellFilter{})
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading cells", s.Logger)
		return
	}

	s.encodeGetCellsResponse(w, cells)
}

type getCellsLinks struct {
	Self string `json:"self"`
}

type getCellsResponse struct {
	Links getCellsLinks    `json:"links"`
	Cells []cellV2Response `json:"cells"`
}

func (s *Service) encodeGetCellsResponse(w http.ResponseWriter, cells []*platform.Cell) {
	res := getCellsResponse{
		Links: getCellsLinks{
			Self: "/chronograf/v2/cells",
		},
		Cells: make([]cellV2Response, 0, len(cells)),
	}

	for _, cell := range cells {
		res.Cells = append(res.Cells, newCellV2Response(cell))
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// NewCellV2 creates a new cell.
func (s *Service) NewCellV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostCellRequest(ctx, r)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	if err := s.Store.Cells(ctx).CreateCell(ctx, req.Cell); err != nil {
		Error(w, http.StatusInternalServerError, fmt.Sprintf("Error loading cells: %v", err), s.Logger)
		return
	}

	s.encodePostCellResponse(w, req.Cell)
}

type postCellRequest struct {
	Cell *platform.Cell
}

func decodePostCellRequest(ctx context.Context, r *http.Request) (*postCellRequest, error) {
	c := &platform.Cell{}
	if err := json.NewDecoder(r.Body).Decode(c); err != nil {
		return nil, err
	}
	return &postCellRequest{
		Cell: c,
	}, nil
}

func (s *Service) encodePostCellResponse(w http.ResponseWriter, cell *platform.Cell) {
	encodeJSON(w, http.StatusCreated, newCellV2Response(cell), s.Logger)
}

// CellIDV2 retrieves a cell by ID.
func (s *Service) CellIDV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetCellRequest(ctx, r)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	cell, err := s.Store.Cells(ctx).FindCellByID(ctx, req.CellID)
	if err == platform.ErrCellNotFound {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, fmt.Sprintf("Error loading cell: %v", err), s.Logger)
		return
	}

	s.encodeGetCellResponse(w, cell)
}

type getCellRequest struct {
	CellID platform.ID
}

func decodeGetCellRequest(ctx context.Context, r *http.Request) (*getCellRequest, error) {
	param := httprouter.GetParamFromContext(ctx, "id")
	return &getCellRequest{
		CellID: platform.ID(param),
	}, nil
}

func (s *Service) encodeGetCellResponse(w http.ResponseWriter, cell *platform.Cell) {
	encodeJSON(w, http.StatusOK, newCellV2Response(cell), s.Logger)
}

// RemoveCellV2 removes a cell by ID.
func (s *Service) RemoveCellV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteCellRequest(ctx, r)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	err = s.Store.Cells(ctx).DeleteCell(ctx, req.CellID)
	if err == platform.ErrCellNotFound {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, fmt.Sprintf("Error deleting cell: %v", err), s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteCellRequest struct {
	CellID platform.ID
}

func decodeDeleteCellRequest(ctx context.Context, r *http.Request) (*deleteCellRequest, error) {
	param := httprouter.GetParamFromContext(ctx, "id")
	return &deleteCellRequest{
		CellID: platform.ID(param),
	}, nil
}

// UpdateCellV2 updates a cell.
func (s *Service) UpdateCellV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchCellRequest(ctx, r)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	cell, err := s.Store.Cells(ctx).UpdateCell(ctx, req.CellID, req.Upd)
	if err == platform.ErrCellNotFound {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, fmt.Sprintf("Error updating cell: %v", err), s.Logger)
		return
	}

	s.encodePatchCellResponse(w, cell)
}

type patchCellRequest struct {
	CellID platform.ID
	Upd    platform.CellUpdate
}

func decodePatchCellRequest(ctx context.Context, r *http.Request) (*patchCellRequest, error) {
	req := &patchCellRequest{}
	upd := platform.CellUpdate{}
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	req.Upd = upd

	param := httprouter.GetParamFromContext(ctx, "id")

	req.CellID = platform.ID(param)

	if err := req.Valid(); err != nil {
		return nil, err
	}

	return req, nil
}

// Valid validates that the cell ID is non zero valued and update has expected values set.
func (r *patchCellRequest) Valid() error {
	if r.CellID == "" {
		return fmt.Errorf("missing cell ID")
	}

	return r.Upd.Valid()
}

func (s *Service) encodePatchCellResponse(w http.ResponseWriter, cell *platform.Cell) {
	encodeJSON(w, http.StatusOK, newCellV2Response(cell), s.Logger)
}
