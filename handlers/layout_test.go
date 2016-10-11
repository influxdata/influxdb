package handlers_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/handlers"
	"github.com/influxdata/mrfusion/mock"
	"github.com/influxdata/mrfusion/models"
	op "github.com/influxdata/mrfusion/restapi/operations"
)

func TestNewLayout(t *testing.T) {
	t.Parallel()
	var tests = []struct {
		Desc            string
		AddError        error
		ExistingLayouts map[string]mrfusion.Layout
		NewLayout       *models.Layout
		ExpectedID      int
		ExpectedHref    string
		ExpectedStatus  int
	}{
		{
			Desc:     "Test that an error in datastore returns 500 status",
			AddError: errors.New("error"),
			NewLayout: &models.Layout{
				Measurement: new(string),
				App:         new(string),
				Cells: []*models.Cell{
					&models.Cell{
						X: new(int32),
						Y: new(int32),
						W: new(int32),
						H: new(int32),
					},
				},
			},
			ExpectedStatus: http.StatusInternalServerError,
		},
		{
			Desc:            "Test that creating a layout returns 201 status",
			ExistingLayouts: map[string]mrfusion.Layout{},
			NewLayout: &models.Layout{
				Measurement: new(string),
				App:         new(string),
				Cells: []*models.Cell{
					&models.Cell{
						X: new(int32),
						Y: new(int32),
						W: new(int32),
						H: new(int32),
					},
				},
			},
			ExpectedID:     0,
			ExpectedHref:   "/chronograf/v1/layouts/0",
			ExpectedStatus: http.StatusCreated,
		},
	}

	for _, test := range tests {
		// The mocked backing store will be used to
		// check stored values.
		store := handlers.Store{
			LayoutStore: &mock.LayoutStore{
				AddError: test.AddError,
				Layouts:  test.ExistingLayouts,
			},
		}

		// Send the test layout to the mocked store.
		params := op.PostLayoutsParams{
			Layout: test.NewLayout,
		}
		resp := store.NewLayout(context.Background(), params)
		w := httptest.NewRecorder()
		resp.WriteResponse(w, runtime.JSONProducer())
		if w.Code != test.ExpectedStatus {
			t.Fatalf("Expected status %d; actual %d", test.ExpectedStatus, w.Code)
		}
		loc := w.Header().Get("Location")
		if loc != test.ExpectedHref {
			t.Fatalf("Expected status %s; actual %s", test.ExpectedHref, loc)
		}
	}
}
