package influxdb

import (
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// Notebook represents all visual and query data for a notebook.
type Notebook struct {
	OrgID     platform.ID  `json:"orgID"`
	ID        platform.ID  `json:"id"`
	Name      string       `json:"name"`
	Spec      NotebookSpec `json:"spec"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

// Spec is a specification which is just a blob of content provided by the client.
type NotebookSpec interface{}
