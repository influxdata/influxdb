package influxdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/influxdb/influxdb/influxql"
)

const (
	MAX_MAP_RESPONSE_SIZE = 1024 * 1024 * 1024
)

// RemoteMapper implements the influxql.Mapper interface. The engine uses the remote mapper
// to pull map results from shards that only exist on other servers in the cluster.
type RemoteMapper struct {
	dataNodes Balancer
	resp      *http.Response
	results   chan interface{}
	unmarshal influxql.UnmarshalFunc
	complete  bool
	decoder   *json.Decoder

	Call            string   `json:",omitempty"`
	Database        string   `json:",omitempty"`
	MeasurementName string   `json:",omitempty"`
	TMin            int64    `json:",omitempty"`
	TMax            int64    `json:",omitempty"`
	SeriesIDs       []uint64 `json:",omitempty"`
	ShardID         uint64   `json:",omitempty"`
	Filters         []string `json:",omitempty"`
	WhereFields     []*Field `json:",omitempty"`
	SelectFields    []*Field `json:",omitempty"`
	SelectTags      []string `json:",omitempty"`
	Limit           int      `json:",omitempty"`
	Offset          int      `json:",omitempty"`
	Interval        int64    `json:",omitempty"`
	ChunkSize       int      `json:",omitempty"`
}

// Responses get streamed back to the remote mapper from the remote machine that runs a local mapper
type MapResponse struct {
	Err       string `json:",omitempty"`
	Data      []byte
	Completed bool `json:",omitempty"`
}

// Open is a no op, real work is done starting with Being
func (m *RemoteMapper) Open() error { return nil }

// Close the response body
func (m *RemoteMapper) Close() {
	if m.resp != nil && m.resp.Body != nil {
		m.resp.Body.Close()
	}
}

// Begin sends a request to the remote server to start streaming map results
func (m *RemoteMapper) Begin(c *influxql.Call, startingTime int64, chunkSize int) error {
	// get the function for unmarshaling results
	f, err := influxql.InitializeUnmarshaller(c)
	if err != nil {
		return err
	}
	m.unmarshal = f

	if c != nil {
		m.Call = c.String()
	}
	m.ChunkSize = chunkSize
	m.TMin = startingTime

	// send the request to map to the remote server
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	var resp *http.Response
	for {
		node := m.dataNodes.Next()
		if node == nil {
			// no data nodes are available to service this query
			return ErrNoDataNodeAvailable
		}

		// request to start streaming results
		resp, err = http.Post(node.URL.String()+"/data/run_mapper", "application/json", bytes.NewReader(b))
		if err != nil {
			node.Down()
			continue
		}
		// Mark the node as up
		node.Up()
		break
	}

	m.resp = resp
	lr := io.LimitReader(m.resp.Body, MAX_MAP_RESPONSE_SIZE)
	m.decoder = json.NewDecoder(lr)

	return nil
}

// NextInterval is part of the mapper interface. In this case we read the next chunk from the remote mapper
func (m *RemoteMapper) NextInterval() (interface{}, error) {
	// just return nil if the mapper has completed its run
	if m.complete {
		return nil, nil
	}

	mr := &MapResponse{}
	err := m.decoder.Decode(&mr)
	if err != nil {
		return nil, err
	}
	if mr.Err != "" {
		return nil, errors.New(mr.Err)
	}

	// if it's a complete message, we've emptied this mapper of all data
	if mr.Completed {
		m.complete = true
		return nil, nil
	}

	// marshal the data that came from the MapFN
	v, err := m.unmarshal(mr.Data)
	if err != nil {
		return nil, err
	}

	return v, nil
}

// CallExpr will parse the Call string into an expression or return nil
func (m *RemoteMapper) CallExpr() (*influxql.Call, error) {
	if m.Call == "" {
		return nil, nil
	}

	c, err := influxql.ParseExpr(m.Call)
	if err != nil {
		return nil, err
	}
	call, ok := c.(*influxql.Call)

	if !ok {
		return nil, errors.New("unable to marshal aggregate call")
	}
	return call, nil
}

// FilterExprs will parse the filter strings and return any expressions. This array
// will be the same size as the SeriesIDs array with each element having a filter (which could be nil)
func (m *RemoteMapper) FilterExprs() []influxql.Expr {
	exprs := make([]influxql.Expr, len(m.SeriesIDs), len(m.SeriesIDs))

	// if filters is empty, they're all nil. if filters has one element, all filters
	// should be set to that. Otherwise marshal each filter
	if len(m.Filters) == 1 {
		f, _ := influxql.ParseExpr(m.Filters[0])
		for i, _ := range exprs {
			exprs[i] = f
		}
	} else if len(m.Filters) > 1 {
		for i, s := range m.Filters {
			f, _ := influxql.ParseExpr(s)
			exprs[i] = f
		}
	}

	return exprs
}

// SetFilters will convert the given arrray of filters into filters that can be marshaled and sent to the remote system
func (m *RemoteMapper) SetFilters(filters []influxql.Expr) {
	l := filters[0]
	allFiltersTheSame := true
	for _, f := range filters {
		if l != f {
			allFiltersTheSame = false
			break
		}
	}

	// we don't need anything if they're all the same and nil
	if l == nil && allFiltersTheSame {
		return
	} else if allFiltersTheSame { // just set one filter element since they're all the same
		m.Filters = []string{l.String()}
		return
	}

	// marshal all of them since there are different ones
	m.Filters = make([]string, len(filters), len(filters))
	for i, f := range filters {
		m.Filters[i] = f.String()
	}
}
