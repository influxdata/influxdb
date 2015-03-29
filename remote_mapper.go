package influxdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/influxql"
)

const (
	MAX_MAP_RESPONSE_SIZE = 1024 * 1024 * 1024
)

// RemoteMapper implements the influxql.Mapper interface. The engine uses the remote mapper
// to pull map results from shards that only exist on other servers in the cluster.
type RemoteMapper struct {
	dataNodes []*DataNode
	resp      *http.Response
	results   chan interface{}
	unmarshal influxql.UnmarshalFunc

	Call            string
	Database        string
	MeasurementName string
	TMin            int64
	TMax            int64
	SeriesIDs       []uint32
	ShardID         uint64
	Filters         []string
	WhereFields     []*Field
	SelectFields    []*Field
	SelectTags      []string
	Limit           int
}

type MapResponse struct {
	Err  string
	Data []byte
}

// Open is a no op, real work is done starting with Being
func (m *RemoteMapper) Open() error { return nil }

// Close the response body
func (m *RemoteMapper) Close() {
	if m.resp != nil && m.resp.Body != nil {
		m.resp.Body.Close()
	}
}

// Begin sends a request to the remote server
func (m *RemoteMapper) Begin(c *influxql.Call, startingTime int64, limit int) error {
	// get the function for unmarshaling results
	f, err := influxql.InitializeUnmarshaller(c)
	if err != nil {
		return err
	}
	m.unmarshal = f

	if c != nil {
		m.Call = c.String()
	}
	m.Limit = limit

	// send the request to map to the remote server
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	resp, err := http.Post(m.dataNodes[0].URL.String()+"/run_mapper", "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	m.resp = resp

	return nil
}

func (m *RemoteMapper) NextInterval(interval int64) (interface{}, error) {
	chunk := make([]byte, MAX_MAP_RESPONSE_SIZE, MAX_MAP_RESPONSE_SIZE)
	n, err := m.resp.Body.Read(chunk)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	warn("... ", string(chunk[:n]))
	mr := &MapResponse{}
	err = json.Unmarshal(chunk[:n], mr)
	if err != nil {
		return nil, err
	}
	if mr.Err != "" {
		return nil, errors.New(mr.Err)
	}
	v, err := m.unmarshal(mr.Data)
	if err != nil {
		return nil, err
	}
	spew.Dump(v)
	return v, nil
}

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
		return nil, errors.New("Could't marshal aggregate call")
	}
	return call, nil
}

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
