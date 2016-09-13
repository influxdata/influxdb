package mock

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/mrfusion"
	"golang.org/x/net/context"
)

type ExplorationStore struct {
	db      map[int]mrfusion.Exploration
	NowFunc func() time.Time
}

func NewExplorationStore(nowFunc func() time.Time) mrfusion.ExplorationStore {
	e := ExplorationStore{
		NowFunc: nowFunc,
		db:      map[int]mrfusion.Exploration{},
	}
	e.db[1] = mrfusion.Exploration{
		ID:        1,
		Name:      "Ferdinand Magellan",
		UserID:    1,
		Data:      `"{"panels":{“123":{"id”:"123","queryIds":[“456"]}},"queryConfigs":{"456":{"id”:"456","database":null,"measurement":null,"retentionPolicy":null,"fields":[],"tags":{},"groupBy":{"time":null,"tags":[]},"areTagsAccepted":true,"rawText":null}}}"`,
		CreatedAt: nowFunc(),
		UpdatedAt: nowFunc(),
	}
	return &e
}

var DefaultExplorationStore mrfusion.ExplorationStore = NewExplorationStore(time.Now)

func (m *ExplorationStore) Query(ctx context.Context, userID int) ([]mrfusion.Exploration, error) {
	res := []mrfusion.Exploration{}
	for _, v := range m.db {
		res = append(res, v)
	}
	return res, nil
}

func (m *ExplorationStore) Add(ctx context.Context, e mrfusion.Exploration) error {
	e.CreatedAt = m.NowFunc()
	e.UpdatedAt = m.NowFunc()
	m.db[len(m.db)] = e
	return nil
}

func (m *ExplorationStore) Delete(ctx context.Context, e mrfusion.Exploration) error {
	delete(m.db, e.ID)
	return nil
}

func (m *ExplorationStore) Get(ctx context.Context, ID int) (mrfusion.Exploration, error) {
	e, ok := m.db[ID]
	if !ok {
		return mrfusion.Exploration{}, fmt.Errorf("Unknown ID %d", ID)
	}
	return e, nil
}

func (m *ExplorationStore) Update(ctx context.Context, e mrfusion.Exploration) error {
	_, ok := m.db[e.ID]
	if !ok {
		return fmt.Errorf("Unknown ID %d", e.ID)
	}
	e.UpdatedAt = m.NowFunc()
	m.db[e.ID] = e
	return nil
}

type Row struct {
	name    string            `json:"name,omitempty"`
	tags    map[string]string `json:"tags,omitempty"`
	columns []string          `json:"columns,omitempty"`
	values  [][]interface{}   `json:"values,omitempty"`
}

func NewRow(row string) mrfusion.Row {
	r := Row{}
	json.Unmarshal([]byte(row), &r)
	return &r
}

var SampleRow mrfusion.Row = NewRow(`{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}`)

func (r *Row) Name() string {
	return r.name
}

func (r *Row) Tags() map[string]string {
	return r.tags
}

func (r *Row) Columns() []string {
	return r.columns
}

func (r *Row) Values() [][]interface{} {
	return r.values
}

type Result struct {
	rows []mrfusion.Row
}

func NewResult(row mrfusion.Row) mrfusion.Result {
	return &Result{
		rows: []mrfusion.Row{row},
	}
}

var SampleResult mrfusion.Result = NewResult(SampleRow)

func (r *Result) Series() ([]mrfusion.Row, error) {
	return r.rows, nil
}

type Response struct {
	results []mrfusion.Result
}

var SampleResponse mrfusion.Response = NewResponse(SampleResult)

func NewResponse(result mrfusion.Result) mrfusion.Response {
	return &Response{
		results: []mrfusion.Result{result},
	}
}

func (r *Response) Results() ([]mrfusion.Result, error) {
	return r.results, nil
}
