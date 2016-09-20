package mock

import (
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
		Name:      "Ferdinand Magellan",
		Data:      "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
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

type Response struct {
}

func (r *Response) MarshalJSON() ([]byte, error) {
	return []byte(`[{"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]`), nil
}

type TimeSeries struct {
	Hosts    []string
	Response mrfusion.Response
}

func NewTimeSeries(hosts []string, response mrfusion.Response) mrfusion.TimeSeries {
	return &TimeSeries{
		Hosts:    hosts,
		Response: response,
	}
}

var DefaultTimeSeries mrfusion.TimeSeries = NewTimeSeries([]string{"hydrogen", "helium", "hadron", "howdy"}, &Response{})

func (t *TimeSeries) Query(context.Context, mrfusion.Query) (mrfusion.Response, error) {
	return t.Response, nil
}

func (t *TimeSeries) MonitoredServices(context.Context) ([]mrfusion.MonitoredService, error) {
	hosts := make([]mrfusion.MonitoredService, len(t.Hosts))
	for i, name := range t.Hosts {
		hosts[i].Type = "host"
		hosts[i].TagKey = "host"
		hosts[i].TagValue = name
	}
	return hosts, nil
}
