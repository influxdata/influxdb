package mock

import (
	"fmt"
	"time"

	"github.com/influxdata/mrfusion"
	"golang.org/x/net/context"
)

type SourcesStore struct {
	srcs map[int]mrfusion.Source
}

func NewSourcesStore() mrfusion.SourcesStore {
	return &SourcesStore{
		srcs: map[int]mrfusion.Source{},
	}
}

func (s *SourcesStore) All(ctx context.Context) ([]mrfusion.Source, error) {
	all := []mrfusion.Source{}
	for _, src := range s.srcs {
		all = append(all, src)
	}
	return all, nil
}

func (s *SourcesStore) Add(ctx context.Context, src mrfusion.Source) (mrfusion.Source, error) {
	id := len(s.srcs) + 1
	for k, _ := range s.srcs {
		if k >= id {
			id = k + 1
			break
		}
	}
	src.ID = id
	s.srcs[id] = src
	return src, nil
}

func (s *SourcesStore) Delete(ctx context.Context, src mrfusion.Source) error {
	if _, ok := s.srcs[src.ID]; !ok {
		return fmt.Errorf("Error unknown id %d", src.ID)
	}
	delete(s.srcs, src.ID)
	return nil
}

func (s *SourcesStore) Get(ctx context.Context, ID int) (mrfusion.Source, error) {
	if src, ok := s.srcs[ID]; ok {
		return src, nil
	}
	return mrfusion.Source{}, fmt.Errorf("Error no such source %d", ID)
}

func (s *SourcesStore) Update(ctx context.Context, src mrfusion.Source) error {
	if _, ok := s.srcs[src.ID]; !ok {
		return fmt.Errorf("Error unknown ID %d", src.ID)
	}
	s.srcs[src.ID] = src
	return nil
}

var DefaultSourcesStore mrfusion.SourcesStore = NewSourcesStore()

type ExplorationStore struct {
	db      map[int]*mrfusion.Exploration
	NowFunc func() time.Time
}

func NewExplorationStore(nowFunc func() time.Time) mrfusion.ExplorationStore {
	e := ExplorationStore{
		NowFunc: nowFunc,
		db:      map[int]*mrfusion.Exploration{},
	}
	e.db[0] = &mrfusion.Exploration{
		Name:      "Ferdinand Magellan",
		Data:      "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		CreatedAt: nowFunc(),
		UpdatedAt: nowFunc(),
	}
	e.db[1] = &mrfusion.Exploration{
		Name:      "Your Mom",
		Data:      "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		CreatedAt: nowFunc(),
		UpdatedAt: nowFunc(),
	}

	return &e
}

var DefaultExplorationStore mrfusion.ExplorationStore = NewExplorationStore(time.Now)

func (m *ExplorationStore) Query(ctx context.Context, userID mrfusion.UserID) ([]*mrfusion.Exploration, error) {
	res := []*mrfusion.Exploration{}
	for _, v := range m.db {
		res = append(res, v)
	}
	return res, nil
}

func (m *ExplorationStore) Add(ctx context.Context, e *mrfusion.Exploration) (*mrfusion.Exploration, error) {
	e.CreatedAt = m.NowFunc()
	e.UpdatedAt = m.NowFunc()
	m.db[len(m.db)] = e
	return e, nil
}

func (m *ExplorationStore) Delete(ctx context.Context, e *mrfusion.Exploration) error {
	delete(m.db, int(e.ID))
	return nil
}

func (m *ExplorationStore) Get(ctx context.Context, ID mrfusion.ExplorationID) (*mrfusion.Exploration, error) {
	e, ok := m.db[int(ID)]
	if !ok {
		return nil, fmt.Errorf("Unknown ID %d", ID)
	}
	return e, nil
}

func (m *ExplorationStore) Update(ctx context.Context, e *mrfusion.Exploration) error {
	_, ok := m.db[int(e.ID)]
	if !ok {
		return fmt.Errorf("Unknown ID %d", e.ID)
	}
	e.UpdatedAt = m.NowFunc()
	m.db[int(e.ID)] = e
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

func (t *TimeSeries) Connect(ctx context.Context, src *mrfusion.Source) error {
	return nil
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
