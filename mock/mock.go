package mock

import (
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/chronograf"
	"golang.org/x/net/context"
)

type SourcesStore struct {
	srcs map[int]chronograf.Source
}

func NewSourcesStore() chronograf.SourcesStore {
	return &SourcesStore{
		srcs: map[int]chronograf.Source{},
	}
}

func (s *SourcesStore) All(ctx context.Context) ([]chronograf.Source, error) {
	all := []chronograf.Source{}
	for _, src := range s.srcs {
		all = append(all, src)
	}
	return all, nil
}

func (s *SourcesStore) Add(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {
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

func (s *SourcesStore) Delete(ctx context.Context, src chronograf.Source) error {
	if _, ok := s.srcs[src.ID]; !ok {
		return fmt.Errorf("Error unknown id %d", src.ID)
	}
	delete(s.srcs, src.ID)
	return nil
}

func (s *SourcesStore) Get(ctx context.Context, ID int) (chronograf.Source, error) {
	if src, ok := s.srcs[ID]; ok {
		return src, nil
	}
	return chronograf.Source{}, fmt.Errorf("Error no such source %d", ID)
}

func (s *SourcesStore) Update(ctx context.Context, src chronograf.Source) error {
	if _, ok := s.srcs[src.ID]; !ok {
		return fmt.Errorf("Error unknown ID %d", src.ID)
	}
	s.srcs[src.ID] = src
	return nil
}

var DefaultSourcesStore chronograf.SourcesStore = NewSourcesStore()

type ExplorationStore struct {
	db      map[int]*chronograf.Exploration
	NowFunc func() time.Time
}

func NewExplorationStore(nowFunc func() time.Time) chronograf.ExplorationStore {
	e := ExplorationStore{
		NowFunc: nowFunc,
		db:      map[int]*chronograf.Exploration{},
	}
	e.db[0] = &chronograf.Exploration{
		Name:      "Ferdinand Magellan",
		Data:      "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		CreatedAt: nowFunc(),
		UpdatedAt: nowFunc(),
	}
	e.db[1] = &chronograf.Exploration{
		Name:      "Your Mom",
		Data:      "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		CreatedAt: nowFunc(),
		UpdatedAt: nowFunc(),
	}

	return &e
}

var DefaultExplorationStore chronograf.ExplorationStore = NewExplorationStore(time.Now)

func (m *ExplorationStore) Query(ctx context.Context, userID chronograf.UserID) ([]*chronograf.Exploration, error) {
	res := []*chronograf.Exploration{}
	for _, v := range m.db {
		res = append(res, v)
	}
	return res, nil
}

func (m *ExplorationStore) Add(ctx context.Context, e *chronograf.Exploration) (*chronograf.Exploration, error) {
	e.CreatedAt = m.NowFunc()
	e.UpdatedAt = m.NowFunc()
	m.db[len(m.db)] = e
	return e, nil
}

func (m *ExplorationStore) Delete(ctx context.Context, e *chronograf.Exploration) error {
	delete(m.db, int(e.ID))
	return nil
}

func (m *ExplorationStore) Get(ctx context.Context, ID chronograf.ExplorationID) (*chronograf.Exploration, error) {
	e, ok := m.db[int(ID)]
	if !ok {
		return nil, fmt.Errorf("Unknown ID %d", ID)
	}
	return e, nil
}

func (m *ExplorationStore) Update(ctx context.Context, e *chronograf.Exploration) error {
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
	Response chronograf.Response
}

func NewTimeSeries(hosts []string, response chronograf.Response) chronograf.TimeSeries {
	return &TimeSeries{
		Hosts:    hosts,
		Response: response,
	}
}

var DefaultTimeSeries chronograf.TimeSeries = NewTimeSeries([]string{"hydrogen", "helium", "hadron", "howdy"}, &Response{})

func (t *TimeSeries) Query(context.Context, chronograf.Query) (chronograf.Response, error) {
	return t.Response, nil
}

func (t *TimeSeries) Connect(ctx context.Context, src *chronograf.Source) error {
	return nil
}

func (t *TimeSeries) MonitoredServices(context.Context) ([]chronograf.MonitoredService, error) {
	hosts := make([]chronograf.MonitoredService, len(t.Hosts))
	for i, name := range t.Hosts {
		hosts[i].Type = "host"
		hosts[i].TagKey = "host"
		hosts[i].TagValue = name
	}
	return hosts, nil
}

type LayoutStore struct {
	Layouts     map[string]chronograf.Layout
	AllError    error
	AddError    error
	DeleteError error
	GetError    error
	UpdateError error
}

// All will return all info in the map or whatever is AllError
func (l *LayoutStore) All(ctx context.Context) ([]chronograf.Layout, error) {
	if l.AllError != nil {
		return nil, l.AllError
	}
	layouts := []chronograf.Layout{}
	for _, l := range l.Layouts {
		layouts = append(layouts, l)
	}
	return layouts, nil
}

// Add create a new ID and add to map or return AddError
func (l *LayoutStore) Add(ctx context.Context, layout chronograf.Layout) (chronograf.Layout, error) {
	if l.AddError != nil {
		return chronograf.Layout{}, l.AddError
	}
	id := strconv.Itoa(len(l.Layouts))
	layout.ID = id
	l.Layouts[id] = layout
	return layout, nil
}

// Delete will remove layout from map or return DeleteError
func (l *LayoutStore) Delete(ctx context.Context, layout chronograf.Layout) error {
	if l.DeleteError != nil {
		return l.DeleteError
	}

	id := layout.ID
	if _, ok := l.Layouts[id]; !ok {
		return chronograf.ErrLayoutNotFound
	}

	delete(l.Layouts, id)
	return nil
}

// Get will return map with key ID or GetError
func (l *LayoutStore) Get(ctx context.Context, ID string) (chronograf.Layout, error) {
	if l.GetError != nil {
		return chronograf.Layout{}, l.GetError
	}

	if layout, ok := l.Layouts[ID]; !ok {
		return chronograf.Layout{}, chronograf.ErrLayoutNotFound
	} else {
		return layout, nil
	}
}

// Update will update layout or return UpdateError
func (l *LayoutStore) Update(ctx context.Context, layout chronograf.Layout) error {
	if l.UpdateError != nil {
		return l.UpdateError
	}
	id := layout.ID
	if _, ok := l.Layouts[id]; !ok {
		return chronograf.ErrLayoutNotFound
	} else {
		l.Layouts[id] = layout
	}
	return nil
}
