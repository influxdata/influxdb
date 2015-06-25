package tsdb

import (
	"fmt"
	"hash/fnv"
	"time"
)

type GoRpcPoint struct {
	Time time.Time
	Name string
	Tags
	Fields
}

type gorpcPoint struct {
	point GoRpcPoint
	data  []byte
}

// NewPoint returns a new point with the given GoRpcPoint
func NewGoRpcPoint(p GoRpcPoint) (Point, error) {
	if err := validateFields(p.Fields); err != nil {
		return nil, err
	}
	return &gorpcPoint{point: p}, nil
}

func (p *gorpcPoint) Data() []byte {
	return p.data
}

func (p *gorpcPoint) SetData(b []byte) {
	p.data = b
}

func (p *gorpcPoint) Key() []byte {
	return append(escape([]byte(p.point.Name)), p.point.Tags.hashKey()...)
}

// Name return the measurement name for the point
func (p *gorpcPoint) Name() string { return p.point.Name }

// SetName updates the measurement name for the point
func (p *gorpcPoint) SetName(name string) {
	p.point.Name = name
}

// Time return the timesteamp for the point
func (p *gorpcPoint) Time() time.Time {
	return p.point.Time
}

// SetTime updates the timestamp for the point
func (p *gorpcPoint) SetTime(t time.Time) {
	p.point.Time = t
}

// Tags returns the tag set for the point
func (p *gorpcPoint) Tags() Tags {
	return p.point.Tags
}

// SetTags replaces the tags for the point
func (p *gorpcPoint) SetTags(tags Tags) {
	p.point.Tags = tags
}

// AddTag adds or replaces a tag value for a point
func (p *gorpcPoint) AddTag(key, value string) {
	p.point.Tags[key] = value
}

// Fields returns the fiels for the point
func (p *gorpcPoint) Fields() Fields {
	return p.point.Fields
}

// AddField adds or replaces a field value for a point
func (p *gorpcPoint) AddField(name string, value interface{}) {
	p.point.Fields[name] = value
}

func (p *gorpcPoint) String() string {
	if p.Time().IsZero() {
		return fmt.Sprintf("%s %v", p.Key(), p.Fields)
	}
	return fmt.Sprintf("%s %v %d", p.Key(), p.Fields, p.UnixNano())
}

func (p *gorpcPoint) HashID() uint64 {
	h := fnv.New64a()
	h.Write(p.Key())
	sum := h.Sum64()
	return sum
}

func (p *gorpcPoint) UnixNano() int64 {
	return p.Time().UnixNano()
}

func validateFields(fields Fields) error {
	for key, value := range fields {
		switch v := value.(type) {
		case float64, int64, string, bool:
			continue
		default:
			return fmt.Errorf("Unsupported value type:%T key:%s,", v, key)
		}
	}
	return nil
}
