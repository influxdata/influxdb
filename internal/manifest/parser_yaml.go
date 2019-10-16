package manifest

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb"

	"gopkg.in/yaml.v2"
)

func ParseYAML(r io.Reader) (Manifest, error) {
	var man Manifest
	err := yaml.NewDecoder(r).Decode(&man)
	if err != nil {
		return Manifest{}, err
	}

	if err := man.graphResources(); err != nil {
		return Manifest{}, err
	}
	return man, nil
}

type Manifest struct {
	Version  string   `yaml:"apiVersion"`
	Kind     string   `yaml:"kind"`
	Name     string   `yaml:"name"`
	Metadata Metadata `yaml:"metadata"`
	Spec     struct {
		Resources []Resource `yaml:"resources"`
		Templates []Resource `yaml:"templates"`
	} `yaml:"spec"`

	labels     map[resourceKey]*Label
	buckets    map[resourceKey]*Bucket
	dashboards map[resourceKey]*Dashboard
}

func (m *Manifest) graphResources() error {
	graphFns := []func() error{
		// order is important here, labels must be run first or
		// whole world implodes since buckets can have ancestors
		// that should be viable labels at the root level of resources.
		m.graphLabels,
		m.graphBuckets,
		m.graphDashboards,
	}

	for _, fn := range graphFns {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manifest) graphLabels() error {
	m.labels = make(map[resourceKey]*Label)
	for i, r := range m.Spec.Resources {
		kind, err := r.kind()
		if err != nil {
			return fmt.Errorf("resource_index=%d err=%q", i, err)
		}
		if kind != kindLabel {
			continue
		}

		key := newResKey(r.Name(), true)
		m.labels[key] = &Label{
			Name: r.Name(),
		}
	}
	return nil
}

func (m *Manifest) graphBuckets() error {
	m.buckets = make(map[resourceKey]*Bucket)
	for i, r := range m.Spec.Resources {
		k, err := r.kind()
		if err != nil {
			return fmt.Errorf("resource_index=%d err=%q", i, err)
		}
		if k != kindBucket {
			continue
		}

		b := Bucket{
			Name:        r.Name(),
			Description: r.stringShort("description"),
		}
		rp, ok := r.duration("retention_period")
		if ok {
			b.RetentionPeriod = rp
		}

		for j, nr := range r.nestedResources() {
			k, err := nr.kind()
			if err != nil {
				return fmt.Errorf("resource_index=%d bucket=%q subresource_index=%d err=%q", i, b.Name, j, err)
			}
			if k != kindLabel {
				continue
			}

			inherits := nr.bool("inherit")

			key := newResKey(nr.Name(), inherits)
			lb, found := m.labels[key]
			if inherits && !found {
				return fmt.Errorf(`resource_index=%d bucket=%q subresource_index=%d ancestor_label=%q  err="label ancestor does not exist"`, i, b.Name, j, nr.Name())
			}
			if !found {
				lb = &Label{
					Name:    nr.Name(),
					inherit: inherits,
				}
				m.labels[key] = lb
			}
			b.Labels = append(b.Labels, lb)
		}
		m.buckets[newResKey(b.Name, true)] = &b
	}
	return nil
}

func (m *Manifest) graphDashboards() error {
	m.dashboards = make(map[resourceKey]*Dashboard)

	for i, r := range m.Spec.Resources {
		k, err := r.kind()
		if err != nil {
			return fmt.Errorf("resource_index=%d err=%q", i, err)
		}
		if k != kindDashboard {
			continue
		}

		d := Dashboard{
			Name:        r.Name(),
			Description: r.stringShort("description"),
		}

		for j, nr := range r.nestedResources() {
			k, err := nr.kind()
			if err != nil {
				return fmt.Errorf("resource_index=%d bucket=%q subresource_index=%d err=%q", i, d.Name, j, err)
			}
			if k != kindLabel {
				continue
			}

			inherits := nr.bool("inherit")

			key := newResKey(nr.Name(), inherits)
			lb, found := m.labels[key]
			if inherits && !found {
				return fmt.Errorf(`resource_index=%d bucket=%q subresource_index=%d ancestor_label=%q  err="label ancestor does not exist"`, i, d.Name, j, nr.Name())
			}
			if !found {
				lb = &Label{
					Name:    nr.Name(),
					inherit: inherits,
				}
				m.labels[key] = lb
			}
			d.Labels = append(d.Labels, lb)
		}

		cellifaces, _ := r["cells"].([]interface{})
		for _, cface := range cellifaces {
			cellRes, ok := ifaceMapToResource(cface)
			if !ok {
				continue
			}

			positions, ok := ifaceMapToResource(cellRes["positions"])
			if !ok {
				continue
			}

			d.Cells = append(d.Cells, &Cell{
				X:      positions.int("x_position"),
				Y:      positions.int("y_position"),
				Height: positions.int("height"),
				Width:  positions.int("width"),
			})
		}

		m.dashboards[newResKey(d.Name, true)] = &d
	}

	return nil
}

type resourceKey struct {
	name     string
	ancestor bool
}

func newResKey(name string, ancestor bool) resourceKey {
	return resourceKey{
		name:     name,
		ancestor: ancestor,
	}
}

func (m *Manifest) Buckets() []*Bucket {
	buckets := make([]*Bucket, 0, len(m.buckets))
	for _, b := range m.buckets {
		buckets = append(buckets, b)
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets
}

func (m *Manifest) Dashboards() []*Dashboard {
	dashs := make([]*Dashboard, 0, len(m.dashboards))
	for _, d := range m.dashboards {
		dashs = append(dashs, d)
	}

	sort.Slice(dashs, func(i, j int) bool {
		return dashs[i].Name < dashs[j].Name
	})

	return dashs
}

func (m *Manifest) Labels() []*Label {
	labels := make([]*Label, 0, len(m.labels))
	for _, l := range m.labels {
		labels = append(labels, l)
	}

	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	return labels
}

type Metadata struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
}

type Resource map[string]interface{}

type kind int

const (
	kindUnknown kind = iota
	kindBucket
	kindLabel
	kindDashboard
	kindPackage
)

func (r Resource) kind() (kind, error) {
	kind, ok := r.string("kind")
	if !ok {
		return kindUnknown, errors.New("no kind provided")
	}

	switch strings.ToLower(kind) {
	case "bucket":
		return kindBucket, nil
	case "label":
		return kindLabel, nil
	case "dashboard":
		return kindDashboard, nil
	case "package":
		return kindPackage, nil
	default:
		return kindUnknown, fmt.Errorf("invalid kind: %q", kind)
	}
}

func (r Resource) Name() string {
	return r.stringShort("name")
}

func (r Resource) nestedResources() []Resource {
	v, ok := r["resources"]
	if !ok {
		return nil
	}

	ifaces, ok := v.([]interface{})
	if !ok {
		return nil
	}

	var resources []Resource
	for _, res := range ifaces {
		newRes, ok := ifaceMapToResource(res)
		if !ok {
			continue
		}
		resources = append(resources, newRes)
	}

	return resources
}

func (r Resource) bool(key string) bool {
	b, _ := r[key].(bool)
	return b
}

func ifaceMapToResource(i interface{}) (Resource, bool) {
	m, ok := i.(map[interface{}]interface{})
	if !ok {
		return nil, false
	}

	newRes := make(Resource)
	for k, v := range m {
		s, ok := k.(string)
		if !ok {
			continue
		}
		newRes[s] = v
	}
	return newRes, true
}

func (r Resource) int(key string) int {
	i, ok := r[key].(int)
	if !ok {
		return 0
	}

	return i
}

func (r Resource) string(key string) (string, bool) {
	s, ok := r[key].(string)
	return s, ok
}

func (r Resource) stringShort(key string) string {
	s, _ := r.string(key)
	return s
}

func (r Resource) duration(key string) (time.Duration, bool) {
	dur, err := time.ParseDuration(r.stringShort(key))
	return dur, err == nil
}

type Bucket struct {
	ID              influxdb.ID
	Description     string
	Name            string
	RetentionPeriod time.Duration

	Labels []*Label
}

type Label struct {
	ID      influxdb.ID
	Name    string
	inherit bool
}

type Dashboard struct {
	ID          influxdb.ID
	Name        string
	Description string
	Cells       []*Cell

	Labels []*Label
}

type Cell struct {
	ID            influxdb.ID
	X, Y          int
	Height, Width int
}
