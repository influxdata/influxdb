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

	buckets map[resourceKey]*Bucket
	labels  map[resourceKey]*Label
}

func (m *Manifest) graphResources() error {
	graphFns := []func() error{
		// order is important here, labels must be run first or
		// whole world implodes since buckets can have ancestors
		// that should be viable labels at the root level of resources.
		m.graphLabels,
		m.graphBuckets,
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

		desc, _ := r.string("description")

		b := Bucket{
			Name:        r.Name(),
			Description: desc,
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
		return kindUnknown, errors.New("invalid kind: " + kind)
	}
}

func (r Resource) Name() string {
	name, _ := r.string("name")
	return name
}

func (r Resource) nestedResources() []Resource {
	v, ok := r["resources"]
	if !ok {
		return nil
	}

	var resources []Resource
	for _, res := range v.([]interface{}) {
		newRes := make(Resource)
		for k, v := range res.(map[interface{}]interface{}) {
			s, ok := k.(string)
			if !ok {
				continue
			}
			newRes[s] = v
		}
		resources = append(resources, newRes)
	}

	return resources
}

func (r Resource) bool(key string) bool {
	b, _ := r[key].(bool)
	return b
}

func (r Resource) string(key string) (string, bool) {
	s, ok := r[key].(string)
	return s, ok
}

func (r Resource) duration(key string) (time.Duration, bool) {
	v, ok := r.string(key)
	if !ok {
		return 0, false
	}
	dur, err := time.ParseDuration(v)
	return dur, err == nil
}

type Bucket struct {
	ID              influxdb.ID
	Description     string
	Name            string
	RetentionPeriod time.Duration
	Labels          []*Label
}

type Label struct {
	ID      influxdb.ID
	Name    string
	inherit bool
}
