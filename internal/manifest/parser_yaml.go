package manifest

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"gopkg.in/yaml.v2"
)

func ParseYAML(r io.Reader) (*Manifest, error) {
	var man Manifest
	err := yaml.NewDecoder(r).Decode(&man)
	if err != nil {
		return nil, err
	}

	if err := man.graphResources(); err != nil {
		return nil, err
	}
	return &man, nil
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
	return m.eachResource(func(r Resource) error {
		kind, err := r.kind()
		if err != nil {
			return fmt.Errorf("err=%q", err)
		}
		if kind != kindLabel {
			return nil
		}

		key := newResKey(r.Name(), true)
		m.labels[key] = resourceToLabel(r)
		return nil
	})
}

func (m *Manifest) graphBuckets() error {
	m.buckets = make(map[resourceKey]*Bucket)

	return m.eachResource(func(r Resource) error {
		k, err := r.kind()
		if err != nil {
			return fmt.Errorf("err=%q", err)
		}
		if k != kindBucket {
			return nil
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
			err := m.proccessNestedLabel(nr, func(lb *Label) {
				b.Labels = append(b.Labels, lb)
			})
			if err != nil {
				return fmt.Errorf(`bucket=%q subresource_index=%d %s`, b.Name, j, err)
			}
		}
		m.buckets[newResKey(b.Name, true)] = &b

		return nil
	})
}

func (m *Manifest) graphDashboards() error {
	m.dashboards = make(map[resourceKey]*Dashboard)
	return m.eachResource(func(r Resource) error {
		k, err := r.kind()
		if err != nil {
			return fmt.Errorf("err=%q", err)
		}
		if k != kindDashboard {
			return nil
		}

		d := Dashboard{
			Name:        r.Name(),
			Description: r.stringShort("description"),
		}

		for j, nr := range r.nestedResources() {
			err := m.proccessNestedLabel(nr, func(lb *Label) {
				d.Labels = append(d.Labels, lb)
			})
			if err != nil {
				return fmt.Errorf(`dashboard=%q subresource_index=%d %s`, d.Name, j, err)
			}
		}

		cellifaces, _ := r["cells"].([]interface{})
		for i, cface := range cellifaces {
			cellRes, ok := ifaceMapToResource(cface)
			if !ok {
				// TODO: these !ok marks should probably return an error here so the
				// 		user can fix it
				continue
			}

			positions, ok := ifaceMapToResource(cellRes["positions"])
			if !ok {
				continue
			}
			newView, err := m.parseView(cellRes["view"])
			if err != nil {
				return fmt.Errorf("cell_index=%d err=%q", i, err)
			}

			d.Cells = append(d.Cells, &Cell{
				X:      positions.int("x_position"),
				Y:      positions.int("y_position"),
				Height: positions.int("height"),
				Width:  positions.int("width"),
				View:   &newView,
			})
		}

		m.dashboards[newResKey(d.Name, true)] = &d
		return nil
	})
}

func (m *Manifest) parseView(v interface{}) (View, error) {
	view, ok := ifaceMapToResource(v)
	if !ok {
		return View{}, errors.New("no view provided")
	}

	viewProps, ok := ifaceMapToResource(view["properties"])
	if !ok {
		return View{}, errors.New("no properties provided for view")
	}

	newView := View{
		Name: view.Name(),
		properties: viewProperties{
			Type:        viewProps.stringShort("type"),
			Name:        viewProps.Name(),
			Prefix:      viewProps.stringShort("prefix"),
			Suffix:      viewProps.stringShort("suffix"),
			Geometry:    viewProps.stringShort("geom"),
			Note:        viewProps.stringShort("note"),
			NoteOnEmpty: viewProps.bool("noteOnEmpty"),
			Shade:       viewProps.bool("shade"),
			XCol:        viewProps.stringShort("xColumn"),
			YCol:        viewProps.stringShort("yColumn"),
			Axes:        m.parseAxes(viewProps["axes"]),
			Colors:      []influxdb.ViewColor{},
			Queries:     m.parseQueries(viewProps["queries"]),
		},
	}

	legend, ok := ifaceMapToResource(viewProps["legend"])
	if ok {
		newView.properties.Legend = influxdb.Legend{
			Type:        legend.stringShort("type"),
			Orientation: legend.stringShort("orientation"),
		}
	}

	decimalPlaces, ok := ifaceMapToResource(viewProps["decimalPlaces"])
	if ok {
		newView.properties.DecimalPlaces = influxdb.DecimalPlaces{
			IsEnforced: decimalPlaces.bool("enforced"),
			Digits:     int32(decimalPlaces.int("digits")),
		}
	}

	colors, ok := viewProps["colors"].([]interface{})
	if ok {
		for _, c := range colors {
			color, ok := ifaceMapToResource(c)
			if !ok {
				// TODO: return error if !ok?
				continue
			}

			// TODO: probably need to map these to corresponding colors we bake out
			//  in the UI for now
			newView.properties.Colors = append(newView.properties.Colors, influxdb.ViewColor{
				ID:    color.stringShort("id"),
				Type:  color.stringShort("label_type"),
				Hex:   color.stringShort("hex"),
				Name:  color.Name(),
				Value: color.float64("value"),
			})
		}
	}

	return newView, nil
}

func (m *Manifest) parseQueries(v interface{}) []influxdb.DashboardQuery {
	queries, ok := v.([]interface{})
	//TODO: do we return error if there is no query?
	if !ok {
		return nil
	}

	dashQueries := make([]influxdb.DashboardQuery, 0, len(queries))
	for _, q := range queries {
		query, ok := ifaceMapToResource(q)
		if !ok {
			// TODO: handle this with error?
			continue
		}

		qCfg, ok := ifaceMapToResource(query["config"])
		if !ok {
			// TODO: handle this with error?
			continue
		}

		newQuery := influxdb.DashboardQuery{
			Text:     query.stringShort("text"),
			EditMode: query.stringShort("editMode"),
			Name:     query.Name(),
			BuilderConfig: influxdb.BuilderConfig{
				Buckets: []string{},
				Functions: []struct {
					Name string `json:"name"`
				}{},
			},
		}

		buckets, ok := qCfg.slcStr("buckets")
		if ok {
			// TODO: handle this with error?
			newQuery.BuilderConfig.Buckets = buckets
		}

		ifaceTags, ok := qCfg["tags"].([]interface{})
		if ok {
			for _, itag := range ifaceTags {
				tag, ok := ifaceMapToResource(itag)
				if !ok {
					// TODO: handle this with error?
					continue
				}

				values, _ := tag.slcStr("values")

				influxTag := influxdb.NewBuildConfigTags(tag.stringShort("key"), values)
				newQuery.BuilderConfig.Tags = append(newQuery.BuilderConfig.Tags, influxTag)
			}
		}

		ifaceFuncs, ok := qCfg["functions"].([]interface{})
		if ok {
			for _, iFunc := range ifaceFuncs {
				fnRes, ok := ifaceMapToResource(iFunc)
				if !ok || fnRes.Name() == "" {
					// TODO: handle error
					continue
				}

				fn := influxdb.NewBuildConfigFunc(fnRes.Name())
				newQuery.BuilderConfig.Functions = append(newQuery.BuilderConfig.Functions, fn)
			}
		}

		ifaceAggs, ok := qCfg["aggregateWindow"]
		if ok {
			aggRes, ok := ifaceMapToResource(ifaceAggs)
			if !ok {
				continue
			}
			newQuery.BuilderConfig.AggregateWindow.Period = aggRes.stringShort("period")
		}
		dashQueries = append(dashQueries, newQuery)
	}

	return dashQueries
}

func (m *Manifest) parseAxes(v interface{}) map[string]influxdb.Axis {
	axes, ok := ifaceMapToResource(v)
	if !ok {
		return nil
	}

	newAxes := make(map[string]influxdb.Axis)
	for k, v := range axes {
		axis, ok := ifaceMapToResource(v)
		if !ok {
			continue
		}

		newAxis := influxdb.Axis{
			Label:  axis.stringShort("label"),
			Prefix: axis.stringShort("prefix"),
			Suffix: axesSuffixConvert(axis.stringShort("suffix")),
			Base:   strconv.Itoa(axis.int("base")),
			Scale:  axis.stringShort("scale"),
		}
		bounds, ok := axis.slcStr("bounds")
		if ok {
			// TODO: are bound required? should this return an error if no bounds?
			newAxis.Bounds = bounds
		}

		// TODO: fix this junks dirty low down hack to get `y` to not be converted into a boolean value
		if k == "y1" {
			k = "y"
		}
		newAxes[k] = newAxis
	}
	return newAxes
}

func (m *Manifest) eachResource(fn func(r Resource) error) error {
	for i, r := range m.Spec.Resources {
		err := fn(r)
		if err != nil {
			return fmt.Errorf("resource_index=%d %s", i, err)
		}
	}
	return nil
}

func (m *Manifest) proccessNestedLabel(nr Resource, fn func(lb *Label)) error {
	k, err := nr.kind()
	if err != nil {
		return fmt.Errorf("err=%q", err)
	}
	if k != kindLabel {
		return nil
	}

	inherits := nr.bool("inherit")

	key := newResKey(nr.Name(), inherits)
	lb, found := m.labels[key]
	if inherits && !found {
		return fmt.Errorf(`ancestor_label=%q  err="label ancestor does not exist"`, nr.Name())
	}
	if !found {
		lb = resourceToLabel(nr)
		m.labels[key] = lb
	}

	fn(lb)
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

type Bucket struct {
	ID              influxdb.ID
	Description     string
	Name            string
	RetentionPeriod time.Duration

	Labels []*Label
}

type Label struct {
	ID         influxdb.ID
	Name       string
	Properties map[string]string
	inherit    bool
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
	View          *View
}

type View struct {
	ID         influxdb.ID
	Name       string
	properties viewProperties
}

type viewProperties struct {
	Type          string
	Name          string
	Prefix        string
	Suffix        string
	Geometry      string
	Note          string
	NoteOnEmpty   bool
	Shade         bool
	XCol, YCol    string
	Axes          map[string]influxdb.Axis
	Legend        influxdb.Legend
	DecimalPlaces influxdb.DecimalPlaces
	Colors        []influxdb.ViewColor
	Queries       []influxdb.DashboardQuery
}

//TODO: finish up manfiest/CLI side of things
func (v *View) Properties() influxdb.ViewProperties {
	switch strings.ToLower(v.properties.Type) {
	case "line_with_single_stat":
		return influxdb.LinePlusSingleStatProperties{
			Queries: v.properties.Queries,
			Axes:    v.properties.Axes,
			// does this magic str live anywhere? couldn't find it
			Type:              "line-plus-single-stat",
			Legend:            v.properties.Legend,
			ViewColors:        v.properties.Colors,
			Prefix:            v.properties.Prefix,
			Suffix:            v.properties.Suffix,
			DecimalPlaces:     v.properties.DecimalPlaces,
			Note:              v.properties.Note,
			ShowNoteWhenEmpty: v.properties.NoteOnEmpty,
			XColumn:           v.properties.XCol,
			YColumn:           v.properties.YCol,
			ShadeBelow:        v.properties.Shade,
		}
	case "single_stat":
		return influxdb.SingleStatViewProperties{
			Type:              "single-stat",
			Queries:           v.properties.Queries,
			Prefix:            v.properties.Prefix,
			Suffix:            v.properties.Suffix,
			ViewColors:        v.properties.Colors,
			DecimalPlaces:     v.properties.DecimalPlaces,
			Note:              v.properties.Note,
			ShowNoteWhenEmpty: v.properties.NoteOnEmpty,
		}
	case "xy":
		return influxdb.XYViewProperties{
			Type:              "xy",
			Queries:           v.properties.Queries,
			Axes:              v.properties.Axes,
			Legend:            v.properties.Legend,
			Geom:              v.properties.Geometry,
			ViewColors:        v.properties.Colors,
			Note:              v.properties.Note,
			ShowNoteWhenEmpty: v.properties.NoteOnEmpty,
			XColumn:           v.properties.XCol,
			YColumn:           v.properties.YCol,
			ShadeBelow:        v.properties.Shade,
		}
	default:
		return nil
	}
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

func (r Resource) duration(key string) (time.Duration, bool) {
	dur, err := time.ParseDuration(r.stringShort(key))
	return dur, err == nil
}

func (r Resource) float64(key string) float64 {
	i, ok := r[key].(float64)
	if !ok {
		return 0
	}
	return i
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

func (r Resource) slcStr(key string) ([]string, bool) {
	v, ok := r[key].([]interface{})
	if !ok {
		return nil, false
	}

	out := make([]string, 0, len(v))
	for _, iface := range v {
		s, ok := iface.(string)
		if ok {
			out = append(out, s)
		}
	}
	return out, true
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

func resourceToLabel(r Resource) *Label {
	props, _ := ifaceMapToResource(r["properties"])
	return &Label{
		Name:    r.Name(),
		inherit: r.bool("inherit"),
		Properties: map[string]string{
			// are these required?
			"color":       props.stringShort("color"),
			"description": props.stringShort("description"),
		},
	}
}

func axesSuffixConvert(suf string) string {
	if strings.ToLower(suf) == "percent" {
		return "%"
	}
	return suf
}
