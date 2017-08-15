package query

import "regexp"

type Dimensions struct {
	dims []string
	m    map[string]struct{}

	wildcard bool
	filters  []*regexp.Regexp
}

func NewDimensions(sizehint int) *Dimensions {
	return &Dimensions{
		dims: make([]string, 0, sizehint),
		m:    make(map[string]struct{}, sizehint),
	}
}

func (d *Dimensions) Append(dim string) {
	if _, ok := d.m[dim]; !ok {
		d.dims = append(d.dims, dim)
		d.m[dim] = struct{}{}
	}
}

func (d *Dimensions) Wildcard() {
	d.wildcard = true
}

func (d *Dimensions) WildcardFilter(r *regexp.Regexp) {
	d.filters = append(d.filters, r)
}

func (d *Dimensions) Get() []string {
	if d == nil {
		return nil
	}
	return d.dims
}

func (d *Dimensions) Map() map[string]struct{} {
	if d == nil {
		return nil
	}
	return d.m
}

func (d *Dimensions) Len() int {
	return len(d.m)
}

func (d *Dimensions) resolveWildcard(s storage) error {
	if !d.wildcard && len(d.filters) == 0 {
		return nil
	}

	_, dimensions, err := s.FieldDimensions()
	if err != nil {
		return err
	}

	// Filter out duplicates and ones filtered by regexp.
	for dim := range dimensions {
		if _, ok := d.m[dim]; ok {
			continue
		}
		if d.wildcard {
			d.Append(dim)
		} else if len(d.filters) > 0 {
			for _, filter := range d.filters {
				if filter.MatchString(dim) {
					d.Append(dim)
					break
				}
			}
		}
	}
	return nil
}

func (d *Dimensions) Clone() *Dimensions {
	dims := make([]string, len(d.dims))
	copy(dims, d.dims)
	m := make(map[string]struct{}, len(d.m))
	for _, dim := range dims {
		m[dim] = struct{}{}
	}

	var filters []*regexp.Regexp
	if d.filters != nil {
		filters = make([]*regexp.Regexp, len(d.filters))
		copy(filters, d.filters)
	}
	return &Dimensions{
		dims:     dims,
		m:        m,
		wildcard: d.wildcard,
		filters:  filters,
	}
}
