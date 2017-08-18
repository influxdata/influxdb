package internal

import (
	"encoding/json"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/chronograf"
)

//go:generate protoc --gogo_out=. internal.proto

// MarshalSource encodes a source to binary protobuf format.
func MarshalSource(s chronograf.Source) ([]byte, error) {
	return proto.Marshal(&Source{
		ID:                 int64(s.ID),
		Name:               s.Name,
		Type:               s.Type,
		Username:           s.Username,
		Password:           s.Password,
		SharedSecret:       s.SharedSecret,
		URL:                s.URL,
		MetaURL:            s.MetaURL,
		InsecureSkipVerify: s.InsecureSkipVerify,
		Default:            s.Default,
		Telegraf:           s.Telegraf,
	})
}

// UnmarshalSource decodes a source from binary protobuf data.
func UnmarshalSource(data []byte, s *chronograf.Source) error {
	var pb Source
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	s.ID = int(pb.ID)
	s.Name = pb.Name
	s.Type = pb.Type
	s.Username = pb.Username
	s.Password = pb.Password
	s.SharedSecret = pb.SharedSecret
	s.URL = pb.URL
	s.MetaURL = pb.MetaURL
	s.InsecureSkipVerify = pb.InsecureSkipVerify
	s.Default = pb.Default
	s.Telegraf = pb.Telegraf
	return nil
}

// MarshalServer encodes a server to binary protobuf format.
func MarshalServer(s chronograf.Server) ([]byte, error) {
	return proto.Marshal(&Server{
		ID:       int64(s.ID),
		SrcID:    int64(s.SrcID),
		Name:     s.Name,
		Username: s.Username,
		Password: s.Password,
		URL:      s.URL,
		Active:   s.Active,
	})
}

// UnmarshalServer decodes a server from binary protobuf data.
func UnmarshalServer(data []byte, s *chronograf.Server) error {
	var pb Server
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	s.ID = int(pb.ID)
	s.SrcID = int(pb.SrcID)
	s.Name = pb.Name
	s.Username = pb.Username
	s.Password = pb.Password
	s.URL = pb.URL
	s.Active = pb.Active
	return nil
}

// MarshalLayout encodes a layout to binary protobuf format.
func MarshalLayout(l chronograf.Layout) ([]byte, error) {
	cells := make([]*Cell, len(l.Cells))
	for i, c := range l.Cells {
		queries := make([]*Query, len(c.Queries))
		for j, q := range c.Queries {
			r := new(Range)
			if q.Range != nil {
				r.Upper, r.Lower = q.Range.Upper, q.Range.Lower
			}
			queries[j] = &Query{
				Command:  q.Command,
				DB:       q.DB,
				RP:       q.RP,
				GroupBys: q.GroupBys,
				Wheres:   q.Wheres,
				Label:    q.Label,
				Range:    r,
			}
		}

		axes := make(map[string]*Axis, len(c.Axes))
		for a, r := range c.Axes {
			axes[a] = &Axis{
				Bounds: r.Bounds,
				Label:  r.Label,
			}
		}

		cells[i] = &Cell{
			X:       c.X,
			Y:       c.Y,
			W:       c.W,
			H:       c.H,
			I:       c.I,
			Name:    c.Name,
			Queries: queries,
			Type:    c.Type,
			Axes:    axes,
		}
	}
	return proto.Marshal(&Layout{
		ID:          l.ID,
		Measurement: l.Measurement,
		Application: l.Application,
		Autoflow:    l.Autoflow,
		Cells:       cells,
	})
}

// UnmarshalLayout decodes a layout from binary protobuf data.
func UnmarshalLayout(data []byte, l *chronograf.Layout) error {
	var pb Layout
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	l.ID = pb.ID
	l.Measurement = pb.Measurement
	l.Application = pb.Application
	l.Autoflow = pb.Autoflow
	cells := make([]chronograf.Cell, len(pb.Cells))
	for i, c := range pb.Cells {
		queries := make([]chronograf.Query, len(c.Queries))
		for j, q := range c.Queries {
			queries[j] = chronograf.Query{
				Command:  q.Command,
				DB:       q.DB,
				RP:       q.RP,
				GroupBys: q.GroupBys,
				Wheres:   q.Wheres,
				Label:    q.Label,
			}
			if q.Range.Upper != q.Range.Lower {
				queries[j].Range = &chronograf.Range{
					Upper: q.Range.Upper,
					Lower: q.Range.Lower,
				}
			}
		}
		axes := make(map[string]chronograf.Axis, len(c.Axes))
		for a, r := range c.Axes {
			axes[a] = chronograf.Axis{
				Bounds: r.Bounds,
				Label:  r.Label,
			}
		}

		cells[i] = chronograf.Cell{
			X:       c.X,
			Y:       c.Y,
			W:       c.W,
			H:       c.H,
			I:       c.I,
			Name:    c.Name,
			Queries: queries,
			Type:    c.Type,
			Axes:    axes,
		}
	}
	l.Cells = cells
	return nil
}

// MarshalDashboard encodes a dashboard to binary protobuf format.
func MarshalDashboard(d chronograf.Dashboard) ([]byte, error) {
	cells := make([]*DashboardCell, len(d.Cells))
	for i, c := range d.Cells {
		queries := make([]*Query, len(c.Queries))
		for j, q := range c.Queries {
			r := new(Range)
			if q.Range != nil {
				r.Upper, r.Lower = q.Range.Upper, q.Range.Lower
			}
			queries[j] = &Query{
				Command: q.Command,
				Label:   q.Label,
				Range:   r,
			}
		}

		axes := make(map[string]*Axis, len(c.Axes))
		for a, r := range c.Axes {
			axes[a] = &Axis{
				Bounds: r.Bounds,
				Label:  r.Label,
				Prefix: r.Prefix,
				Suffix: r.Suffix,
				Base:   r.Base,
				Scale:  r.Scale,
			}
		}

		cells[i] = &DashboardCell{
			ID:      c.ID,
			X:       c.X,
			Y:       c.Y,
			W:       c.W,
			H:       c.H,
			Name:    c.Name,
			Queries: queries,
			Type:    c.Type,
			Axes:    axes,
		}
	}
	templates := make([]*Template, len(d.Templates))
	for i, t := range d.Templates {
		vals := make([]*TemplateValue, len(t.Values))
		for j, v := range t.Values {
			vals[j] = &TemplateValue{
				Selected: v.Selected,
				Type:     v.Type,
				Value:    v.Value,
			}
		}

		template := &Template{
			ID:      string(t.ID),
			TempVar: t.Var,
			Values:  vals,
			Type:    t.Type,
			Label:   t.Label,
		}
		if t.Query != nil {
			template.Query = &TemplateQuery{
				Command:     t.Query.Command,
				Db:          t.Query.DB,
				Rp:          t.Query.RP,
				Measurement: t.Query.Measurement,
				TagKey:      t.Query.TagKey,
				FieldKey:    t.Query.FieldKey,
			}
		}
		templates[i] = template
	}
	return proto.Marshal(&Dashboard{
		ID:        int64(d.ID),
		Cells:     cells,
		Templates: templates,
		Name:      d.Name,
	})
}

// UnmarshalDashboard decodes a layout from binary protobuf data.
func UnmarshalDashboard(data []byte, d *chronograf.Dashboard) error {
	var pb Dashboard
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	cells := make([]chronograf.DashboardCell, len(pb.Cells))
	for i, c := range pb.Cells {
		queries := make([]chronograf.DashboardQuery, len(c.Queries))
		for j, q := range c.Queries {
			queries[j] = chronograf.DashboardQuery{
				Command: q.Command,
				Label:   q.Label,
			}
			if q.Range.Upper != q.Range.Lower {
				queries[j].Range = &chronograf.Range{
					Upper: q.Range.Upper,
					Lower: q.Range.Lower,
				}
			}
		}

		axes := make(map[string]chronograf.Axis, len(c.Axes))
		for a, r := range c.Axes {
			// axis base defaults to 10
			if r.Base == "" {
				r.Base = "10"
			}

			if r.Bounds != nil {
				axes[a] = chronograf.Axis{
					Bounds: r.Bounds,
					Label:  r.Label,
					Prefix: r.Prefix,
					Suffix: r.Suffix,
					Base:   r.Base,
					Scale:  r.Scale,
				}

			} else {
				axes[a] = chronograf.Axis{
					Bounds: []string{},
					Base:   r.Base,
				}
			}
		}

		cells[i] = chronograf.DashboardCell{
			ID:      c.ID,
			X:       c.X,
			Y:       c.Y,
			W:       c.W,
			H:       c.H,
			Name:    c.Name,
			Queries: queries,
			Type:    c.Type,
			Axes:    axes,
		}
	}

	templates := make([]chronograf.Template, len(pb.Templates))
	for i, t := range pb.Templates {
		vals := make([]chronograf.BasicTemplateValue, len(t.Values))
		for j, v := range t.Values {
			vals[j] = chronograf.BasicTemplateValue{
				Selected: v.Selected,
				Type:     v.Type,
				Value:    v.Value,
			}
		}

		template := chronograf.Template{
			ID: chronograf.TemplateID(t.ID),
			BasicTemplateVar: chronograf.BasicTemplateVar{
				Var:    t.TempVar,
				Values: vals,
			},
			Type:  t.Type,
			Label: t.Label,
		}

		if t.Query != nil {
			template.Query = &chronograf.TemplateQuery{
				Command:     t.Query.Command,
				DB:          t.Query.Db,
				RP:          t.Query.Rp,
				Measurement: t.Query.Measurement,
				TagKey:      t.Query.TagKey,
				FieldKey:    t.Query.FieldKey,
			}
		}
		templates[i] = template
	}

	d.ID = chronograf.DashboardID(pb.ID)
	d.Cells = cells
	d.Templates = templates
	d.Name = pb.Name
	return nil
}

// ScopedAlert contains the source and the kapacitor id
type ScopedAlert struct {
	chronograf.AlertRule
	SrcID  int
	KapaID int
}

// MarshalAlertRule encodes an alert rule to binary protobuf format.
func MarshalAlertRule(r *ScopedAlert) ([]byte, error) {
	j, err := json.Marshal(r.AlertRule)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&AlertRule{
		ID:     r.ID,
		SrcID:  int64(r.SrcID),
		KapaID: int64(r.KapaID),
		JSON:   string(j),
	})
}

// UnmarshalAlertRule decodes an alert rule from binary protobuf data.
func UnmarshalAlertRule(data []byte, r *ScopedAlert) error {
	var pb AlertRule
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	err := json.Unmarshal([]byte(pb.JSON), &r.AlertRule)
	if err != nil {
		return err
	}
	r.SrcID = int(pb.SrcID)
	r.KapaID = int(pb.KapaID)
	return nil
}

// MarshalUser encodes a user to binary protobuf format.
// We are ignoring the password for now.
func MarshalUser(u *chronograf.User) ([]byte, error) {
	return MarshalUserPB(&User{
		Name: u.Name,
	})
}

// MarshalUserPB encodes a user to binary protobuf format.
// We are ignoring the password for now.
func MarshalUserPB(u *User) ([]byte, error) {
	return proto.Marshal(u)
}

// UnmarshalUser decodes a user from binary protobuf data.
// We are ignoring the password for now.
func UnmarshalUser(data []byte, u *chronograf.User) error {
	var pb User
	if err := UnmarshalUserPB(data, &pb); err != nil {
		return err
	}
	u.Name = pb.Name
	return nil
}

// UnmarshalUserPB decodes a user from binary protobuf data.
// We are ignoring the password for now.
func UnmarshalUserPB(data []byte, u *User) error {
	if err := proto.Unmarshal(data, u); err != nil {
		return err
	}
	return nil
}
