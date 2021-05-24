package bolt

import (
	"log"
	"strings"

	"github.com/gogo/protobuf/proto"
	bolt "go.etcd.io/bbolt"
)

// changeIntervalToDuration
// Before, we supported queries that included `GROUP BY :interval:`
// After, we only support queries with `GROUP BY time(:interval:)`
// thereby allowing non_negative_derivative(_____, :interval)
var changeIntervalToDuration = Migration{
	ID:   "59b0cda4fc7909ff84ee5c4f9cb4b655b6a26620",
	Up:   up,
	Down: down,
}

func updateDashboard(board *Dashboard) {
	for _, cell := range board.Cells {
		for _, query := range cell.Queries {
			query.Command = strings.Replace(query.Command, ":interval:", "time(:interval:)", -1)
		}
	}
}

var up = func(db *bolt.DB) error {
	// For each dashboard
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(dashboardBucket)
		err := bucket.ForEach(func(id, data []byte) error {
			board := &Dashboard{}

			err := proto.Unmarshal(data, board)
			if err != nil {
				log.Fatal("unmarshalling error: ", err)
			}

			// Migrate the dashboard
			updateDashboard(board)

			data, err = proto.Marshal(board)
			if err != nil {
				log.Fatal("marshaling error: ", err)
			}

			err = bucket.Put(id, data)
			if err != nil {
				log.Fatal("error updating dashboard: ", err)
			}

			return nil
		})

		if err != nil {
			log.Fatal("error updating dashboards: ", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

var down = func(db *bolt.DB) error {
	return nil
}

/*
	Import protobuf types and bucket names that are pertinent to this migration.
	This isolates the migration from the codebase, and prevents a future change
	to a type definition from invalidating the migration functions.
*/
var dashboardBucket = []byte("Dashoard") // N.B. leave the misspelling for backwards-compat!

type Source struct {
	ID                 int64  `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Name               string `protobuf:"bytes,2,opt,name=Name,proto3" json:"Name,omitempty"`
	Type               string `protobuf:"bytes,3,opt,name=Type,proto3" json:"Type,omitempty"`
	Username           string `protobuf:"bytes,4,opt,name=Username,proto3" json:"Username,omitempty"`
	Password           string `protobuf:"bytes,5,opt,name=Password,proto3" json:"Password,omitempty"`
	URL                string `protobuf:"bytes,6,opt,name=URL,proto3" json:"URL,omitempty"`
	Default            bool   `protobuf:"varint,7,opt,name=Default,proto3" json:"Default,omitempty"`
	Telegraf           string `protobuf:"bytes,8,opt,name=Telegraf,proto3" json:"Telegraf,omitempty"`
	InsecureSkipVerify bool   `protobuf:"varint,9,opt,name=InsecureSkipVerify,proto3" json:"InsecureSkipVerify,omitempty"`
	MetaURL            string `protobuf:"bytes,10,opt,name=MetaURL,proto3" json:"MetaURL,omitempty"`
	SharedSecret       string `protobuf:"bytes,11,opt,name=SharedSecret,proto3" json:"SharedSecret,omitempty"`
	Organization       string `protobuf:"bytes,12,opt,name=Organization,proto3" json:"Organization,omitempty"`
	Role               string `protobuf:"bytes,13,opt,name=Role,proto3" json:"Role,omitempty"`
}

func (m *Source) Reset()                    { *m = Source{} }
func (m *Source) String() string            { return proto.CompactTextString(m) }
func (*Source) ProtoMessage()               {}
func (*Source) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{0} }

func (m *Source) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *Source) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Source) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *Source) GetUsername() string {
	if m != nil {
		return m.Username
	}
	return ""
}

func (m *Source) GetPassword() string {
	if m != nil {
		return m.Password
	}
	return ""
}

func (m *Source) GetURL() string {
	if m != nil {
		return m.URL
	}
	return ""
}

func (m *Source) GetDefault() bool {
	if m != nil {
		return m.Default
	}
	return false
}

func (m *Source) GetTelegraf() string {
	if m != nil {
		return m.Telegraf
	}
	return ""
}

func (m *Source) GetInsecureSkipVerify() bool {
	if m != nil {
		return m.InsecureSkipVerify
	}
	return false
}

func (m *Source) GetMetaURL() string {
	if m != nil {
		return m.MetaURL
	}
	return ""
}

func (m *Source) GetSharedSecret() string {
	if m != nil {
		return m.SharedSecret
	}
	return ""
}

func (m *Source) GetOrganization() string {
	if m != nil {
		return m.Organization
	}
	return ""
}

func (m *Source) GetRole() string {
	if m != nil {
		return m.Role
	}
	return ""
}

type Dashboard struct {
	ID           int64            `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Name         string           `protobuf:"bytes,2,opt,name=Name,proto3" json:"Name,omitempty"`
	Cells        []*DashboardCell `protobuf:"bytes,3,rep,name=cells" json:"cells,omitempty"`
	Templates    []*Template      `protobuf:"bytes,4,rep,name=templates" json:"templates,omitempty"`
	Organization string           `protobuf:"bytes,5,opt,name=Organization,proto3" json:"Organization,omitempty"`
}

func (m *Dashboard) Reset()                    { *m = Dashboard{} }
func (m *Dashboard) String() string            { return proto.CompactTextString(m) }
func (*Dashboard) ProtoMessage()               {}
func (*Dashboard) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{1} }

func (m *Dashboard) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *Dashboard) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Dashboard) GetCells() []*DashboardCell {
	if m != nil {
		return m.Cells
	}
	return nil
}

func (m *Dashboard) GetTemplates() []*Template {
	if m != nil {
		return m.Templates
	}
	return nil
}

func (m *Dashboard) GetOrganization() string {
	if m != nil {
		return m.Organization
	}
	return ""
}

type DashboardCell struct {
	X            int32            `protobuf:"varint,1,opt,name=x,proto3" json:"x,omitempty"`
	Y            int32            `protobuf:"varint,2,opt,name=y,proto3" json:"y,omitempty"`
	W            int32            `protobuf:"varint,3,opt,name=w,proto3" json:"w,omitempty"`
	H            int32            `protobuf:"varint,4,opt,name=h,proto3" json:"h,omitempty"`
	Queries      []*Query         `protobuf:"bytes,5,rep,name=queries" json:"queries,omitempty"`
	Name         string           `protobuf:"bytes,6,opt,name=name,proto3" json:"name,omitempty"`
	Type         string           `protobuf:"bytes,7,opt,name=type,proto3" json:"type,omitempty"`
	ID           string           `protobuf:"bytes,8,opt,name=ID,proto3" json:"ID,omitempty"`
	Axes         map[string]*Axis `protobuf:"bytes,9,rep,name=axes" json:"axes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value"`
	Colors       []*Color         `protobuf:"bytes,10,rep,name=colors" json:"colors,omitempty"`
	TableOptions *TableOptions    `protobuf:"bytes,12,opt,name=tableOptions" json:"tableOptions,omitempty"`
}

func (m *DashboardCell) Reset()                    { *m = DashboardCell{} }
func (m *DashboardCell) String() string            { return proto.CompactTextString(m) }
func (*DashboardCell) ProtoMessage()               {}
func (*DashboardCell) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{2} }

func (m *DashboardCell) GetX() int32 {
	if m != nil {
		return m.X
	}
	return 0
}

func (m *DashboardCell) GetY() int32 {
	if m != nil {
		return m.Y
	}
	return 0
}

func (m *DashboardCell) GetW() int32 {
	if m != nil {
		return m.W
	}
	return 0
}

func (m *DashboardCell) GetH() int32 {
	if m != nil {
		return m.H
	}
	return 0
}

func (m *DashboardCell) GetQueries() []*Query {
	if m != nil {
		return m.Queries
	}
	return nil
}

func (m *DashboardCell) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *DashboardCell) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *DashboardCell) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *DashboardCell) GetAxes() map[string]*Axis {
	if m != nil {
		return m.Axes
	}
	return nil
}

func (m *DashboardCell) GetColors() []*Color {
	if m != nil {
		return m.Colors
	}
	return nil
}

func (m *DashboardCell) GetTableOptions() *TableOptions {
	if m != nil {
		return m.TableOptions
	}
	return nil
}

type TableOptions struct {
	VerticalTimeAxis bool              `protobuf:"varint,2,opt,name=verticalTimeAxis,proto3" json:"verticalTimeAxis,omitempty"`
	SortBy           *RenamableField   `protobuf:"bytes,3,opt,name=sortBy" json:"sortBy,omitempty"`
	Wrapping         string            `protobuf:"bytes,4,opt,name=wrapping,proto3" json:"wrapping,omitempty"`
	FieldNames       []*RenamableField `protobuf:"bytes,5,rep,name=fieldNames" json:"fieldNames,omitempty"`
	FixFirstColumn   bool              `protobuf:"varint,6,opt,name=fixFirstColumn,proto3" json:"fixFirstColumn,omitempty"`
}

func (m *TableOptions) Reset()                    { *m = TableOptions{} }
func (m *TableOptions) String() string            { return proto.CompactTextString(m) }
func (*TableOptions) ProtoMessage()               {}
func (*TableOptions) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{3} }

func (m *TableOptions) GetVerticalTimeAxis() bool {
	if m != nil {
		return m.VerticalTimeAxis
	}
	return false
}

func (m *TableOptions) GetSortBy() *RenamableField {
	if m != nil {
		return m.SortBy
	}
	return nil
}

func (m *TableOptions) GetWrapping() string {
	if m != nil {
		return m.Wrapping
	}
	return ""
}

func (m *TableOptions) GetFieldNames() []*RenamableField {
	if m != nil {
		return m.FieldNames
	}
	return nil
}

func (m *TableOptions) GetFixFirstColumn() bool {
	if m != nil {
		return m.FixFirstColumn
	}
	return false
}

type RenamableField struct {
	InternalName string `protobuf:"bytes,1,opt,name=internalName,proto3" json:"internalName,omitempty"`
	DisplayName  string `protobuf:"bytes,2,opt,name=displayName,proto3" json:"displayName,omitempty"`
	Visible      bool   `protobuf:"varint,3,opt,name=visible,proto3" json:"visible,omitempty"`
}

func (m *RenamableField) Reset()                    { *m = RenamableField{} }
func (m *RenamableField) String() string            { return proto.CompactTextString(m) }
func (*RenamableField) ProtoMessage()               {}
func (*RenamableField) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{4} }

func (m *RenamableField) GetInternalName() string {
	if m != nil {
		return m.InternalName
	}
	return ""
}

func (m *RenamableField) GetDisplayName() string {
	if m != nil {
		return m.DisplayName
	}
	return ""
}

func (m *RenamableField) GetVisible() bool {
	if m != nil {
		return m.Visible
	}
	return false
}

type Color struct {
	ID    string `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Type  string `protobuf:"bytes,2,opt,name=Type,proto3" json:"Type,omitempty"`
	Hex   string `protobuf:"bytes,3,opt,name=Hex,proto3" json:"Hex,omitempty"`
	Name  string `protobuf:"bytes,4,opt,name=Name,proto3" json:"Name,omitempty"`
	Value string `protobuf:"bytes,5,opt,name=Value,proto3" json:"Value,omitempty"`
}

func (m *Color) Reset()                    { *m = Color{} }
func (m *Color) String() string            { return proto.CompactTextString(m) }
func (*Color) ProtoMessage()               {}
func (*Color) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{5} }

func (m *Color) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *Color) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *Color) GetHex() string {
	if m != nil {
		return m.Hex
	}
	return ""
}

func (m *Color) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Color) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type Axis struct {
	LegacyBounds []int64  `protobuf:"varint,1,rep,packed,name=legacyBounds" json:"legacyBounds,omitempty"`
	Bounds       []string `protobuf:"bytes,2,rep,name=bounds" json:"bounds,omitempty"`
	Label        string   `protobuf:"bytes,3,opt,name=label,proto3" json:"label,omitempty"`
	Prefix       string   `protobuf:"bytes,4,opt,name=prefix,proto3" json:"prefix,omitempty"`
	Suffix       string   `protobuf:"bytes,5,opt,name=suffix,proto3" json:"suffix,omitempty"`
	Base         string   `protobuf:"bytes,6,opt,name=base,proto3" json:"base,omitempty"`
	Scale        string   `protobuf:"bytes,7,opt,name=scale,proto3" json:"scale,omitempty"`
}

func (m *Axis) Reset()                    { *m = Axis{} }
func (m *Axis) String() string            { return proto.CompactTextString(m) }
func (*Axis) ProtoMessage()               {}
func (*Axis) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{7} }

func (m *Axis) GetLegacyBounds() []int64 {
	if m != nil {
		return m.LegacyBounds
	}
	return nil
}

func (m *Axis) GetBounds() []string {
	if m != nil {
		return m.Bounds
	}
	return nil
}

func (m *Axis) GetLabel() string {
	if m != nil {
		return m.Label
	}
	return ""
}

func (m *Axis) GetPrefix() string {
	if m != nil {
		return m.Prefix
	}
	return ""
}

func (m *Axis) GetSuffix() string {
	if m != nil {
		return m.Suffix
	}
	return ""
}

func (m *Axis) GetBase() string {
	if m != nil {
		return m.Base
	}
	return ""
}

func (m *Axis) GetScale() string {
	if m != nil {
		return m.Scale
	}
	return ""
}

type Template struct {
	ID      string           `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	TempVar string           `protobuf:"bytes,2,opt,name=temp_var,json=tempVar,proto3" json:"temp_var,omitempty"`
	Values  []*TemplateValue `protobuf:"bytes,3,rep,name=values" json:"values,omitempty"`
	Type    string           `protobuf:"bytes,4,opt,name=type,proto3" json:"type,omitempty"`
	Label   string           `protobuf:"bytes,5,opt,name=label,proto3" json:"label,omitempty"`
	Query   *TemplateQuery   `protobuf:"bytes,6,opt,name=query" json:"query,omitempty"`
}

func (m *Template) Reset()                    { *m = Template{} }
func (m *Template) String() string            { return proto.CompactTextString(m) }
func (*Template) ProtoMessage()               {}
func (*Template) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{8} }

func (m *Template) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *Template) GetTempVar() string {
	if m != nil {
		return m.TempVar
	}
	return ""
}

func (m *Template) GetValues() []*TemplateValue {
	if m != nil {
		return m.Values
	}
	return nil
}

func (m *Template) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *Template) GetLabel() string {
	if m != nil {
		return m.Label
	}
	return ""
}

func (m *Template) GetQuery() *TemplateQuery {
	if m != nil {
		return m.Query
	}
	return nil
}

type TemplateValue struct {
	Type     string `protobuf:"bytes,1,opt,name=type,proto3" json:"type,omitempty"`
	Value    string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Selected bool   `protobuf:"varint,3,opt,name=selected,proto3" json:"selected,omitempty"`
}

func (m *TemplateValue) Reset()                    { *m = TemplateValue{} }
func (m *TemplateValue) String() string            { return proto.CompactTextString(m) }
func (*TemplateValue) ProtoMessage()               {}
func (*TemplateValue) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{9} }

func (m *TemplateValue) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *TemplateValue) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

func (m *TemplateValue) GetSelected() bool {
	if m != nil {
		return m.Selected
	}
	return false
}

type TemplateQuery struct {
	Command     string `protobuf:"bytes,1,opt,name=command,proto3" json:"command,omitempty"`
	Db          string `protobuf:"bytes,2,opt,name=db,proto3" json:"db,omitempty"`
	Rp          string `protobuf:"bytes,3,opt,name=rp,proto3" json:"rp,omitempty"`
	Measurement string `protobuf:"bytes,4,opt,name=measurement,proto3" json:"measurement,omitempty"`
	TagKey      string `protobuf:"bytes,5,opt,name=tag_key,json=tagKey,proto3" json:"tag_key,omitempty"`
	FieldKey    string `protobuf:"bytes,6,opt,name=field_key,json=fieldKey,proto3" json:"field_key,omitempty"`
}

func (m *TemplateQuery) Reset()                    { *m = TemplateQuery{} }
func (m *TemplateQuery) String() string            { return proto.CompactTextString(m) }
func (*TemplateQuery) ProtoMessage()               {}
func (*TemplateQuery) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{10} }

func (m *TemplateQuery) GetCommand() string {
	if m != nil {
		return m.Command
	}
	return ""
}

func (m *TemplateQuery) GetDb() string {
	if m != nil {
		return m.Db
	}
	return ""
}

func (m *TemplateQuery) GetRp() string {
	if m != nil {
		return m.Rp
	}
	return ""
}

func (m *TemplateQuery) GetMeasurement() string {
	if m != nil {
		return m.Measurement
	}
	return ""
}

func (m *TemplateQuery) GetTagKey() string {
	if m != nil {
		return m.TagKey
	}
	return ""
}

func (m *TemplateQuery) GetFieldKey() string {
	if m != nil {
		return m.FieldKey
	}
	return ""
}

type Server struct {
	ID                 int64  `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Name               string `protobuf:"bytes,2,opt,name=Name,proto3" json:"Name,omitempty"`
	Username           string `protobuf:"bytes,3,opt,name=Username,proto3" json:"Username,omitempty"`
	Password           string `protobuf:"bytes,4,opt,name=Password,proto3" json:"Password,omitempty"`
	URL                string `protobuf:"bytes,5,opt,name=URL,proto3" json:"URL,omitempty"`
	SrcID              int64  `protobuf:"varint,6,opt,name=SrcID,proto3" json:"SrcID,omitempty"`
	Active             bool   `protobuf:"varint,7,opt,name=Active,proto3" json:"Active,omitempty"`
	Organization       string `protobuf:"bytes,8,opt,name=Organization,proto3" json:"Organization,omitempty"`
	InsecureSkipVerify bool   `protobuf:"varint,9,opt,name=InsecureSkipVerify,proto3" json:"InsecureSkipVerify,omitempty"`
}

func (m *Server) Reset()                    { *m = Server{} }
func (m *Server) String() string            { return proto.CompactTextString(m) }
func (*Server) ProtoMessage()               {}
func (*Server) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{11} }

func (m *Server) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *Server) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Server) GetUsername() string {
	if m != nil {
		return m.Username
	}
	return ""
}

func (m *Server) GetPassword() string {
	if m != nil {
		return m.Password
	}
	return ""
}

func (m *Server) GetURL() string {
	if m != nil {
		return m.URL
	}
	return ""
}

func (m *Server) GetSrcID() int64 {
	if m != nil {
		return m.SrcID
	}
	return 0
}

func (m *Server) GetActive() bool {
	if m != nil {
		return m.Active
	}
	return false
}

func (m *Server) GetOrganization() string {
	if m != nil {
		return m.Organization
	}
	return ""
}

func (m *Server) GetInsecureSkipVerify() bool {
	if m != nil {
		return m.InsecureSkipVerify
	}
	return false
}

type Layout struct {
	ID          string  `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Application string  `protobuf:"bytes,2,opt,name=Application,proto3" json:"Application,omitempty"`
	Measurement string  `protobuf:"bytes,3,opt,name=Measurement,proto3" json:"Measurement,omitempty"`
	Cells       []*Cell `protobuf:"bytes,4,rep,name=Cells" json:"Cells,omitempty"`
	Autoflow    bool    `protobuf:"varint,5,opt,name=Autoflow,proto3" json:"Autoflow,omitempty"`
}

func (m *Layout) Reset()                    { *m = Layout{} }
func (m *Layout) String() string            { return proto.CompactTextString(m) }
func (*Layout) ProtoMessage()               {}
func (*Layout) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{12} }

func (m *Layout) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *Layout) GetApplication() string {
	if m != nil {
		return m.Application
	}
	return ""
}

func (m *Layout) GetMeasurement() string {
	if m != nil {
		return m.Measurement
	}
	return ""
}

func (m *Layout) GetCells() []*Cell {
	if m != nil {
		return m.Cells
	}
	return nil
}

func (m *Layout) GetAutoflow() bool {
	if m != nil {
		return m.Autoflow
	}
	return false
}

type Cell struct {
	X       int32            `protobuf:"varint,1,opt,name=x,proto3" json:"x,omitempty"`
	Y       int32            `protobuf:"varint,2,opt,name=y,proto3" json:"y,omitempty"`
	W       int32            `protobuf:"varint,3,opt,name=w,proto3" json:"w,omitempty"`
	H       int32            `protobuf:"varint,4,opt,name=h,proto3" json:"h,omitempty"`
	Queries []*Query         `protobuf:"bytes,5,rep,name=queries" json:"queries,omitempty"`
	I       string           `protobuf:"bytes,6,opt,name=i,proto3" json:"i,omitempty"`
	Name    string           `protobuf:"bytes,7,opt,name=name,proto3" json:"name,omitempty"`
	Yranges []int64          `protobuf:"varint,8,rep,packed,name=yranges" json:"yranges,omitempty"`
	Ylabels []string         `protobuf:"bytes,9,rep,name=ylabels" json:"ylabels,omitempty"`
	Type    string           `protobuf:"bytes,10,opt,name=type,proto3" json:"type,omitempty"`
	Axes    map[string]*Axis `protobuf:"bytes,11,rep,name=axes" json:"axes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value"`
}

func (m *Cell) Reset()                    { *m = Cell{} }
func (m *Cell) String() string            { return proto.CompactTextString(m) }
func (*Cell) ProtoMessage()               {}
func (*Cell) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{13} }

func (m *Cell) GetX() int32 {
	if m != nil {
		return m.X
	}
	return 0
}

func (m *Cell) GetY() int32 {
	if m != nil {
		return m.Y
	}
	return 0
}

func (m *Cell) GetW() int32 {
	if m != nil {
		return m.W
	}
	return 0
}

func (m *Cell) GetH() int32 {
	if m != nil {
		return m.H
	}
	return 0
}

func (m *Cell) GetQueries() []*Query {
	if m != nil {
		return m.Queries
	}
	return nil
}

func (m *Cell) GetI() string {
	if m != nil {
		return m.I
	}
	return ""
}

func (m *Cell) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Cell) GetYranges() []int64 {
	if m != nil {
		return m.Yranges
	}
	return nil
}

func (m *Cell) GetYlabels() []string {
	if m != nil {
		return m.Ylabels
	}
	return nil
}

func (m *Cell) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *Cell) GetAxes() map[string]*Axis {
	if m != nil {
		return m.Axes
	}
	return nil
}

type Query struct {
	Command  string       `protobuf:"bytes,1,opt,name=Command,proto3" json:"Command,omitempty"`
	DB       string       `protobuf:"bytes,2,opt,name=DB,proto3" json:"DB,omitempty"`
	RP       string       `protobuf:"bytes,3,opt,name=RP,proto3" json:"RP,omitempty"`
	GroupBys []string     `protobuf:"bytes,4,rep,name=GroupBys" json:"GroupBys,omitempty"`
	Wheres   []string     `protobuf:"bytes,5,rep,name=Wheres" json:"Wheres,omitempty"`
	Label    string       `protobuf:"bytes,6,opt,name=Label,proto3" json:"Label,omitempty"`
	Range    *Range       `protobuf:"bytes,7,opt,name=Range" json:"Range,omitempty"`
	Source   string       `protobuf:"bytes,8,opt,name=Source,proto3" json:"Source,omitempty"`
	Shifts   []*TimeShift `protobuf:"bytes,9,rep,name=Shifts" json:"Shifts,omitempty"`
}

func (m *Query) Reset()                    { *m = Query{} }
func (m *Query) String() string            { return proto.CompactTextString(m) }
func (*Query) ProtoMessage()               {}
func (*Query) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{14} }

func (m *Query) GetCommand() string {
	if m != nil {
		return m.Command
	}
	return ""
}

func (m *Query) GetDB() string {
	if m != nil {
		return m.DB
	}
	return ""
}

func (m *Query) GetRP() string {
	if m != nil {
		return m.RP
	}
	return ""
}

func (m *Query) GetGroupBys() []string {
	if m != nil {
		return m.GroupBys
	}
	return nil
}

func (m *Query) GetWheres() []string {
	if m != nil {
		return m.Wheres
	}
	return nil
}

func (m *Query) GetLabel() string {
	if m != nil {
		return m.Label
	}
	return ""
}

func (m *Query) GetRange() *Range {
	if m != nil {
		return m.Range
	}
	return nil
}

func (m *Query) GetSource() string {
	if m != nil {
		return m.Source
	}
	return ""
}

func (m *Query) GetShifts() []*TimeShift {
	if m != nil {
		return m.Shifts
	}
	return nil
}

type TimeShift struct {
	Label    string `protobuf:"bytes,1,opt,name=Label,proto3" json:"Label,omitempty"`
	Unit     string `protobuf:"bytes,2,opt,name=Unit,proto3" json:"Unit,omitempty"`
	Quantity string `protobuf:"bytes,3,opt,name=Quantity,proto3" json:"Quantity,omitempty"`
}

func (m *TimeShift) Reset()                    { *m = TimeShift{} }
func (m *TimeShift) String() string            { return proto.CompactTextString(m) }
func (*TimeShift) ProtoMessage()               {}
func (*TimeShift) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{15} }

func (m *TimeShift) GetLabel() string {
	if m != nil {
		return m.Label
	}
	return ""
}

func (m *TimeShift) GetUnit() string {
	if m != nil {
		return m.Unit
	}
	return ""
}

func (m *TimeShift) GetQuantity() string {
	if m != nil {
		return m.Quantity
	}
	return ""
}

type Range struct {
	Upper int64 `protobuf:"varint,1,opt,name=Upper,proto3" json:"Upper,omitempty"`
	Lower int64 `protobuf:"varint,2,opt,name=Lower,proto3" json:"Lower,omitempty"`
}

func (m *Range) Reset()                    { *m = Range{} }
func (m *Range) String() string            { return proto.CompactTextString(m) }
func (*Range) ProtoMessage()               {}
func (*Range) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{16} }

func (m *Range) GetUpper() int64 {
	if m != nil {
		return m.Upper
	}
	return 0
}

func (m *Range) GetLower() int64 {
	if m != nil {
		return m.Lower
	}
	return 0
}

type AlertRule struct {
	ID     string `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	JSON   string `protobuf:"bytes,2,opt,name=JSON,proto3" json:"JSON,omitempty"`
	SrcID  int64  `protobuf:"varint,3,opt,name=SrcID,proto3" json:"SrcID,omitempty"`
	KapaID int64  `protobuf:"varint,4,opt,name=KapaID,proto3" json:"KapaID,omitempty"`
}

func (m *AlertRule) Reset()                    { *m = AlertRule{} }
func (m *AlertRule) String() string            { return proto.CompactTextString(m) }
func (*AlertRule) ProtoMessage()               {}
func (*AlertRule) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{17} }

func (m *AlertRule) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *AlertRule) GetJSON() string {
	if m != nil {
		return m.JSON
	}
	return ""
}

func (m *AlertRule) GetSrcID() int64 {
	if m != nil {
		return m.SrcID
	}
	return 0
}

func (m *AlertRule) GetKapaID() int64 {
	if m != nil {
		return m.KapaID
	}
	return 0
}

type User struct {
	ID         uint64  `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Name       string  `protobuf:"bytes,2,opt,name=Name,proto3" json:"Name,omitempty"`
	Provider   string  `protobuf:"bytes,3,opt,name=Provider,proto3" json:"Provider,omitempty"`
	Scheme     string  `protobuf:"bytes,4,opt,name=Scheme,proto3" json:"Scheme,omitempty"`
	Roles      []*Role `protobuf:"bytes,5,rep,name=Roles" json:"Roles,omitempty"`
	SuperAdmin bool    `protobuf:"varint,6,opt,name=SuperAdmin,proto3" json:"SuperAdmin,omitempty"`
}

func (m *User) Reset()                    { *m = User{} }
func (m *User) String() string            { return proto.CompactTextString(m) }
func (*User) ProtoMessage()               {}
func (*User) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{18} }

func (m *User) GetID() uint64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *User) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *User) GetProvider() string {
	if m != nil {
		return m.Provider
	}
	return ""
}

func (m *User) GetScheme() string {
	if m != nil {
		return m.Scheme
	}
	return ""
}

func (m *User) GetRoles() []*Role {
	if m != nil {
		return m.Roles
	}
	return nil
}

func (m *User) GetSuperAdmin() bool {
	if m != nil {
		return m.SuperAdmin
	}
	return false
}

type Role struct {
	Organization string `protobuf:"bytes,1,opt,name=Organization,proto3" json:"Organization,omitempty"`
	Name         string `protobuf:"bytes,2,opt,name=Name,proto3" json:"Name,omitempty"`
}

func (m *Role) Reset()                    { *m = Role{} }
func (m *Role) String() string            { return proto.CompactTextString(m) }
func (*Role) ProtoMessage()               {}
func (*Role) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{19} }

func (m *Role) GetOrganization() string {
	if m != nil {
		return m.Organization
	}
	return ""
}

func (m *Role) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type Mapping struct {
	Provider             string `protobuf:"bytes,1,opt,name=Provider,proto3" json:"Provider,omitempty"`
	Scheme               string `protobuf:"bytes,2,opt,name=Scheme,proto3" json:"Scheme,omitempty"`
	ProviderOrganization string `protobuf:"bytes,3,opt,name=ProviderOrganization,proto3" json:"ProviderOrganization,omitempty"`
	ID                   string `protobuf:"bytes,4,opt,name=ID,proto3" json:"ID,omitempty"`
	Organization         string `protobuf:"bytes,5,opt,name=Organization,proto3" json:"Organization,omitempty"`
}

func (m *Mapping) Reset()                    { *m = Mapping{} }
func (m *Mapping) String() string            { return proto.CompactTextString(m) }
func (*Mapping) ProtoMessage()               {}
func (*Mapping) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{20} }

func (m *Mapping) GetProvider() string {
	if m != nil {
		return m.Provider
	}
	return ""
}

func (m *Mapping) GetScheme() string {
	if m != nil {
		return m.Scheme
	}
	return ""
}

func (m *Mapping) GetProviderOrganization() string {
	if m != nil {
		return m.ProviderOrganization
	}
	return ""
}

func (m *Mapping) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *Mapping) GetOrganization() string {
	if m != nil {
		return m.Organization
	}
	return ""
}

type Organization struct {
	ID          string `protobuf:"bytes,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Name        string `protobuf:"bytes,2,opt,name=Name,proto3" json:"Name,omitempty"`
	DefaultRole string `protobuf:"bytes,3,opt,name=DefaultRole,proto3" json:"DefaultRole,omitempty"`
}

func (m *Organization) Reset()                    { *m = Organization{} }
func (m *Organization) String() string            { return proto.CompactTextString(m) }
func (*Organization) ProtoMessage()               {}
func (*Organization) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{21} }

func (m *Organization) GetID() string {
	if m != nil {
		return m.ID
	}
	return ""
}

func (m *Organization) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Organization) GetDefaultRole() string {
	if m != nil {
		return m.DefaultRole
	}
	return ""
}

type Config struct {
	Auth *AuthConfig `protobuf:"bytes,1,opt,name=Auth" json:"Auth,omitempty"`
}

func (m *Config) Reset()                    { *m = Config{} }
func (m *Config) String() string            { return proto.CompactTextString(m) }
func (*Config) ProtoMessage()               {}
func (*Config) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{22} }

func (m *Config) GetAuth() *AuthConfig {
	if m != nil {
		return m.Auth
	}
	return nil
}

type AuthConfig struct {
	SuperAdminNewUsers bool `protobuf:"varint,1,opt,name=SuperAdminNewUsers,proto3" json:"SuperAdminNewUsers,omitempty"`
}

func (m *AuthConfig) Reset()                    { *m = AuthConfig{} }
func (m *AuthConfig) String() string            { return proto.CompactTextString(m) }
func (*AuthConfig) ProtoMessage()               {}
func (*AuthConfig) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{23} }

func (m *AuthConfig) GetSuperAdminNewUsers() bool {
	if m != nil {
		return m.SuperAdminNewUsers
	}
	return false
}

type BuildInfo struct {
	Version string `protobuf:"bytes,1,opt,name=Version,proto3" json:"Version,omitempty"`
	Commit  string `protobuf:"bytes,2,opt,name=Commit,proto3" json:"Commit,omitempty"`
}

func (m *BuildInfo) Reset()                    { *m = BuildInfo{} }
func (m *BuildInfo) String() string            { return proto.CompactTextString(m) }
func (*BuildInfo) ProtoMessage()               {}
func (*BuildInfo) Descriptor() ([]byte, []int) { return fileDescriptorInternal, []int{24} }

func (m *BuildInfo) GetVersion() string {
	if m != nil {
		return m.Version
	}
	return ""
}

func (m *BuildInfo) GetCommit() string {
	if m != nil {
		return m.Commit
	}
	return ""
}

var fileDescriptorInternal = []byte{
	// 1586 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xbc, 0x57, 0x5f, 0x8f, 0xdb, 0x44,
	0x10, 0x97, 0x93, 0x38, 0x89, 0x27, 0xd7, 0xe3, 0x64, 0x4e, 0xad, 0x29, 0x12, 0x0a, 0x16, 0x7f,
	0xc2, 0x9f, 0x1e, 0x55, 0x2a, 0xa4, 0xaa, 0x82, 0x4a, 0xb9, 0x0b, 0x2d, 0x47, 0xaf, 0xbd, 0xeb,
	0xe6, 0xee, 0x78, 0x42, 0xd5, 0x26, 0x99, 0x24, 0x56, 0x1d, 0xdb, 0xac, 0xed, 0xbb, 0x98, 0x8f,
	0xc0, 0x87, 0x40, 0x42, 0x82, 0x2f, 0x80, 0x78, 0xe1, 0x89, 0x77, 0x3e, 0x08, 0x5f, 0x01, 0x1e,
	0xd1, 0xec, 0xae, 0x1d, 0xe7, 0x92, 0x56, 0x45, 0x42, 0xbc, 0xed, 0x6f, 0x66, 0x3c, 0xbb, 0xf3,
	0x7f, 0x0c, 0xdb, 0x5e, 0x90, 0xa0, 0x08, 0xb8, 0xbf, 0x17, 0x89, 0x30, 0x09, 0xed, 0x66, 0x8e,
	0xdd, 0x3f, 0x2b, 0x50, 0x1f, 0x84, 0xa9, 0x18, 0xa1, 0xbd, 0x0d, 0x95, 0xc3, 0xbe, 0x63, 0xb4,
	0x8d, 0x4e, 0x95, 0x55, 0x0e, 0xfb, 0xb6, 0x0d, 0xb5, 0x27, 0x7c, 0x8e, 0x4e, 0xa5, 0x6d, 0x74,
	0x2c, 0x26, 0xcf, 0x44, 0x3b, 0xcd, 0x22, 0x74, 0xaa, 0x8a, 0x46, 0x67, 0xfb, 0x26, 0x34, 0xcf,
	0x62, 0xd2, 0x36, 0x47, 0xa7, 0x26, 0xe9, 0x05, 0x26, 0xde, 0x09, 0x8f, 0xe3, 0xcb, 0x50, 0x8c,
	0x1d, 0x53, 0xf1, 0x72, 0x6c, 0xef, 0x40, 0xf5, 0x8c, 0x1d, 0x39, 0x75, 0x49, 0xa6, 0xa3, 0xed,
	0x40, 0xa3, 0x8f, 0x13, 0x9e, 0xfa, 0x89, 0xd3, 0x68, 0x1b, 0x9d, 0x26, 0xcb, 0x21, 0xe9, 0x39,
	0x45, 0x1f, 0xa7, 0x82, 0x4f, 0x9c, 0xa6, 0xd2, 0x93, 0x63, 0x7b, 0x0f, 0xec, 0xc3, 0x20, 0xc6,
	0x51, 0x2a, 0x70, 0xf0, 0xdc, 0x8b, 0xce, 0x51, 0x78, 0x93, 0xcc, 0xb1, 0xa4, 0x82, 0x0d, 0x1c,
	0xba, 0xe5, 0x31, 0x26, 0x9c, 0xee, 0x06, 0xa9, 0x2a, 0x87, 0xb6, 0x0b, 0x5b, 0x83, 0x19, 0x17,
	0x38, 0x1e, 0xe0, 0x48, 0x60, 0xe2, 0xb4, 0x24, 0x7b, 0x85, 0x46, 0x32, 0xc7, 0x62, 0xca, 0x03,
	0xef, 0x3b, 0x9e, 0x78, 0x61, 0xe0, 0x6c, 0x29, 0x99, 0x32, 0x8d, 0xbc, 0xc4, 0x42, 0x1f, 0x9d,
	0x6b, 0xca, 0x4b, 0x74, 0x76, 0x7f, 0x35, 0xc0, 0xea, 0xf3, 0x78, 0x36, 0x0c, 0xb9, 0x18, 0xbf,
	0x92, 0xaf, 0x6f, 0x81, 0x39, 0x42, 0xdf, 0x8f, 0x9d, 0x6a, 0xbb, 0xda, 0x69, 0x75, 0x6f, 0xec,
	0x15, 0x41, 0x2c, 0xf4, 0x1c, 0xa0, 0xef, 0x33, 0x25, 0x65, 0xdf, 0x06, 0x2b, 0xc1, 0x79, 0xe4,
	0xf3, 0x04, 0x63, 0xa7, 0x26, 0x3f, 0xb1, 0x97, 0x9f, 0x9c, 0x6a, 0x16, 0x5b, 0x0a, 0xad, 0x99,
	0x62, 0xae, 0x9b, 0xe2, 0xfe, 0x56, 0x85, 0x6b, 0x2b, 0xd7, 0xd9, 0x5b, 0x60, 0x2c, 0xe4, 0xcb,
	0x4d, 0x66, 0x2c, 0x08, 0x65, 0xf2, 0xd5, 0x26, 0x33, 0x32, 0x42, 0x97, 0x32, 0x37, 0x4c, 0x66,
	0x5c, 0x12, 0x9a, 0xc9, 0x8c, 0x30, 0x99, 0x31, 0xb3, 0x3f, 0x80, 0xc6, 0xb7, 0x29, 0x0a, 0x0f,
	0x63, 0xc7, 0x94, 0xaf, 0x7b, 0x6d, 0xf9, 0xba, 0xa7, 0x29, 0x8a, 0x8c, 0xe5, 0x7c, 0xf2, 0x86,
	0xcc, 0x26, 0x95, 0x1a, 0xf2, 0x4c, 0xb4, 0x84, 0x32, 0xaf, 0xa1, 0x68, 0x74, 0xd6, 0x5e, 0x54,
	0xf9, 0x40, 0x5e, 0xfc, 0x14, 0x6a, 0x7c, 0x81, 0xb1, 0x63, 0x49, 0xfd, 0x6f, 0xbf, 0xc0, 0x61,
	0x7b, 0xbd, 0x05, 0xc6, 0x5f, 0x04, 0x89, 0xc8, 0x98, 0x14, 0xb7, 0xdf, 0x87, 0xfa, 0x28, 0xf4,
	0x43, 0x11, 0x3b, 0x70, 0xf5, 0x61, 0x07, 0x44, 0x67, 0x9a, 0x6d, 0x77, 0xa0, 0xee, 0xe3, 0x14,
	0x83, 0xb1, 0xcc, 0x8c, 0x56, 0x77, 0x67, 0x29, 0x78, 0x24, 0xe9, 0x4c, 0xf3, 0xed, 0x7b, 0xb0,
	0x95, 0xf0, 0xa1, 0x8f, 0xc7, 0x11, 0x79, 0x31, 0x96, 0x59, 0xd2, 0xea, 0x5e, 0x2f, 0xc5, 0xa3,
	0xc4, 0x65, 0x2b, 0xb2, 0x37, 0x1f, 0x82, 0x55, 0xbc, 0x90, 0x8a, 0xe4, 0x39, 0x66, 0xd2, 0xdf,
	0x16, 0xa3, 0xa3, 0xfd, 0x0e, 0x98, 0x17, 0xdc, 0x4f, 0x55, 0xae, 0xb4, 0xba, 0xdb, 0x4b, 0x9d,
	0xbd, 0x85, 0x17, 0x33, 0xc5, 0xbc, 0x57, 0xb9, 0x6b, 0xb8, 0xdf, 0x57, 0x60, 0xab, 0x7c, 0x8f,
	0xfd, 0x16, 0x40, 0xe2, 0xcd, 0xf1, 0x41, 0x28, 0xe6, 0x3c, 0xd1, 0x3a, 0x4b, 0x14, 0xfb, 0x43,
	0xd8, 0xb9, 0x40, 0x91, 0x78, 0x23, 0xee, 0x9f, 0x7a, 0x73, 0x24, 0x7d, 0xf2, 0x96, 0x26, 0x5b,
	0xa3, 0xdb, 0xb7, 0xa1, 0x1e, 0x87, 0x22, 0xd9, 0xcf, 0x64, 0xbc, 0x5b, 0x5d, 0x67, 0xf9, 0x0e,
	0x86, 0x01, 0x9f, 0xd3, 0xbd, 0x0f, 0x3c, 0xf4, 0xc7, 0x4c, 0xcb, 0x51, 0x0d, 0x5f, 0x0a, 0x1e,
	0x45, 0x5e, 0x30, 0xcd, 0xfb, 0x44, 0x8e, 0xed, 0xbb, 0x00, 0x13, 0x12, 0xa6, 0xc4, 0xcf, 0xf3,
	0xe3, 0xc5, 0x1a, 0x4b, 0xb2, 0xf6, 0x7b, 0xb0, 0x3d, 0xf1, 0x16, 0x0f, 0x3c, 0x11, 0x27, 0x07,
	0xa1, 0x9f, 0xce, 0x03, 0x99, 0x35, 0x4d, 0x76, 0x85, 0xea, 0x46, 0xb0, 0xbd, 0xaa, 0x85, 0xd2,
	0x3f, 0xbf, 0x40, 0xd6, 0x9e, 0xf2, 0xc7, 0x0a, 0xcd, 0x6e, 0x43, 0x6b, 0xec, 0xc5, 0x91, 0xcf,
	0xb3, 0x52, 0x79, 0x96, 0x49, 0xd4, 0x4d, 0x2e, 0xbc, 0xd8, 0x1b, 0xfa, 0xaa, 0x29, 0x36, 0x59,
	0x0e, 0xdd, 0x29, 0x98, 0x32, 0x7d, 0x4a, 0xc5, 0x6e, 0xe5, 0xc5, 0x2e, 0x9b, 0x68, 0xa5, 0xd4,
	0x44, 0x77, 0xa0, 0xfa, 0x25, 0x2e, 0x74, 0x5f, 0xa5, 0x63, 0xd1, 0x12, 0x6a, 0xa5, 0x96, 0xb0,
	0x0b, 0xe6, 0xb9, 0x8c, 0xbd, 0x2a, 0x55, 0x05, 0xdc, 0xfb, 0x50, 0x57, 0xe9, 0x57, 0x68, 0x36,
	0x4a, 0x9a, 0xdb, 0xd0, 0x3a, 0x16, 0x1e, 0x06, 0x89, 0x2a, 0x72, 0x6d, 0x42, 0x89, 0xe4, 0xfe,
	0x62, 0x40, 0x4d, 0xc6, 0xd4, 0x85, 0x2d, 0x1f, 0xa7, 0x7c, 0x94, 0xed, 0x87, 0x69, 0x30, 0x8e,
	0x1d, 0xa3, 0x5d, 0xed, 0x54, 0xd9, 0x0a, 0xcd, 0xbe, 0x0e, 0xf5, 0xa1, 0xe2, 0x56, 0xda, 0xd5,
	0x8e, 0xc5, 0x34, 0xa2, 0xa7, 0xf9, 0x7c, 0x88, 0xbe, 0x36, 0x41, 0x01, 0x92, 0x8e, 0x04, 0x4e,
	0xbc, 0x85, 0x36, 0x43, 0x23, 0xa2, 0xc7, 0xe9, 0x84, 0xe8, 0xca, 0x12, 0x8d, 0xc8, 0x80, 0x21,
	0x8f, 0x8b, 0xca, 0xa7, 0x33, 0x69, 0x8e, 0x47, 0xdc, 0xcf, 0x4b, 0x5f, 0x01, 0xf7, 0x77, 0x83,
	0x46, 0x82, 0x6a, 0x65, 0x6b, 0x1e, 0x7e, 0x03, 0x9a, 0xd4, 0xe6, 0x9e, 0x5d, 0x70, 0xa1, 0x0d,
	0x6e, 0x10, 0x3e, 0xe7, 0xc2, 0xfe, 0x04, 0xea, 0xb2, 0x42, 0x36, 0xb4, 0xd5, 0x5c, 0x9d, 0xf4,
	0x2a, 0xd3, 0x62, 0x45, 0xe3, 0xa9, 0x95, 0x1a, 0x4f, 0x61, 0xac, 0x59, 0x36, 0xf6, 0x16, 0x98,
	0xd4, 0xc1, 0x32, 0xf9, 0xfa, 0x8d, 0x9a, 0x55, 0x9f, 0x53, 0x52, 0xee, 0x19, 0x5c, 0x5b, 0xb9,
	0xb1, 0xb8, 0xc9, 0x58, 0xbd, 0x69, 0x59, 0xed, 0x96, 0xae, 0x6e, 0x2a, 0xa5, 0x18, 0x7d, 0x1c,
	0x25, 0x38, 0xd6, 0x59, 0x57, 0x60, 0xf7, 0x47, 0x63, 0xa9, 0x57, 0xde, 0x47, 0x29, 0x3a, 0x0a,
	0xe7, 0x73, 0x1e, 0x8c, 0xb5, 0xea, 0x1c, 0x92, 0xdf, 0xc6, 0x43, 0xad, 0xba, 0x32, 0x1e, 0x12,
	0x16, 0x91, 0x8e, 0x60, 0x45, 0x44, 0x94, 0x3b, 0x73, 0xe4, 0x71, 0x2a, 0x70, 0x8e, 0x41, 0xa2,
	0x5d, 0x50, 0x26, 0xd9, 0x37, 0xa0, 0x91, 0xf0, 0xe9, 0x33, 0xea, 0x51, 0x3a, 0x92, 0x09, 0x9f,
	0x3e, 0xc2, 0xcc, 0x7e, 0x13, 0x2c, 0x59, 0xa5, 0x92, 0xa5, 0xc2, 0xd9, 0x94, 0x84, 0x47, 0x98,
	0xb9, 0x7f, 0x1b, 0x50, 0x1f, 0xa0, 0xb8, 0x40, 0xf1, 0x4a, 0x93, 0xb0, 0xbc, 0x61, 0x54, 0x5f,
	0xb2, 0x61, 0xd4, 0x36, 0x6f, 0x18, 0xe6, 0x72, 0xc3, 0xd8, 0x05, 0x73, 0x20, 0x46, 0x87, 0x7d,
	0xf9, 0xa2, 0x2a, 0x53, 0x80, 0xb2, 0xb1, 0x37, 0x4a, 0xbc, 0x0b, 0xd4, 0x6b, 0x87, 0x46, 0x6b,
	0x03, 0xb2, 0xb9, 0x61, 0xd6, 0xff, 0xcb, 0xed, 0xc3, 0xfd, 0xc1, 0x80, 0xfa, 0x11, 0xcf, 0xc2,
	0x34, 0x59, 0xcb, 0xda, 0x36, 0xb4, 0x7a, 0x51, 0xe4, 0x7b, 0xa3, 0x95, 0x4a, 0x2d, 0x91, 0x48,
	0xe2, 0x71, 0x29, 0x1e, 0xca, 0x17, 0x65, 0x12, 0x4d, 0x87, 0x03, 0xb9, 0x34, 0xa8, 0x0d, 0xa0,
	0x34, 0x1d, 0xd4, 0xae, 0x20, 0x99, 0xe4, 0xb4, 0x5e, 0x9a, 0x84, 0x13, 0x3f, 0xbc, 0x94, 0xde,
	0x69, 0xb2, 0x02, 0xbb, 0x7f, 0x54, 0xa0, 0xf6, 0x7f, 0x0d, 0xfa, 0x2d, 0x30, 0x3c, 0x9d, 0x1c,
	0x86, 0x57, 0x8c, 0xfd, 0x46, 0x69, 0xec, 0x3b, 0xd0, 0xc8, 0x04, 0x0f, 0xa6, 0x18, 0x3b, 0x4d,
	0xd9, 0x8d, 0x72, 0x28, 0x39, 0xb2, 0xee, 0xd4, 0xbc, 0xb7, 0x58, 0x0e, 0x8b, 0x3a, 0x82, 0x52,
	0x1d, 0x7d, 0xac, 0x57, 0x83, 0xd6, 0xd5, 0xd1, 0xb2, 0x69, 0x23, 0xf8, 0xef, 0x46, 0xf0, 0x5f,
	0x06, 0x98, 0x45, 0x11, 0x1e, 0xac, 0x16, 0xe1, 0xc1, 0xb2, 0x08, 0xfb, 0xfb, 0x79, 0x11, 0xf6,
	0xf7, 0x09, 0xb3, 0x93, 0xbc, 0x08, 0xd9, 0x09, 0x05, 0xeb, 0xa1, 0x08, 0xd3, 0x68, 0x3f, 0x53,
	0x51, 0xb5, 0x58, 0x81, 0x29, 0x73, 0xbf, 0x9e, 0xa1, 0xd0, 0xae, 0xb6, 0x98, 0x46, 0x94, 0xe7,
	0x47, 0xb2, 0x41, 0x29, 0xe7, 0x2a, 0x60, 0xbf, 0x0b, 0x26, 0x23, 0xe7, 0x49, 0x0f, 0xaf, 0xc4,
	0x45, 0x92, 0x99, 0xe2, 0x92, 0x52, 0xf5, 0x4b, 0xa0, 0x13, 0x3e, 0xff, 0x41, 0xf8, 0x08, 0xea,
	0x83, 0x99, 0x37, 0x49, 0xf2, 0x05, 0xeb, 0xf5, 0x52, 0x83, 0xf3, 0xe6, 0x28, 0x79, 0x4c, 0x8b,
	0xb8, 0x4f, 0xc1, 0x2a, 0x88, 0xcb, 0xe7, 0x18, 0xe5, 0xe7, 0xd8, 0x50, 0x3b, 0x0b, 0xbc, 0x24,
	0x2f, 0x75, 0x3a, 0x93, 0xb1, 0x4f, 0x53, 0x1e, 0x24, 0x5e, 0x92, 0xe5, 0xa5, 0x9e, 0x63, 0xf7,
	0x8e, 0x7e, 0x3e, 0xa9, 0x3b, 0x8b, 0x22, 0x14, 0xba, 0x6d, 0x28, 0x20, 0x2f, 0x09, 0x2f, 0x51,
	0x75, 0xfc, 0x2a, 0x53, 0xc0, 0xfd, 0x06, 0xac, 0x9e, 0x8f, 0x22, 0x61, 0xa9, 0x8f, 0x9b, 0x26,
	0xf1, 0x57, 0x83, 0xe3, 0x27, 0xf9, 0x0b, 0xe8, 0xbc, 0x6c, 0x11, 0xd5, 0x2b, 0x2d, 0xe2, 0x11,
	0x8f, 0xf8, 0x61, 0x5f, 0xe6, 0x79, 0x95, 0x69, 0xe4, 0xfe, 0x64, 0x40, 0x8d, 0x7a, 0x51, 0x49,
	0x75, 0xed, 0x65, 0x7d, 0xec, 0x44, 0x84, 0x17, 0xde, 0x18, 0x45, 0x6e, 0x5c, 0x8e, 0xa5, 0xd3,
	0x47, 0x33, 0x2c, 0x06, 0xbe, 0x46, 0x94, 0x6b, 0xf4, 0xff, 0x90, 0xd7, 0x52, 0x29, 0xd7, 0x88,
	0xcc, 0x14, 0x93, 0x36, 0xbb, 0x41, 0x1a, 0xa1, 0xe8, 0x8d, 0xe7, 0x5e, 0xbe, 0x01, 0x95, 0x28,
	0xee, 0x7d, 0xf5, 0x47, 0xb2, 0xd6, 0xd1, 0x8c, 0xcd, 0x7f, 0x2f, 0x57, 0x5f, 0xee, 0xfe, 0x6c,
	0x40, 0xe3, 0xb1, 0xde, 0xd5, 0xca, 0x56, 0x18, 0x2f, 0xb4, 0xa2, 0xb2, 0x62, 0x45, 0x17, 0x76,
	0x73, 0x99, 0x95, 0xfb, 0x95, 0x17, 0x36, 0xf2, 0xb4, 0x47, 0x6b, 0x45, 0xb0, 0x5e, 0xe5, 0x77,
	0xe5, 0x74, 0x55, 0x66, 0x53, 0xc0, 0xd7, 0xa2, 0xd2, 0x86, 0x96, 0xfe, 0xcd, 0x94, 0x3f, 0x6d,
	0xba, 0xa9, 0x96, 0x48, 0x6e, 0x17, 0xea, 0x07, 0x61, 0x30, 0xf1, 0xa6, 0x76, 0x07, 0x6a, 0xbd,
	0x34, 0x99, 0x49, 0x8d, 0xad, 0xee, 0x6e, 0xa9, 0xf0, 0xd3, 0x64, 0xa6, 0x64, 0x98, 0x94, 0x70,
	0x3f, 0x03, 0x58, 0xd2, 0x68, 0x4a, 0x2c, 0xa3, 0xf1, 0x04, 0x2f, 0x29, 0x65, 0x62, 0xa9, 0xa5,
	0xc9, 0x36, 0x70, 0xdc, 0xcf, 0xc1, 0xda, 0x4f, 0x3d, 0x7f, 0x7c, 0x18, 0x4c, 0x42, 0x6a, 0x1d,
	0xe7, 0x28, 0xe2, 0x65, 0xbc, 0x72, 0x48, 0xee, 0xa6, 0x2e, 0x52, 0xd4, 0x90, 0x46, 0xc3, 0xba,
	0xfc, 0xcd, 0xbf, 0xf3, 0x4f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xda, 0x7c, 0x0d, 0xab, 0xf8, 0x0f,
	0x00, 0x00,
}
