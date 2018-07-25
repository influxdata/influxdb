package tsdb

import (
	"unsafe"

	"github.com/influxdata/influxdb/models"
)

const (
	// constants describing bit layout of id and type info
	seriesIDTypeFlag  = 1 << 63                   // a flag marking that the id contains type info
	seriesIDValueMask = 0xFFFFFFFF                // series ids numerically are 32 bits
	seriesIDTypeShift = 32                        // we put the type right after the value info
	seriesIDTypeMask  = 0xFF << seriesIDTypeShift // a mask for the type byte
)

// SeriesIDHasType returns if the raw id contains type information.
func SeriesIDHasType(id uint64) bool { return id&seriesIDTypeFlag > 0 }

// SeriesID is the type of a series id.
type SeriesID struct{ ID uint64 }

// NewSeriesID constructs a series id from the raw value. It discards any type information.
func NewSeriesID(id uint64) SeriesID { return SeriesID{ID: id & seriesIDValueMask} }

// IsZero returns if the SeriesID is zero.
func (s SeriesID) IsZero() bool { return s.ID == 0 }

// ID returns the raw id for the SeriesID.
func (s SeriesID) RawID() uint64 { return s.ID }

// WithType constructs a SeriesIDTyped with the given type.
func (s SeriesID) WithType(typ models.FieldType) SeriesIDTyped {
	return NewSeriesIDTyped(s.ID | seriesIDTypeFlag | (uint64(typ&0xFF) << seriesIDTypeShift))
}

// Greater returns if the SeriesID is greater than the passed in value.
func (s SeriesID) Greater(o SeriesID) bool { return s.ID > o.ID }

// Less returns if the SeriesID is less than the passed in value.
func (s SeriesID) Less(o SeriesID) bool { return s.ID < o.ID }

// SeriesIDType represents a series id with a type.
type SeriesIDTyped struct{ ID uint64 }

// NewSeriesIDTyped constructs a typed series id from the raw values.
func NewSeriesIDTyped(id uint64) SeriesIDTyped { return SeriesIDTyped{ID: id} }

// IsZero returns if the SeriesIDTyped is zero. It ignores any type information.
func (s SeriesIDTyped) IsZero() bool { return s.ID&seriesIDValueMask == 0 }

// ID returns the raw id for the SeriesIDTyped.
func (s SeriesIDTyped) RawID() uint64 { return s.ID }

// SeriesID constructs a SeriesID, discarding any type information.
func (s SeriesIDTyped) SeriesID() SeriesID { return NewSeriesID(s.ID) }

// Type returns the associated type.
func (s SeriesIDTyped) Type() models.FieldType {
	return models.FieldType((s.ID & seriesIDTypeMask) >> seriesIDTypeShift)
}

// some static assertions that the SeriesIDSize matches
const (
	seriesIDStructSize      = unsafe.Sizeof(SeriesID{})
	seriesIDTypedStructSize = unsafe.Sizeof(SeriesIDTyped{})
)

type (
	// if the values are not the same, at least one will be negative causing a compilation failure
	_ [SeriesIDSize - seriesIDStructSize]byte
	_ [seriesIDStructSize - SeriesIDSize]byte
	_ [SeriesIDSize - seriesIDTypedStructSize]byte
	_ [seriesIDTypedStructSize - SeriesIDSize]byte
)
