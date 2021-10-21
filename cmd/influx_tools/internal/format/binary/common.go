package binary

//go:generate protoc --go_out=. tools_binary.proto
//go:generate stringer -type=MessageType

import "errors"

var (
	ErrWriteAfterClose       = errors.New("format/binary: write after close")
	ErrWriteBucketAfterClose = errors.New("format/binary: write to closed bucket")
)

var (
	Magic = [...]byte{0x49, 0x46, 0x4c, 0x58, 0x44, 0x55, 0x4d, 0x50} // IFLXDUMP
)

type MessageType byte

const (
	HeaderType MessageType = iota + 1
	BucketHeaderType
	BucketFooterType
	SeriesHeaderType
	FloatPointsType
	IntegerPointsType
	UnsignedPointsType
	BooleanPointsType
	StringPointsType
	SeriesFooterType
)

/*
Stream format

FILE:
┌─────────────────┬────────────────────┬─────────────────┐
│                 │                    │                 │
│  IFLXDUMP (8)   │       Header       │  BUCKET 0..n    │
│                 │                    │                 │
└─────────────────┴────────────────────┴─────────────────┘

BUCKET:
┌─────────────────┬────────────────────┬─────────────────┐
│                 │                    │                 │
│  Bucket Header  │  SERIES DATA 0..n  │  Bucket Footer  │
│                 │                    │                 │
└─────────────────┴────────────────────┴─────────────────┘

SERIES DATA:
┌─────────────────┬────────────────────┬─────────────────┐
│                 │                    │                 │
│  Series Header  │    POINTS 0..n     │  Series Footer  │
│                 │                    │                 │
└─────────────────┴────────────────────┴─────────────────┘
*/
