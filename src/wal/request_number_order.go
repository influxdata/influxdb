package wal

type RequestNumberOrder interface {
	isAfter(uint32, uint32) bool
	isAfterOrEqual(uint32, uint32) bool
	isBefore(uint32, uint32) bool
	isBeforeOrEqual(uint32, uint32) bool
}
