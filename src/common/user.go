package common

type User interface {
	GetName() string
	IsDeleted() bool
	IsClusterAdmin() bool
	IsDbAdmin(db string) bool
	GetDb() string
	HasWriteAccess(name string) bool
	GetWritePermission() string
	HasReadAccess(name string) bool
	GetReadPermission() string
}
