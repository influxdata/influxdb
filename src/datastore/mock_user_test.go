package datastore

type MockUser struct {
	dbCannotRead  map[string]bool
	dbCannotWrite map[string]bool
}

func (self *MockUser) GetName() string {
	return "mockuser"
}
func (self *MockUser) IsDeleted() bool {
	return false
}
func (self *MockUser) IsClusterAdmin() bool {
	return false
}
func (self *MockUser) IsDbAdmin(db string) bool {
	return false
}
func (self *MockUser) GetDb() string {
	return ""
}
func (self *MockUser) HasWriteAccess(name string) bool {
	_, ok := self.dbCannotWrite[name]
	return !ok
}
func (self *MockUser) HasReadAccess(name string) bool {
	_, ok := self.dbCannotRead[name]
	return !ok
}
