package server

type MockUser struct {
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
	return true
}
func (self *MockUser) HasReadAccess(name string) bool {
	return true
}
