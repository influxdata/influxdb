package http

import (
	"coordinator"
)

type Operation struct {
	operation string
	username  string
	password  string
	isAdmin   bool
}

type MockUserManager struct {
	dbUsers       map[string][]string
	clusterAdmins []string
	ops           []*Operation
}

func (self *MockUserManager) AuthenticateDbUser(db, username, password string) (coordinator.User, error) {
	return nil, nil
}
func (self *MockUserManager) AuthenticateClusterAdmin(username, password string) (coordinator.User, error) {
	return nil, nil
}
func (self *MockUserManager) CreateClusterAdminUser(request coordinator.User, username string) error {
	self.ops = append(self.ops, &Operation{"cluster_admin_add", username, "", false})
	return nil
}
func (self *MockUserManager) DeleteClusterAdminUser(requester coordinator.User, username string) error {
	self.ops = append(self.ops, &Operation{"cluster_admin_del", username, "", false})
	return nil
}
func (self *MockUserManager) ChangeClusterAdminPassword(requester coordinator.User, username, password string) error {
	self.ops = append(self.ops, &Operation{"cluster_admin_passwd", username, password, false})
	return nil
}
func (self *MockUserManager) CreateDbUser(request coordinator.User, db, username string) error {
	self.ops = append(self.ops, &Operation{"db_user_add", username, "", false})
	return nil
}
func (self *MockUserManager) DeleteDbUser(requester coordinator.User, db, username string) error {
	self.ops = append(self.ops, &Operation{"db_user_del", username, "", false})
	return nil
}
func (self *MockUserManager) ChangeDbUserPassword(requester coordinator.User, db, username, password string) error {
	self.ops = append(self.ops, &Operation{"db_user_passwd", username, password, false})
	return nil
}
func (self *MockUserManager) SetDbAdmin(requester coordinator.User, db, username string, isAdmin bool) error {
	self.ops = append(self.ops, &Operation{"db_user_admin", username, "", isAdmin})
	return nil
}
func (self *MockUserManager) ListClusterAdmins(requester coordinator.User) ([]string, error) {
	return self.clusterAdmins, nil
}
func (self *MockUserManager) ListDbUsers(requester coordinator.User, db string) ([]string, error) {
	return self.dbUsers[db], nil
}
