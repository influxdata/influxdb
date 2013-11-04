package http

import (
	"common"
	"fmt"
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

func (self *MockUserManager) AuthenticateDbUser(db, username, password string) (common.User, error) {
	if username == "fail_auth" {
		return nil, fmt.Errorf("Invalid username/password")
	}
	return nil, nil
}
func (self *MockUserManager) AuthenticateClusterAdmin(username, password string) (common.User, error) {
	if username == "fail_auth" {
		return nil, fmt.Errorf("Invalid username/password")
	}
	return nil, nil
}
func (self *MockUserManager) CreateClusterAdminUser(request common.User, username string) error {
	self.ops = append(self.ops, &Operation{"cluster_admin_add", username, "", false})
	return nil
}
func (self *MockUserManager) DeleteClusterAdminUser(requester common.User, username string) error {
	self.ops = append(self.ops, &Operation{"cluster_admin_del", username, "", false})
	return nil
}
func (self *MockUserManager) ChangeClusterAdminPassword(requester common.User, username, password string) error {
	self.ops = append(self.ops, &Operation{"cluster_admin_passwd", username, password, false})
	return nil
}
func (self *MockUserManager) CreateDbUser(request common.User, db, username string) error {
	self.ops = append(self.ops, &Operation{"db_user_add", username, "", false})
	return nil
}
func (self *MockUserManager) DeleteDbUser(requester common.User, db, username string) error {
	self.ops = append(self.ops, &Operation{"db_user_del", username, "", false})
	return nil
}
func (self *MockUserManager) ChangeDbUserPassword(requester common.User, db, username, password string) error {
	self.ops = append(self.ops, &Operation{"db_user_passwd", username, password, false})
	return nil
}
func (self *MockUserManager) SetDbAdmin(requester common.User, db, username string, isAdmin bool) error {
	self.ops = append(self.ops, &Operation{"db_user_admin", username, "", isAdmin})
	return nil
}
func (self *MockUserManager) ListClusterAdmins(requester common.User) ([]string, error) {
	return self.clusterAdmins, nil
}
func (self *MockUserManager) ListDbUsers(requester common.User, db string) ([]string, error) {
	return self.dbUsers[db], nil
}
