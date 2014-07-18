package http

import (
	"fmt"

	"github.com/influxdb/influxdb/common"
)

type Operation struct {
	operation string
	username  string
	password  string
	isAdmin   bool
}

type MockDbUser struct {
	Name    string
	IsAdmin bool
}

func (self MockDbUser) GetName() string {
	return self.Name
}

func (self MockDbUser) IsDeleted() bool {
	return false
}

func (self MockDbUser) IsClusterAdmin() bool {
	return false
}

func (self MockDbUser) IsDbAdmin(_ string) bool {
	return self.IsAdmin
}

func (self MockDbUser) GetDb() string {
	return ""
}

func (self MockDbUser) HasWriteAccess(_ string) bool {
	return true
}

func (self MockDbUser) GetWritePermission() string {
	return ".*"
}

func (self MockDbUser) HasReadAccess(_ string) bool {
	return true
}

func (self MockDbUser) GetReadPermission() string {
	return ".*"
}

type MockUserManager struct {
	UserManager
	dbUsers       map[string]map[string]MockDbUser
	clusterAdmins []string
	ops           []*Operation
}

func (self *MockUserManager) AuthenticateDbUser(db, username, password string) (common.User, error) {
	if username == "fail_auth" {
		return nil, fmt.Errorf("Invalid username/password")
	}

	if username != "dbuser" {
		return nil, fmt.Errorf("Invalid username/password")
	}

	return nil, nil
}

func (self *MockUserManager) AuthenticateClusterAdmin(username, password string) (common.User, error) {
	if username == "fail_auth" {
		return nil, fmt.Errorf("Invalid username/password")
	}

	if username != "root" {
		return nil, fmt.Errorf("Invalid username/password")
	}

	return nil, nil
}

func (self *MockUserManager) CreateClusterAdminUser(request common.User, username, password string) error {
	if username == "" {
		return fmt.Errorf("Invalid empty username")
	}

	self.ops = append(self.ops, &Operation{"cluster_admin_add", username, password, false})
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

func (self *MockUserManager) CreateDbUser(request common.User, db, username, password string, permissions ...string) error {
	if username == "" {
		return fmt.Errorf("Invalid empty username")
	}

	self.ops = append(self.ops, &Operation{"db_user_add", username, password, false})
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

func (self *MockUserManager) ListDbUsers(requester common.User, db string) ([]common.User, error) {
	dbUsers := self.dbUsers[db]
	users := make([]common.User, 0, len(dbUsers))
	for _, user := range dbUsers {
		users = append(users, user)
	}

	return users, nil
}

func (self *MockUserManager) GetDbUser(requester common.User, db, username string) (common.User, error) {
	dbUsers := self.dbUsers[db]
	if dbUser, ok := dbUsers[username]; ok {
		return MockDbUser{Name: dbUser.GetName(), IsAdmin: dbUser.IsDbAdmin(db)}, nil
	} else {
		return nil, fmt.Errorf("'%s' is not a valid username for database '%s'", username, db)
	}
}
