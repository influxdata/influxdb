package coordinator

import (
	"code.google.com/p/go.crypto/bcrypt"
	"fmt"
	"protocol"
)

type User struct {
	u *protocol.User
}

func (self *User) GetName() string {
	return *self.u.Name
}

func (self *User) CreateUser(name string) (*User, error) {
	if !self.IsClusterAdmin() {
		return nil, fmt.Errorf("You don't have permissions to create new users")
	}

	return &User{u: &protocol.User{Name: &name}}, nil
}

func (self *User) ChangePassword(u *User, newPwd string) error {
	if !self.IsClusterAdmin() && u.u.GetName() != self.u.GetName() {
		return fmt.Errorf("You don't have permissions to change someone else's password")
	}

	hash, err := hashPassword(newPwd)
	if err != nil {
		return err
	}
	hashStr := string(hash)
	u.u.Hash = &hashStr
	return nil
}

func (self *User) IsClusterAdmin() bool {
	return self.u.GetClusterAdmin()
}

func (self *User) SetClusterAdmin(u *User, isAdmin bool) error {
	if !self.IsClusterAdmin() {
		return fmt.Errorf("User %s doesn't have enough permissions to make %s a cluster admin", self.GetName(), u.GetName())
	}

	if u.GetName() == self.GetName() {
		return fmt.Errorf("Cannot remove admin access from yourself. Use a different account")
	}

	u.u.ClusterAdmin = &isAdmin
	return nil
}

func (self *User) IsDbAdmin(db string) bool {
	for _, dbName := range self.u.AdminFor {
		if db == dbName {
			return true
		}
	}

	return false
}

func (self *User) SetDbAdmin(u *User, db string) error {
	if u.IsDbAdmin(db) {
		return nil
	}

	if !self.IsClusterAdmin() && !self.IsDbAdmin(db) {
		return fmt.Errorf("User %s doesn't have enough permissions to make %s a db admin", self.GetName(), u.GetName())
	}

	u.u.AdminFor = append(u.u.AdminFor, db)
	return nil
}

func (self *User) RemoveDbAdmin(u *User, db string) error {
	if !self.IsClusterAdmin() && !self.IsDbAdmin(db) {
		return fmt.Errorf("User %s doesn't have enough permissions to make %s a db admin", self.GetName(), u.GetName())
	}

	if self.GetName() == u.GetName() {
		return fmt.Errorf("Cannot db admin access from yourself. Use a different account")
	}

	dbIndex := -1
	for idx, dbName := range u.u.AdminFor {
		if dbName == db {
			dbIndex = idx
			break
		}
	}

	if dbIndex > -1 {
		u.u.AdminFor = append(u.u.AdminFor[:dbIndex], u.u.AdminFor[dbIndex+1:]...)
	}

	return nil
}

// private funcs

func (self *User) isValidPwd(password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(self.u.GetHash()), []byte(password)) == nil
}

func hashPassword(password string) ([]byte, error) {
	// The second arg is the cost of the hashing, higher is slower but makes it harder
	// to brute force, since it will be really slow and impractical
	return bcrypt.GenerateFromPassword([]byte(password), 13)
}
