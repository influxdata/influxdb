package coordinator

import (
	"code.google.com/p/go.crypto/bcrypt"
	"regexp"
)

type Matcher struct {
	isRegex bool
	name    string
}

func (self *Matcher) Matches(name string) bool {
	if self.isRegex {
		matches, _ := regexp.MatchString(self.name, name)
		return matches
	}
	return self.name == name
}

type CommonUser struct {
	Name          string `json:"name"`
	Hash          string `json:"hash"`
	IsUserDeleted bool   `json:"is_deleted"`
}

func (self *CommonUser) GetName() string {
	return self.Name
}

func (self *CommonUser) IsDeleted() bool {
	return self.IsUserDeleted
}

func (self *CommonUser) changePassword(password string) error {
	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	self.Hash = string(hash)
	return nil
}

func (self *CommonUser) isValidPwd(password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(self.Hash), []byte(password)) == nil
}

func (self *CommonUser) IsClusterAdmin() bool {
	return false
}

func (self *CommonUser) IsDbAdmin(db string) bool {
	return false
}

func (self *CommonUser) GetDb() string {
	return ""
}

func (self *CommonUser) HasWriteAccess(name string) bool {
	return false
}

func (self *CommonUser) HasReadAccess(name string) bool {
	return false
}

type clusterAdmin struct {
	CommonUser `josn:"common"`
}

func (self *clusterAdmin) IsClusterAdmin() bool {
	return true
}

type dbUser struct {
	CommonUser `json:"common"`
	Db         string     `json:"db"`
	WriteTo    []*Matcher `json:"write_matchers"`
	ReadFrom   []*Matcher `json:"read_matchers"`
	IsAdmin    bool       `json:"is_admin"`
}

func (self *dbUser) IsDbAdmin(db string) bool {
	return self.IsAdmin && self.Db == db
}

func (self *dbUser) HasWriteAccess(name string) bool {
	for _, matcher := range self.WriteTo {
		if matcher.Matches(name) {
			return true
		}
	}

	return false
}

func (self *dbUser) HasReadAccess(name string) bool {
	for _, matcher := range self.ReadFrom {
		if matcher.Matches(name) {
			return true
		}
	}

	return false
}

func (self *dbUser) GetDb() string {
	return self.Db
}

// private funcs

func hashPassword(password string) ([]byte, error) {
	// The second arg is the cost of the hashing, higher is slower but makes it harder
	// to brute force, since it will be really slow and impractical
	return bcrypt.GenerateFromPassword([]byte(password), 10)
}
