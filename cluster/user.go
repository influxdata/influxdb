package cluster

import (
	"regexp"

	"code.google.com/p/go.crypto/bcrypt"
	"github.com/influxdb/go-cache"
	"github.com/influxdb/influxdb/common"
)

var userCache *cache.Cache

func init() {
	userCache = cache.New(0, 0)
}

type Matcher struct {
	IsRegex bool
	Name    string
}

func (self *Matcher) Matches(name string) bool {
	if self.IsRegex {
		matches, _ := regexp.MatchString(self.Name, name)
		return matches
	}
	return self.Name == name
}

type CommonUser struct {
	Name          string `json:"name"`
	Hash          string `json:"hash"`
	IsUserDeleted bool   `json:"is_deleted"`
	CacheKey      string `json:"cache_key"`
}

func (self *CommonUser) GetName() string {
	return self.Name
}

func (self *CommonUser) IsDeleted() bool {
	return self.IsUserDeleted
}

func (self *CommonUser) ChangePassword(hash string) error {
	self.Hash = hash
	userCache.Delete(self.CacheKey)
	return nil
}

func (self *CommonUser) isValidPwd(password string) bool {
	if pwd, ok := userCache.Get(self.CacheKey); ok {
		return password == pwd.(string)
	}

	isValid := bcrypt.CompareHashAndPassword([]byte(self.Hash), []byte(password)) == nil
	if isValid {
		userCache.Set(self.CacheKey, password, 0)
	}
	return isValid
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

type ClusterAdmin struct {
	CommonUser `json:"common"`
}

func (self *ClusterAdmin) IsClusterAdmin() bool {
	return true
}

func (self *ClusterAdmin) IsDbAdmin(_ string) bool {
	return true
}

func (self *ClusterAdmin) HasWriteAccess(_ string) bool {
	return true
}

func (self *ClusterAdmin) GetWritePermission() string {
	return ".*"
}

func (self *ClusterAdmin) HasReadAccess(_ string) bool {
	return true
}

func (self *ClusterAdmin) GetReadPermission() string {
	return ".*"
}

type DbUser struct {
	CommonUser `json:"common"`
	Db         string     `json:"db"`
	ReadFrom   []*Matcher `json:"read_matchers"`
	WriteTo    []*Matcher `json:"write_matchers"`
	IsAdmin    bool       `json:"is_admin"`
}

func (self *DbUser) IsDbAdmin(db string) bool {
	return self.IsAdmin && self.Db == db
}

func (self *DbUser) HasWriteAccess(name string) bool {
	for _, matcher := range self.WriteTo {
		if matcher.Matches(name) {
			return true
		}
	}

	return false
}

func (self *DbUser) GetWritePermission() string {
	if len(self.WriteTo) > 0 {
		matcher := self.WriteTo[0]
		return matcher.Name
	}
	return ""
}

func (self *DbUser) HasReadAccess(name string) bool {
	for _, matcher := range self.ReadFrom {
		if matcher.Matches(name) {
			return true
		}
	}

	return false
}

func (self *DbUser) GetReadPermission() string {
	if len(self.ReadFrom) > 0 {
		matcher := self.ReadFrom[0]
		return matcher.Name
	}
	return ""
}

func (self *DbUser) GetDb() string {
	return self.Db
}

func (self *DbUser) ChangePermissions(readPermissions, writePermissions string) {
	self.ReadFrom = []*Matcher{{true, readPermissions}}
	self.WriteTo = []*Matcher{{true, writePermissions}}
}

func HashPassword(password string) ([]byte, error) {
	if length := len(password); length < 4 || length > 56 {
		return nil, common.NewQueryError(common.InvalidArgument, "Password must be more than 4 and less than 56 characters")
	}

	// The second arg is the cost of the hashing, higher is slower but makes it harder
	// to brute force, since it will be really slow and impractical
	return bcrypt.GenerateFromPassword([]byte(password), 10)
}
