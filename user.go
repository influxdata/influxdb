package influxdb

import (
	"fmt"
	"regexp"
	"strings"

	"code.google.com/p/go.crypto/bcrypt"
	"github.com/influxdb/go-cache"
)

var userCache = cache.New(0, 0)

type Matcher struct {
	IsRegex bool
	Name    string
}

func (m *Matcher) Matches(name string) bool {
	if m.IsRegex {
		matches, _ := regexp.MatchString(m.Name, name)
		return matches
	}
	return m.Name == name
}

type CommonUser struct {
	Name          string `json:"name"`
	Hash          string `json:"hash"`
	IsUserDeleted bool   `json:"is_deleted"`
	CacheKey      string `json:"cache_key"`
}

func (u *CommonUser) GetName() string                 { return u.Name }
func (u *CommonUser) IsDeleted() bool                 { return u.IsUserDeleted }
func (u *CommonUser) IsClusterAdmin() bool            { return false }
func (u *CommonUser) IsDbAdmin(db string) bool        { return false }
func (u *CommonUser) GetDb() string                   { return "" }
func (u *CommonUser) HasWriteAccess(name string) bool { return false }
func (u *CommonUser) HasReadAccess(name string) bool  { return false }

func (u *CommonUser) ChangePassword(hash string) error {
	u.Hash = hash
	userCache.Delete(u.CacheKey)
	return nil
}

func (u *CommonUser) isValidPwd(password string) bool {
	if pwd, ok := userCache.Get(u.CacheKey); ok {
		return password == pwd.(string)
	}

	isValid := bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password)) == nil
	if isValid {
		userCache.Set(u.CacheKey, password, 0)
	}
	return isValid
}

type ClusterAdmin struct {
	CommonUser `json:"common"`
}

func (u *ClusterAdmin) IsClusterAdmin() bool         { return true }
func (u *ClusterAdmin) IsDbAdmin(_ string) bool      { return true }
func (u *ClusterAdmin) HasWriteAccess(_ string) bool { return true }
func (u *ClusterAdmin) GetWritePermission() string   { return ".*" }
func (u *ClusterAdmin) HasReadAccess(_ string) bool  { return true }
func (u *ClusterAdmin) GetReadPermission() string    { return ".*" }

// clusterAdmins represents a list of cluster admins, sortable by name.
type clusterAdmins []*ClusterAdmin

func (p clusterAdmins) Len() int           { return len(p) }
func (p clusterAdmins) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p clusterAdmins) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type DBUser struct {
	CommonUser `json:"common"`
	DB         string     `json:"db"`
	ReadFrom   []*Matcher `json:"read_matchers"`
	WriteTo    []*Matcher `json:"write_matchers"`
	IsAdmin    bool       `json:"is_admin"`
}

func (u *DBUser) IsDBAdmin(db string) bool {
	return u.IsAdmin && u.DB == db
}

func (u *DBUser) HasWriteAccess(name string) bool {
	for _, matcher := range u.WriteTo {
		if matcher.Matches(name) {
			return true
		}
	}

	return false
}

func (u *DBUser) GetWritePermission() string {
	if len(u.WriteTo) > 0 {
		matcher := u.WriteTo[0]
		return matcher.Name
	}
	return ""
}

func (u *DBUser) HasReadAccess(name string) bool {
	for _, matcher := range u.ReadFrom {
		if matcher.Matches(name) {
			return true
		}
	}

	return false
}

func (u *DBUser) GetReadPermission() string {
	if len(u.ReadFrom) > 0 {
		matcher := u.ReadFrom[0]
		return matcher.Name
	}
	return ""
}

func (u *DBUser) GetDb() string {
	return u.DB
}

func (u *DBUser) ChangePermissions(readPermissions, writePermissions string) {
	u.ReadFrom = []*Matcher{{true, readPermissions}}
	u.WriteTo = []*Matcher{{true, writePermissions}}
}

// dbUsers represents a list of database users, sortable by name.
type dbUsers []*DBUser

func (p dbUsers) Len() int           { return len(p) }
func (p dbUsers) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p dbUsers) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func HashPassword(password string) ([]byte, error) {
	if length := len(password); length < 4 || length > 56 {
		return nil, fmt.Errorf("Password must be more than 4 and less than 56 characters")
	}

	// The second arg is the cost of the hashing, higher is slower but makes it harder
	// to brute force, since it will be really slow and impractical
	return bcrypt.GenerateFromPassword([]byte(password), 10)
}

// isValidName returns true if the name contains no invalid characters.
func isValidName(name string) bool {
	return !strings.Contains(name, "%")
}
