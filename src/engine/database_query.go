package engine

import (
	"common"
	"parser"
)

type DatabaseQuery struct {
	User     common.User
	Database string
	Query    *parser.SelectQuery
}
