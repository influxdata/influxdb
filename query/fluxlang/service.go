// Package language exposes the flux parser as an interface.
package fluxlang

import (
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb"
)

// DefaultService is the default language service.
var DefaultService influxdb.FluxLanguageService = defaultService{}

type defaultService struct{}

func (d defaultService) Parse(source string) (pkg *ast.Package, err error) {
	pkg = parser.ParseSource(source)
	if ast.Check(pkg) > 0 {
		err = ast.GetError(pkg)
	}
	return pkg, err
}
