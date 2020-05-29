package cursors

//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@arrayvalues.gen.go.tmpldata arrayvalues.gen.go.tmpl
//go:generate stringer -type FieldType
