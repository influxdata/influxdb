package gen

//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@types.tmpldata arrays.gen.go.tmpl values_constant.gen.go.tmpl
