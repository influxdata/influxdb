package gen

//go:generate tmpl -data=@types.tmpldata arrays.gen.go.tmpl values.gen.go.tmpl values_sequence.gen.go.tmpl
//go:generate stringer -type=precision -trimprefix=precision
