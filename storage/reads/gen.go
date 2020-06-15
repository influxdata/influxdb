package reads

//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@array_cursor.gen.go.tmpldata array_cursor.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@array_cursor.gen.go.tmpldata -o=array_cursor_gen_test.go array_cursor_test.gen.go.tmpl
