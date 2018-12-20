package reads

//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@array_cursor.gen.go.tmpldata array_cursor.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@array_cursor.gen.go.tmpldata response_writer.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@array_cursor.gen.go.tmpldata stream_reader.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@array_cursor.gen.go.tmpldata stream_reader_gen_test.go.tmpl
//go:generate stringer -type=readState -trimprefix=state
