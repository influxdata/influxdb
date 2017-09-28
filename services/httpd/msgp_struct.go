package httpd

//go:generate msgp -io=true -marshal=false -o=msgp_encode.go

type Message struct {
	Level string `msg:"level"`
	Text  string `msg:"text"`
}

type ResultHeader struct {
	ID       int       `msg:"id"`
	Messages []Message `msg:"messages"`
	Error    *string   `msg:"error"`
}

type Column struct {
	Name string `msg:"name"`
	Type string `msg:"type"`
}

type SeriesHeader struct {
	Name    *string           `msg:"name"`
	Tags    map[string]string `msg:"tags"`
	Columns []Column          `msg:"columns"`
}

type SeriesError struct {
	Error string `msg:"error"`
}

type RowBatchHeader struct {
	Length   int  `msg:"length"`
	Continue bool `msg:"continue"`
}

type RowBatchError struct {
	Error string `msg:"error"`
}

type Row struct {
	Value []interface{} `msg:"values"`
}

type RowError struct {
	Error string `msg:"error"`
}
