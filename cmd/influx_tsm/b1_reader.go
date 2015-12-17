package main

type B1Reader struct {
	path string
}

func NewB1Reader(path string) *B1Reader {
	return &B1Reader{
		path: path,
	}
}

func (b *B1Reader) Open() error {
	return nil
}

func (b *B1Reader) Next() (int64, []byte) {
	return 0, nil
}

func (b *B1Reader) Close() error {
	return nil
}
