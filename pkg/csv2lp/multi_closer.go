package csv2lp

import (
	"io"
	"log"
)

// multicloser
type multiCloser struct {
	closers []io.Closer
}

// Close implements io.Closer to closes all nested closers and logs a warning on error
func (mc *multiCloser) Close() error {
	var err error
	for i := 0; i < len(mc.closers); i++ {
		e := mc.closers[i].Close()
		if e != nil {
			if err == nil {
				err = e
			}
			log.Println(err)
		}
	}
	return err
}

//MultiCloser creates am io.Closer that silently closes supplied io.Closer instances
func MultiCloser(closers ...io.Closer) io.Closer {
	c := make([]io.Closer, len(closers))
	copy(c, closers)
	return &multiCloser{c}
}
