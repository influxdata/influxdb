package write

import (
	"io"
	"log"
)

type multiCloser struct {
	closers []io.Closer
}

// Close closes all closers and logs a warning on error
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

//MultiCloser is an io.Closer that silently closes more io.Closers
func MultiCloser(closers ...io.Closer) io.Closer {
	c := make([]io.Closer, len(closers))
	copy(c, closers)
	return &multiCloser{c}
}
