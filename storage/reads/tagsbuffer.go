package reads

import (
	"github.com/influxdata/influxdb/models"
)

type tagsBuffer struct {
	sz  int
	i   int
	buf models.Tags
}

func (tb *tagsBuffer) copyTags(src models.Tags) models.Tags {
	var buf models.Tags
	if len(src) > tb.sz {
		buf = make(models.Tags, len(src))
	} else {
		if tb.i+len(src) > len(tb.buf) {
			tb.buf = make(models.Tags, tb.sz)
			tb.i = 0
		}

		buf = tb.buf[tb.i : tb.i+len(src)]
		tb.i += len(src)
	}

	copy(buf, src)

	return buf
}
