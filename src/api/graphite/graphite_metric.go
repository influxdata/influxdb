package graphite

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type GraphiteMetric struct {
	name         string
	isInt        bool
	integerValue int64
	floatValue   float64
	timestamp    int64
}

func (self *GraphiteMetric) Read(reader *bufio.Reader) error {
	buf, err := reader.ReadBytes('\n')
	str := strings.TrimSpace(string(buf))
	if err != nil {
		if err != io.EOF {
			return fmt.Errorf("GraphiteServer: connection closed uncleanly/broken: %s\n", err.Error())
		}
		if len(str) > 0 {
			return fmt.Errorf("GraphiteServer: incomplete read, line read: '%s'. neglecting line because connection closed because of %s\n", str, err.Error())
		}
		return err
	}
	elements := strings.Fields(str)
	if len(elements) != 3 {
		return fmt.Errorf("Received '%s' which doesn't have three fields", str)
	}
	self.name = elements[0]
	self.floatValue, err = strconv.ParseFloat(elements[1], 64)
	if err != nil {
		return err
	}
	if i := int64(self.floatValue); float64(i) == self.floatValue {
		self.isInt = true
		self.integerValue = int64(self.floatValue)
	}
	timestamp, err := strconv.ParseUint(elements[2], 10, 32)
	if err != nil {
		return err
	}
	self.timestamp = int64(timestamp * 1000000)
	return nil
}
