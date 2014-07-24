package parser

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/common"
)

type GroupByClause struct {
	FillWithZero bool
	FillValue    *Value
	Elems        []*Value
}

func (self GroupByClause) GetGroupByTime() (*time.Duration, error) {
	for _, groupBy := range self.Elems {
		if groupBy.IsFunctionCall() && strings.ToLower(groupBy.Name) == "time" {
			// TODO: check the number of arguments and return an error
			if len(groupBy.Elems) != 1 {
				return nil, common.NewQueryError(common.WrongNumberOfArguments, "time function only accepts one argument")
			}

			if groupBy.Elems[0].Type != ValueDuration {
				log.Debug("Get a time function without a duration argument %v", groupBy.Elems[0].Type)
			}
			arg := groupBy.Elems[0].Name
			durationInt, err := common.ParseTimeDuration(arg)
			if err != nil {
				return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("invalid argument %s to the time function", arg))
			}
			duration := time.Duration(durationInt)
			return &duration, nil
		}
	}
	return nil, nil
}

func (self *GroupByClause) GetString() string {
	buffer := bytes.NewBufferString("")

	buffer.WriteString(Values(self.Elems).GetString())

	if self.FillWithZero {
		fmt.Fprintf(buffer, " fill(%s)", self.FillValue.GetString())
	}
	return buffer.String()
}
