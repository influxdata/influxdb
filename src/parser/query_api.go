package parser

import (
	"fmt"
	"protocol"
	"strconv"
	"time"
)

// this file provides the high level api of the query object

var (
	ZERO_TIME    = time.Unix(0, 0)
	charToPeriod = map[byte]int64{
		's': int64(1),
		'm': int64(time.Minute / time.Second),
		'h': int64(time.Hour / time.Second),
		'd': int64(24 * time.Hour / time.Second),
		'w': int64(7 * 24 * time.Hour / time.Second),
	}
)

func (self *Query) Filter(series *protocol.Series) *protocol.Series {
	return nil
}

// parse time expressions, e.g. now() - 1d
func parseTime(expr *Expression) (int64, error) {
	if value, ok := expr.GetLeftValue(); ok {
		if value.IsFunctionCall() && value.Name == "now" {
			return time.Now().Unix(), nil
		}

		if value.IsFunctionCall() {
			return 0, fmt.Errorf("Invalid use of function %s", value.Name)
		}

		name := value.Name
		if period, ok := charToPeriod[name[len(name)-1]]; ok {
			parsedInt, err := strconv.Atoi(name[:len(name)-1])
			if err != nil {
				return 0, err
			}
			return int64(parsedInt) * period, nil
		}

		parsedInt, err := strconv.Atoi(name)
		return int64(parsedInt), err
	}

	leftExpression, _ := expr.GetLeftExpression()
	leftValue, err := parseTime(leftExpression)
	if err != nil {
		return 0, err
	}
	rightValue, err := parseTime(expr.Right)
	if err != nil {
		return 0, err
	}
	switch expr.Operation {
	case '+':
		return leftValue + rightValue, nil
	case '-':
		return leftValue - rightValue, nil
	default:
		return 0, fmt.Errorf("Cannot use '%c' in a time expression", expr.Operation)
	}
}

func GetTime(condition *WhereCondition, isParsingStartTime bool) (time.Time, error) {
	if condition == nil {
		return ZERO_TIME, nil
	}

	if expr, ok := condition.GetBoolExpression(); ok {
		leftValue, isLeftValue := expr.Left.GetLeftValue()
		rightValue, isRightValue := expr.Right.GetLeftValue()

		// if this expression isn't "time > xxx" or "xxx < time" then return
		// TODO: we should do a check to make sure "time" doesn't show up in
		// either expressions
		if !isLeftValue && !isRightValue {
			return ZERO_TIME, nil
		}

		var timeExpression *Expression
		if !isRightValue {
			if leftValue.Name != "time" {
				return ZERO_TIME, nil
			}
			timeExpression = expr.Right
		} else {
			if rightValue.Name != "time" {
				return ZERO_TIME, nil
			}
			timeExpression = expr.Left
		}

		switch expr.Operation {
		case ">":
			if isParsingStartTime && !isLeftValue || !isParsingStartTime && !isRightValue {
				return ZERO_TIME, nil
			}
		case "<":
			if !isParsingStartTime && !isLeftValue || isParsingStartTime && !isRightValue {
				return ZERO_TIME, nil
			}
		default:
			return ZERO_TIME, fmt.Errorf("Cannot use time with '%s'", expr.Operation)
		}

		seconds, err := parseTime(timeExpression)
		if err != nil {
			return ZERO_TIME, err
		}
		return time.Unix(seconds, 0), nil
	}

	leftCondition, _ := condition.GetLeftWhereCondition()
	startTimeLeft, err := GetTime(leftCondition, isParsingStartTime)
	if err != nil {
		return ZERO_TIME, err
	}
	startTimeRight, err := GetTime(condition.Right, isParsingStartTime)
	if err != nil {
		return ZERO_TIME, err
	}

	if condition.Operation == "OR" && (startTimeLeft != ZERO_TIME || startTimeRight != ZERO_TIME) {
		// we can't have two start times or'd together
		return ZERO_TIME, fmt.Errorf("Invalid where clause, time must appear twice to specify start and end time")
	}

	if startTimeLeft == ZERO_TIME {
		return startTimeRight, nil
	}
	if startTimeRight == ZERO_TIME {
		return startTimeLeft, nil
	}
	if isParsingStartTime && startTimeLeft.Unix() < startTimeRight.Unix() {
		return startTimeLeft, nil
	}
	if !isParsingStartTime && startTimeLeft.Unix() > startTimeRight.Unix() {
		return startTimeLeft, nil
	}
	return startTimeRight, nil
}

// Returns the start time of the query. Queries can only have
// one condition of the form time > start_time
func (self *Query) GetStartTime() time.Time {
	return self.startTime
}

// Returns the start time of the query. Queries can only have
// one condition of the form time > start_time
func (self *Query) GetEndTime() time.Time {
	return self.endTime
}

// Returns a mapping from the time series names (or regex) to the
// column names that are references
func (self *Query) GetReferencedColumnNames() map[string]string {
	return nil
}
