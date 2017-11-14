package kapacitor

import (
	"fmt"
)

const (
	greaterThan      = "greater than"
	lessThan         = "less than"
	LessThanEqual    = "equal to or less than"
	GreaterThanEqual = "equal to or greater"
	Equal            = "equal to"
	NotEqual         = "not equal to"
	InsideRange      = "inside range"
	OutsideRange     = "outside range"
)

// kapaOperator converts UI strings to kapacitor operators
func kapaOperator(operator string) (string, error) {
	switch operator {
	case greaterThan:
		return ">", nil
	case lessThan:
		return "<", nil
	case LessThanEqual:
		return "<=", nil
	case GreaterThanEqual:
		return ">=", nil
	case Equal:
		return "==", nil
	case NotEqual:
		return "!=", nil
	default:
		return "", fmt.Errorf("invalid operator: %s is unknown", operator)
	}
}

func chronoOperator(operator string) (string, error) {
	switch operator {
	case ">":
		return greaterThan, nil
	case "<":
		return lessThan, nil
	case "<=":
		return LessThanEqual, nil
	case ">=":
		return GreaterThanEqual, nil
	case "==":
		return Equal, nil
	case "!=":
		return NotEqual, nil
	default:
		return "", fmt.Errorf("invalid operator: %s is unknown", operator)
	}
}

func rangeOperators(operator string) ([]string, error) {
	switch operator {
	case InsideRange:
		return []string{">=", "AND", "<="}, nil
	case OutsideRange:
		return []string{"<", "OR", ">"}, nil
	default:
		return nil, fmt.Errorf("invalid operator: %s is unknown", operator)
	}
}

func chronoRangeOperators(ops []string) (string, error) {
	if len(ops) != 3 {
		return "", fmt.Errorf("Unknown operators")
	}
	if ops[0] == ">=" && ops[1] == "AND" && ops[2] == "<=" {
		return InsideRange, nil
	} else if ops[0] == "<" && ops[1] == "OR" && ops[2] == ">" {
		return OutsideRange, nil
	}
	return "", fmt.Errorf("Unknown operators")
}
