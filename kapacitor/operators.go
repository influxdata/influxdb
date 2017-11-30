package kapacitor

import (
	"fmt"
)

const (
	greaterThan      = "greater than"
	lessThan         = "less than"
	lessThanEqual    = "equal to or less than"
	greaterThanEqual = "equal to or greater"
	equal            = "equal to"
	notEqual         = "not equal to"
	insideRange      = "inside range"
	outsideRange     = "outside range"
)

// kapaOperator converts UI strings to kapacitor operators
func kapaOperator(operator string) (string, error) {
	switch operator {
	case greaterThan:
		return ">", nil
	case lessThan:
		return "<", nil
	case lessThanEqual:
		return "<=", nil
	case greaterThanEqual:
		return ">=", nil
	case equal:
		return "==", nil
	case notEqual:
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
		return lessThanEqual, nil
	case ">=":
		return greaterThanEqual, nil
	case "==":
		return equal, nil
	case "!=":
		return notEqual, nil
	default:
		return "", fmt.Errorf("invalid operator: %s is unknown", operator)
	}
}

func rangeOperators(operator string) ([]string, error) {
	switch operator {
	case insideRange:
		return []string{">=", "AND", "<="}, nil
	case outsideRange:
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
		return insideRange, nil
	} else if ops[0] == "<" && ops[1] == "OR" && ops[2] == ">" {
		return outsideRange, nil
	}
	return "", fmt.Errorf("Unknown operators")
}
