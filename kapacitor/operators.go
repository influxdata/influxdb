package kapacitor

import "fmt"

const (
	GreaterThan      = "greater than"
	LessThan         = "less than"
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
	case GreaterThan:
		return ">", nil
	case LessThan:
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
