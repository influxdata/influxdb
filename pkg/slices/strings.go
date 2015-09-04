package slices

import "strings"

func Union(setA, setB []string, ignoreCase bool) []string {
	for _, b := range setB {
		if ignoreCase {
			if !ExistsIgnoreCase(setA, b) {
				setA = append(setA, b)
			}
			continue
		}
		if !Exists(setA, b) {
			setA = append(setA, b)
		}
	}
	return setA
}

func Exists(set []string, find string) bool {
	for _, s := range set {
		if s == find {
			return true
		}
	}
	return false
}

func ExistsIgnoreCase(set []string, find string) bool {
	find = strings.ToLower(find)
	for _, s := range set {
		if strings.ToLower(s) == find {
			return true
		}
	}
	return false
}
