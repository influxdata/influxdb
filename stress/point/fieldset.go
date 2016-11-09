package point

import "strings"

// TODO: add correct error handling/panic when appropriate
func generateFieldSet(s string) ([]string, []string) {
	ints := []string{}
	floats := []string{}

	parts := strings.Split(s, ",")

	for _, part := range parts {
		if strings.HasSuffix(part, "i") {
			ints = append(ints, strings.Split(part, "=")[0])
			continue
		}
		floats = append(floats, strings.Split(part, "=")[0])
	}

	return ints, floats
}
