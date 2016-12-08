package cli

import (
	"bytes"
	"fmt"
)

func parseDatabaseAndRetentionPolicy(stmt []byte) (string, string, error) {
	var db, rp []byte
	var quoted bool
	var seperatorCount int

	stmt = bytes.TrimSpace(stmt)

	for _, b := range stmt {
		if b == '"' {
			quoted = !quoted
			continue
		}
		if b == '.' && !quoted {
			seperatorCount++
			if seperatorCount > 1 {
				return "", "", fmt.Errorf("unable to parse database and retention policy from %s", string(stmt))
			}
			continue
		}
		if seperatorCount == 1 {
			rp = append(rp, b)
			continue
		}
		db = append(db, b)
	}
	return string(db), string(rp), nil
}
