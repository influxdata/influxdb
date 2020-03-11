package password

import (
	"testing"
)

func TestIsValidPassword(t *testing.T) {
	tables := []struct {
		password string
		err      error
	}{
		{"pass", EPasswordTooShort},
		{"password", ENotEnoughCharacterClasses},
		{"passWord", ENotEnoughCharacterClasses},
		{"passWord120", nil},
		{"password120%", nil},
		{"passWord.", nil},
	}

	for _, table := range tables {
		err := IsValidPassword(table.password)

		if err != table.err {
			t.Errorf("IsValidPassword(%v) did not return correctly.\ngot: %v\nexpected: %v", table.password, err.Error(), table.err)
		}
	}
}
