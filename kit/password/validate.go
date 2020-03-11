package password

import (
	"fmt"
	"unicode"
)

const (
	MinPasswordLength   = 8
	MinCharacterClasses = 3
)

var (
	EPasswordTooShort          = fmt.Errorf("Password does not meet minimum length requirements (%d characters)", MinPasswordLength)
	ENotEnoughCharacterClasses = fmt.Errorf("Password does not use enough character classes (%d required)", MinCharacterClasses)
)

func IsValidPassword(password string) error {
	if len(password) < MinPasswordLength {
		return EPasswordTooShort
	}

	if countCharacterClasses(password) < MinCharacterClasses {
		return ENotEnoughCharacterClasses
	}

	return nil
}

func countCharacterClasses(password string) int {
	var (
		hasUpperLetter = 0
		hasLowerLetter = 0
		hasNumeric     = 0
		hasSymbol      = 0
	)

	for _, char := range password {
		if unicode.IsUpper(char) {
			hasUpperLetter = 1
		}
		if unicode.IsLower(char) {
			hasLowerLetter = 1
		}

		if unicode.IsNumber(char) || unicode.IsDigit(char) {
			hasNumeric = 1
		}

		if unicode.IsPunct(char) || unicode.IsSymbol(char) || unicode.IsMark(char) {
			hasSymbol = 1
		}
	}

	return hasLowerLetter + hasUpperLetter + hasNumeric + hasSymbol
}
