package internal

import (
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/v2/task/options"
	"os"
	"strings"
	"time"

	"github.com/tcnksm/go-input"
)

const MinPasswordLen int = 8

var (
	ErrPasswordNotMatch   = fmt.Errorf("passwords do not match")
	ErrPasswordIsTooShort = fmt.Errorf("password is too short")
)

// vt100EscapeCodes
var (
	KeyEscape   = byte(27)
	ColorRed    = []byte{KeyEscape, '[', '3', '1', 'm'}
	ColorYellow = []byte{KeyEscape, '[', '3', '3', 'm'}
	ColorCyan   = []byte{KeyEscape, '[', '3', '6', 'm'}
	KeyReset    = []byte{KeyEscape, '[', '0', 'm'}
)

func PromptWithColor(s string, color []byte) []byte {
	bb := append(color, []byte(s)...)
	return append(bb, KeyReset...)
}

func GetConfirm(ui *input.UI, promptFunc func() string) bool {
	prompt := PromptWithColor("Confirm? (y/n)", ColorRed)
	for {
		ui.Writer.Write(PromptWithColor(promptFunc(), ColorCyan))
		result, err := ui.Ask(string(prompt), &input.Options{
			HideOrder: true,
		})
		if err != nil {
			return false
		}
		switch result {
		case "y":
			return true
		case "n":
			return false
		default:
			continue
		}
	}
}

func GetSecret(ui *input.UI) (secret string) {
	var err error
	query := string(PromptWithColor("Please type your secret", ColorCyan))
	for {
		secret, err = ui.Ask(query, &input.Options{
			Required:  true,
			HideOrder: true,
			Hide:      true,
			Mask:      false,
		})
		switch err {
		case input.ErrInterrupted:
			os.Exit(1)
		default:
			if secret = strings.TrimSpace(secret); secret == "" {
				continue
			}
		}
		break
	}
	return secret
}

func GetPassword(ui *input.UI, showNew bool) (password string) {
	newStr := ""
	if showNew {
		newStr = " new"
	}
	var err error
enterPassword:
	query := string(PromptWithColor("Please type your"+newStr+" password", ColorCyan))
	for {
		password, err = ui.Ask(query, &input.Options{
			Required:  true,
			HideOrder: true,
			Hide:      true,
			Mask:      false,
			ValidateFunc: func(s string) error {
				if len(s) < 8 {
					return ErrPasswordIsTooShort
				}
				return nil
			},
		})
		switch err {
		case input.ErrInterrupted:
			os.Exit(1)
		case ErrPasswordIsTooShort:
			ui.Writer.Write(PromptWithColor("Password too short - minimum length is 8 characters.\n\r", ColorRed))
			continue
		default:
			if password = strings.TrimSpace(password); password == "" {
				continue
			}
		}
		break
	}
	query = string(PromptWithColor("Please type your"+newStr+" password again", ColorCyan))
	for {
		_, err = ui.Ask(query, &input.Options{
			Required:  true,
			HideOrder: true,
			Hide:      true,
			ValidateFunc: func(s string) error {
				if s != password {
					return ErrPasswordNotMatch
				}
				return nil
			},
		})
		switch err {
		case input.ErrInterrupted:
			os.Exit(1)
		case nil:
			// Nothing.
		default:
			ui.Writer.Write(PromptWithColor("Passwords do not match.\n", ColorRed))
			goto enterPassword
		}
		break
	}
	return password
}

func GetInput(ui *input.UI, prompt, defaultValue string) string {
	option := &input.Options{
		Required:  true,
		HideOrder: true,
	}
	if defaultValue != "" {
		option.Default = defaultValue
		option.HideDefault = true
	}
	prompt = string(PromptWithColor(prompt, ColorCyan))
	for {
		line, err := ui.Ask(prompt, option)
		switch err {
		case input.ErrInterrupted:
			os.Exit(1)
		default:
			if line = strings.TrimSpace(line); line == "" {
				continue
			}
			return line
		}
	}
}

func RawDurationToTimeDuration(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}

	if dur, err := time.ParseDuration(raw); err == nil {
		return dur, nil
	}

	retention, err := options.ParseSignedDuration(raw)
	if err != nil {
		return 0, err
	}

	const (
		day  = 24 * time.Hour
		week = 7 * day
	)

	var dur time.Duration
	for _, d := range retention.Values {
		if d.Magnitude < 0 {
			return 0, errors.New("must be greater than 0")
		}
		mag := time.Duration(d.Magnitude)
		switch d.Unit {
		case "w":
			dur += mag * week
		case "d":
			dur += mag * day
		case "m":
			dur += mag * time.Minute
		case "s":
			dur += mag * time.Second
		case "ms":
			dur += mag * time.Minute
		case "us":
			dur += mag * time.Microsecond
		case "ns":
			dur += mag * time.Nanosecond
		default:
			return 0, errors.New("duration must be week(w), day(d), hour(h), min(m), sec(s), millisec(ms), microsec(us), or nanosec(ns)")
		}
	}
	return dur, nil
}
