package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/cmd/influx/internal"
	"github.com/influxdata/platform/http"
	"github.com/spf13/cobra"
	"github.com/tcnksm/go-input"
)

// setup Command
var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Create default username, password, org, bucket...",
	Run:   setupF,
}

func setupF(cmd *cobra.Command, args []string) {
	// check if setup is allowed
	s := &http.SetupService{
		Addr: flags.host,
	}

	allowed, err := s.IsOnboarding(context.Background())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if !allowed {
		fmt.Println("Initialization has been already completed")
		os.Exit(0)
	}

	or := getOnboardingRequest()

	result, err := s.Generate(context.Background(), or)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"UserID",
		"Username",
		"Organization",
		"Bucket",
		"Token",
	)
	w.Write(map[string]interface{}{
		"UserID":       result.User.ID.String(),
		"Username":     result.User.Name,
		"Organization": result.Org.Name,
		"Bucket":       result.Bucket.Name,
		"Token":        result.Auth.Token,
	})
	w.Flush()
}

func getOnboardingRequest() (or *platform.OnboardingRequest) {
	ui := &input.UI{
		Writer: os.Stdout,
		Reader: os.Stdin,
	}
	var confirmed bool
	for !confirmed {
		or = new(platform.OnboardingRequest)
		fmt.Println(promptWithColor(`Welcome to InfluxDB 2.0!`, colorYellow))
		or.User = getInput(ui, "Please type your primary username", "")
		or.Password = getPassword(ui)
		or.Org = getInput(ui, "Please type your primary organization name.\r\nOr ENTER to use \"default\"", "default")
		or.Bucket = getInput(ui, "Please type your primary bucket name.\r\nOr ENTER to use \"default\"", "default")

		confirmed = getConfirm(ui, or)
	}

	return or
}

// vt100EscapeCodes
var (
	keyEscape    = byte(27)
	colorBlack   = []byte{keyEscape, '[', '3', '0', 'm'}
	colorRed     = []byte{keyEscape, '[', '3', '1', 'm'}
	colorGreen   = []byte{keyEscape, '[', '3', '2', 'm'}
	colorYellow  = []byte{keyEscape, '[', '3', '3', 'm'}
	colorBlue    = []byte{keyEscape, '[', '3', '4', 'm'}
	colorMagenta = []byte{keyEscape, '[', '3', '5', 'm'}
	colorCyan    = []byte{keyEscape, '[', '3', '6', 'm'}
	colorWhite   = []byte{keyEscape, '[', '3', '7', 'm'}
	keyReset     = []byte{keyEscape, '[', '0', 'm'}
)

func promptWithColor(s string, color []byte) string {
	return string(color) + s + string(keyReset)
}

func getConfirm(ui *input.UI, or *platform.OnboardingRequest) bool {
	fmt.Print(promptWithColor(fmt.Sprintf(`
You have entered:
    Username:      %s
    Organization:  %s
    Bucket:        %s
`, or.User, or.Org, or.Bucket), colorCyan))
	prompt := promptWithColor("Confirm? (y/n)", colorRed)
	result, err := ui.Ask(prompt, &input.Options{
		HideOrder: true,
	})
	if err != nil {
		os.Exit(1)
	}
	switch result {
	case "y":
		return true
	default:
		return false
	}
}

var errPasswordIsNotMatch = fmt.Errorf("passwords do not match")

func getPassword(ui *input.UI) (password string) {
	var err error
enterPasswd:
	query := promptWithColor("Please type your password", colorCyan)
	for {
		password, err = ui.Ask(query, &input.Options{
			Required:  true,
			HideOrder: true,
			Hide:      true,
		})
		switch err {
		case input.ErrInterrupted:
			os.Exit(1)
		default:
			if password = strings.TrimSpace(password); password == "" {
				continue
			}
		}
		break
	}
	query = promptWithColor("Please type your password again", colorCyan)
	for {
		_, err = ui.Ask(query, &input.Options{
			Required:  true,
			HideOrder: true,
			Hide:      true,
			ValidateFunc: func(s string) error {
				if s != password {
					return errPasswordIsNotMatch
				}
				return nil
			},
		})
		switch err {
		case input.ErrInterrupted:
			os.Exit(1)
		case nil:
			break
		default:
			fmt.Println(promptWithColor("Passwords do not match!", colorRed))
			goto enterPasswd
		}
		break
	}
	return password
}

func getInput(ui *input.UI, prompt, defaultValue string) string {
	option := &input.Options{
		Required:  true,
		HideOrder: true,
	}
	if defaultValue != "" {
		option.Default = defaultValue
		option.HideDefault = true
	}
	prompt = promptWithColor(prompt, colorCyan)
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
