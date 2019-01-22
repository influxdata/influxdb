package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
	input "github.com/tcnksm/go-input"
)

// setup Command
var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Setup instance with initial user, org, bucket",
	RunE:  wrapErrorFmt(setupF),
}

func setupF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for setup command")
	}

	// check if setup is allowed
	s := &http.SetupService{
		Addr: flags.host,
	}

	allowed, err := s.IsOnboarding(context.Background())
	if err != nil {
		return fmt.Errorf("failed to determine if instance has been configured: %v", err)
	}
	if !allowed {
		return fmt.Errorf("instance at %q has already been setup", flags.host)
	}

	req, err := getOnboardingRequest()
	if err != nil {
		return fmt.Errorf("failed to retrieve data to setup instance: %v", err)
	}

	result, err := s.Generate(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to setup instance: %v", err)
	}
	writeTokenToPath(result.Auth.Token, defaultTokenPath())
	fmt.Println(promptWithColor("Your token has been stored in "+defaultTokenPath()+".", colorCyan))

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"User",
		"Organization",
		"Bucket",
	)
	w.Write(map[string]interface{}{
		"User":         result.User.Name,
		"Organization": result.Org.Name,
		"Bucket":       result.Bucket.Name,
	})

	w.Flush()

	return nil
}

func getOnboardingRequest() (req *platform.OnboardingRequest, err error) {
	ui := &input.UI{
		Writer: os.Stdout,
		Reader: os.Stdin,
	}
	req = new(platform.OnboardingRequest)
	fmt.Println(promptWithColor(`Welcome to InfluxDB 2.0!`, colorYellow))
	req.User = getInput(ui, "Please type your primary username", "")
	req.Password = getPassword(ui)
	req.Org = getInput(ui, "Please type your primary organization name", "")
	req.Bucket = getInput(ui, "Please type your primary bucket name", "")
	for {
		rpStr := getInput(ui, "Please type your retention period in hours (exp 168 for 1 week).\r\nOr press ENTER for infinite.", strconv.Itoa(platform.InfiniteRetention))
		rp, err := strconv.Atoi(rpStr)
		if rp >= 0 && err == nil {
			req.RetentionPeriod = uint(rp)
			break
		}
	}

	if confirmed := getConfirm(ui, req); !confirmed {
		return nil, fmt.Errorf("setup was canceled")
	}

	return req, nil
}

// vt100EscapeCodes
var (
	keyEscape   = byte(27)
	colorRed    = []byte{keyEscape, '[', '3', '1', 'm'}
	colorYellow = []byte{keyEscape, '[', '3', '3', 'm'}
	colorCyan   = []byte{keyEscape, '[', '3', '6', 'm'}
	keyReset    = []byte{keyEscape, '[', '0', 'm'}
)

func promptWithColor(s string, color []byte) string {
	return string(color) + s + string(keyReset)
}

func getConfirm(ui *input.UI, or *platform.OnboardingRequest) bool {
	prompt := promptWithColor("Confirm? (y/n)", colorRed)
	for {
		rp := "infinite"
		if or.RetentionPeriod > 0 {
			rp = fmt.Sprintf("%d hrs", or.RetentionPeriod)
		}
		fmt.Print(promptWithColor(fmt.Sprintf(`
You have entered:
  Username:          %s
  Organization:      %s
  Bucket:            %s
  Retention Period:  %s
`, or.User, or.Org, or.Bucket, rp), colorCyan))
		result, err := ui.Ask(prompt, &input.Options{
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
			// Nothing.
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
