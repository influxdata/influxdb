package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
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
	if flags.local {
		fmt.Println("Local flag not supported for setup command")
		os.Exit(1)
	}

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

	req := getOnboardingRequest()

	result, err := s.Generate(context.Background(), req)
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

func getOnboardingRequest() (req *platform.OnboardingRequest) {
	ui := &input.UI{
		Writer: os.Stdout,
		Reader: os.Stdin,
	}
	req = new(platform.OnboardingRequest)
	fmt.Println(promptWithColor(`Welcome to InfluxDB 2.0!`, colorYellow))
	req.User = getInput(ui, "Please type your primary username", "")
	req.Password = getPassword(ui)
	req.Org = getInput(ui, "Please type your primary organization name.", "")
	req.Bucket = getInput(ui, "Please type your primary bucket name.", "")
	for {
		rpStr := getInput(ui, "Please type your retention period in hours (exp 168 for 1 week).\r\nOr press ENTER for infinite.", strconv.Itoa(platform.InfiniteRetention))
		rp, err := strconv.Atoi(rpStr)
		if rp >= 0 && err == nil {
			req.RetentionPeriod = uint(rp)
			break
		}
	}

	if confirmed := getConfirm(ui, req); !confirmed {
		fmt.Println("Setup is canceled.")
		// user cancel
		os.Exit(0)
	}

	return req
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
			// interrupt
			os.Exit(1)
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
