package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/cmd/influx/internal"
	"github.com/influxdata/platform/http"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
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
		"UserName",
		"Organization",
		"Bucket",
		"Token",
	)
	w.Write(map[string]interface{}{
		"UserID":       result.User.ID.String(),
		"UserName":     result.User.Name,
		"Organization": result.Org.Name,
		"Bucket":       result.Bucket.Name,
		"Token":        result.Auth.Token,
	})
	w.Flush()
}

func getOnboardingRequest() (or *platform.OnboardingRequest) {
	term := terminal.NewTerminal(struct {
		io.Reader
		io.Writer
	}{os.Stdin, os.Stdout}, " ")
	var confirmed bool
	for !confirmed {
		or = new(platform.OnboardingRequest)
		term.Write([]byte(promptWithColor(`Welcome to the influxdata platform!

Type cancel at anytime to terminate setup
`, term.Escape.Yellow, term)))
		or.User = getInput(term, "Please type your primary username:", "", false)
		or.Password = getInput(term, "Please type your password:", "", true)
		or.Org = getInput(term, "Please type your primary organization name.\r\nOr ENTER to use \"default\":", "default", false)
		or.Bucket = getInput(term, "Please type your primary bucket name.\r\nOr ENTER to use \"default\":", "default", false)

		confirmed = getConfirm(term, or)
	}

	return or
}

func promptWithColor(s string, color []byte, term *terminal.Terminal) string {
	return string(color) + s + string(term.Escape.Reset)
}

func getConfirm(term *terminal.Terminal, or *platform.OnboardingRequest) bool {
	oldState, err := terminal.MakeRaw(0)
	if err != nil {
		return false
	}
	defer terminal.Restore(0, oldState)
	term.Write([]byte(promptWithColor(fmt.Sprintf(`
You have entered:
    username:      %s
    organization:  %s
    bucket:        %s
`, or.User, or.Org, or.Bucket), term.Escape.Cyan, term)))
	prompt := promptWithColor("Confirm? (y/n): ", term.Escape.Red, term)
	term.SetPrompt(prompt)
	for {
		line, _ := term.ReadLine()
		line = strings.TrimSpace(line)
		switch line {
		case "y":
			return true
		case "n":
			return false
		case "cancel":
			terminal.Restore(0, oldState)
			os.Exit(0)
		default:
			continue
		}
	}
}

func getInput(term *terminal.Terminal, prompt, defaultValue string, isPassword bool) string {
	var line string
	oldState, err := terminal.MakeRaw(0)
	if err != nil {
		return ""
	}
	defer terminal.Restore(0, oldState)

	prompt = promptWithColor(prompt, term.Escape.Cyan, term)
	if isPassword {
		for {
			line, _ = term.ReadPassword(prompt)
			if strings.TrimSpace(line) == "" {
				continue
			}
			goto handleCancel
		}
	}
	for {
		term.SetPrompt(prompt)
		line, _ = term.ReadLine()
		line = strings.TrimSpace(line)
		if defaultValue != "" && line == "" {
			line = defaultValue
		} else if defaultValue == "" && line == "" {
			continue
		}
		goto handleCancel
	}

handleCancel:
	if line == "cancel" {
		terminal.Restore(0, oldState)
		os.Exit(0)
	}
	return line
}
