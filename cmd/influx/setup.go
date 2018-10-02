package main

import (
	"context"
	"fmt"
	"io"
	"os"

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
		os.Exit(1)
	}

	or := new(platform.OnboardingRequest)

	fmt.Println("Welcome to influxdata platform!")
	or.User = getInput("Please type your primary username.\r\nOr ENTER to use \"admin\":", "admin", false)
	or.Password = getInput("Please type your password:", "", true)
	or.Org = getInput("Please type your primary organization name.\r\nOr ENTER to use \"default\":", "default", false)
	or.Bucket = getInput("Please type your primary bucket name.\r\nOr ENTER to use \"default\":", "default", false)
	fmt.Println(or)

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

func getInput(prompt, defaultValue string, isPassword bool) string {
	var line string
	oldState, err := terminal.MakeRaw(0)
	if err != nil {
		return ""
	}
	defer terminal.Restore(0, oldState)
	term := terminal.NewTerminal(struct {
		io.Reader
		io.Writer
	}{os.Stdin, os.Stdout}, "")
	prompt = string(term.Escape.Cyan) + prompt + " " + string(term.Escape.Reset)
	if isPassword {
		for {
			line, _ = term.ReadPassword(prompt)
			if line == "" {
				continue
			}
			return line
		}
	}
	term.SetPrompt(prompt)
	if line, _ = term.ReadLine(); line == "" {
		line = defaultValue
	}
	return line
}
