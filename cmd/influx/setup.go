package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/config"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
	input "github.com/tcnksm/go-input"
)

var setupFlags struct {
	bucket      string
	force       bool
	hideHeaders bool
	json        bool
	name        string
	org         string
	password    string
	retention   time.Duration
	token       string
	username    string
}

func cmdSetup(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("setup", nil, true)
	cmd.RunE = setupF
	cmd.Short = "Setup instance with initial user, org, bucket"

	cmd.Flags().StringVarP(&setupFlags.username, "username", "u", "", "primary username")
	cmd.Flags().StringVarP(&setupFlags.password, "password", "p", "", "password for username")
	cmd.Flags().StringVarP(&setupFlags.token, "token", "t", "", "token for username, else auto-generated")
	cmd.Flags().StringVarP(&setupFlags.org, "org", "o", "", "primary organization name")
	cmd.Flags().StringVarP(&setupFlags.bucket, "bucket", "b", "", "primary bucket name")
	cmd.Flags().StringVarP(&setupFlags.name, "name", "n", "", "config name, only required if you already have existing configs")
	cmd.Flags().DurationVarP(&setupFlags.retention, "retention", "r", -1, "Duration bucket will retain data. 0 is infinite. Default is 0.")
	cmd.Flags().BoolVarP(&setupFlags.force, "force", "f", false, "skip confirmation prompt")
	registerPrintOptions(cmd, &setupFlags.hideHeaders, &setupFlags.json)

	cmd.AddCommand(
		cmdSetupUser(opt),
	)
	return cmd
}

func cmdSetupUser(opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("user", nil, true)
	cmd.RunE = setupUserF
	cmd.Short = "Setup instance with user, org, bucket"

	cmd.Flags().StringVarP(&setupFlags.username, "username", "u", "", "primary username")
	cmd.Flags().StringVarP(&setupFlags.password, "password", "p", "", "password for username")
	cmd.Flags().StringVarP(&setupFlags.token, "token", "t", "", "token for username, else auto-generated")
	cmd.Flags().StringVarP(&setupFlags.org, "org", "o", "", "primary organization name")
	cmd.Flags().StringVarP(&setupFlags.bucket, "bucket", "b", "", "primary bucket name")
	cmd.Flags().StringVarP(&setupFlags.name, "name", "n", "", "config name, only required if you already have existing configs")
	cmd.Flags().DurationVarP(&setupFlags.retention, "retention", "r", -1, "Duration bucket will retain data. 0 is infinite. Default is 0.")
	cmd.Flags().BoolVarP(&setupFlags.force, "force", "f", false, "skip confirmation prompt")
	registerPrintOptions(cmd, &setupFlags.hideHeaders, &setupFlags.json)

	return cmd
}

func setupUserF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for setup command")
	}

	// check if setup is allowed
	client, err := newHTTPClient()
	if err != nil {
		return err
	}
	s := tenant.OnboardClientService{
		Client: client,
	}

	req, err := onboardingRequest()
	if err != nil {
		return fmt.Errorf("failed to retrieve data to setup instance: %v", err)
	}

	result, err := s.OnboardUser(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to setup instance: %v", err)
	}

	w := cmd.OutOrStdout()
	if setupFlags.json {
		return writeJSON(w, map[string]interface{}{
			"user":         result.User.Name,
			"organization": result.Org.Name,
			"bucket":       result.Bucket.Name,
		})
	}

	tabW := internal.NewTabWriter(w)
	defer tabW.Flush()

	tabW.HideHeaders(setupFlags.hideHeaders)

	tabW.WriteHeaders("User", "Organization", "Bucket")
	tabW.Write(map[string]interface{}{
		"User":         result.User.Name,
		"Organization": result.Org.Name,
		"Bucket":       result.Bucket.Name,
	})

	return nil
}

func setupF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for setup command")
	}

	// check if setup is allowed
	client, err := newHTTPClient()
	if err != nil {
		return err
	}

	s := tenant.OnboardClientService{
		Client: client,
	}
	allowed, err := s.IsOnboarding(context.Background())
	if err != nil {
		return fmt.Errorf("failed to determine if instance has been configured: %v", err)
	}
	if !allowed {
		return fmt.Errorf("instance at %q has already been setup", flags.Host)
	}

	dPath, dir, err := defaultConfigPath()
	if err != nil {
		return err
	}

	localSVC := config.NewLocalConfigSVC(dPath, dir)

	existingConfigs := make(config.Configs)
	if _, err := os.Stat(dPath); err == nil {
		existingConfigs, _ = localSVC.ListConfigs()
		// ignore the error if found nothing
		if setupFlags.name == "" {
			return errors.New("flag name is required if you already have existing configs")
		}
		if _, ok := existingConfigs[setupFlags.name]; ok {
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  fmt.Sprintf("config name %q already existed", setupFlags.name),
			}
		}
	}

	req, err := onboardingRequest()
	if err != nil {
		return fmt.Errorf("failed to retrieve data to setup instance: %v", err)
	}

	result, err := s.OnboardInitialUser(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to setup instance: %v", err)
	}

	p := config.DefaultConfig
	p.Token = result.Auth.Token
	p.Org = result.Org.Name
	if len(existingConfigs) > 0 {
		p.Name = setupFlags.name
	}
	if flags.Host != "" {
		p.Host = flags.Host
	}

	if _, err = localSVC.CreateConfig(p); err != nil {
		return fmt.Errorf("failed to write config to path %q: %v", dPath, err)
	}

	fmt.Println(string(promptWithColor(fmt.Sprintf("Config %s has been stored in %s.", p.Name, dPath), colorCyan)))

	w := cmd.OutOrStdout()
	if setupFlags.json {
		return writeJSON(w, map[string]interface{}{
			"user":         result.User.Name,
			"organization": result.Org.Name,
			"bucket":       result.Bucket.Name,
		})
	}

	tabW := internal.NewTabWriter(w)
	defer tabW.Flush()

	tabW.HideHeaders(setupFlags.hideHeaders)

	tabW.WriteHeaders("User", "Organization", "Bucket")
	tabW.Write(map[string]interface{}{
		"User":         result.User.Name,
		"Organization": result.Org.Name,
		"Bucket":       result.Bucket.Name,
	})

	return nil
}

func isInteractive() bool {
	return !setupFlags.force ||
		setupFlags.username == "" ||
		setupFlags.password == "" ||
		setupFlags.org == "" ||
		setupFlags.bucket == ""
}

func onboardingRequest() (*influxdb.OnboardingRequest, error) {
	if isInteractive() {
		return interactive()
	}
	return nonInteractive()
}

func nonInteractive() (*influxdb.OnboardingRequest, error) {
	req := &influxdb.OnboardingRequest{
		User:     setupFlags.username,
		Password: setupFlags.password,
		Token:    setupFlags.token,
		Org:      setupFlags.org,
		Bucket:   setupFlags.bucket,
		// TODO: this manipulation is required by the API, something that
		// 	we should fixup to be a duration instead
		RetentionPeriod: uint(setupFlags.retention / time.Hour),
	}

	if setupFlags.retention < 0 {
		req.RetentionPeriod = influxdb.InfiniteRetention
	}
	return req, nil
}

func interactive() (req *influxdb.OnboardingRequest, err error) {
	ui := &input.UI{
		Writer: os.Stdout,
		Reader: os.Stdin,
	}
	req = new(influxdb.OnboardingRequest)
	fmt.Println(string(promptWithColor(`Welcome to InfluxDB 2.0!`, colorYellow)))
	if setupFlags.username != "" {
		req.User = setupFlags.username
	} else {
		req.User = getInput(ui, "Please type your primary username", "")
	}
	if setupFlags.password != "" {
		req.Password = setupFlags.password
	} else {
		req.Password = getPassword(ui, false)
	}
	if setupFlags.token != "" {
		req.Token = setupFlags.token
		// else auto-generated by service
	}
	if setupFlags.org != "" {
		req.Org = setupFlags.org
	} else {
		req.Org = getInput(ui, "Please type your primary organization name", "")
	}
	if setupFlags.bucket != "" {
		req.Bucket = setupFlags.bucket
	} else {
		req.Bucket = getInput(ui, "Please type your primary bucket name", "")
	}
	if setupFlags.retention >= 0 {
		req.RetentionPeriod = uint(setupFlags.retention)
	} else {
		for {
			rpStr := getInput(ui, "Please type your retention period in hours.\r\nOr press ENTER for infinite.", strconv.Itoa(influxdb.InfiniteRetention))
			rp, err := strconv.Atoi(rpStr)
			if rp >= 0 && err == nil {
				req.RetentionPeriod = uint(rp)
				break
			}
		}
	}

	if !setupFlags.force {
		if confirmed := getConfirm(ui, req); !confirmed {
			return nil, fmt.Errorf("setup was canceled")
		}
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

func promptWithColor(s string, color []byte) []byte {
	bb := append(color, []byte(s)...)
	return append(bb, keyReset...)
}

func getConfirm(ui *input.UI, or *influxdb.OnboardingRequest) bool {
	prompt := promptWithColor("Confirm? (y/n)", colorRed)
	for {
		rp := "infinite"
		if or.RetentionPeriod > 0 {
			rp = fmt.Sprintf("%d hrs", or.RetentionPeriod/uint(time.Hour))
		}
		ui.Writer.Write(promptWithColor(fmt.Sprintf(`
You have entered:
  Username:          %s
  Organization:      %s
  Bucket:            %s
  Retention Period:  %s
`, or.User, or.Org, or.Bucket, rp), colorCyan))
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

var errPasswordNotMatch = fmt.Errorf("passwords do not match")

var errPasswordIsTooShort error = fmt.Errorf("password is too short")

func getSecret(ui *input.UI) (secret string) {
	var err error
	query := string(promptWithColor("Please type your secret", colorCyan))
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

func getPassword(ui *input.UI, showNew bool) (password string) {
	newStr := ""
	if showNew {
		newStr = " new"
	}
	var err error
enterPassword:
	query := string(promptWithColor("Please type your"+newStr+" password", colorCyan))
	for {
		password, err = ui.Ask(query, &input.Options{
			Required:  true,
			HideOrder: true,
			Hide:      true,
			Mask:      false,
			ValidateFunc: func(s string) error {
				if len(s) < 8 {
					return errPasswordIsTooShort
				}
				return nil
			},
		})
		switch err {
		case input.ErrInterrupted:
			os.Exit(1)
		case errPasswordIsTooShort:
			ui.Writer.Write(promptWithColor("Password too short - minimum length is 8 characters!\n\r", colorRed))
			continue
		default:
			if password = strings.TrimSpace(password); password == "" {
				continue
			}
		}
		break
	}
	query = string(promptWithColor("Please type your"+newStr+" password again", colorCyan))
	for {
		_, err = ui.Ask(query, &input.Options{
			Required:  true,
			HideOrder: true,
			Hide:      true,
			ValidateFunc: func(s string) error {
				if s != password {
					return errPasswordNotMatch
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
			ui.Writer.Write(promptWithColor("Passwords do not match!\n", colorRed))
			goto enterPassword
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
	prompt = string(promptWithColor(prompt, colorCyan))
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
