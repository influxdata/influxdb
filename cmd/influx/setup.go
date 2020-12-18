package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/config"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	internal2 "github.com/influxdata/influxdb/v2/cmd/internal"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
	"github.com/tcnksm/go-input"
)

var setupFlags struct {
	bucket      string
	force       bool
	hideHeaders bool
	json        bool
	name        string
	org         string
	password    string
	retention   string
	token       string
	username    string
}

func cmdSetup(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("setup", nil, true)
	cmd.RunE = setupF
	cmd.Short = "Setup instance with initial user, org, bucket"

	f.registerFlags(opt.viper, cmd, "token")
	cmd.Flags().StringVarP(&setupFlags.username, "username", "u", "", "primary username")
	cmd.Flags().StringVarP(&setupFlags.password, "password", "p", "", "password for username")
	cmd.Flags().StringVarP(&setupFlags.token, "token", "t", "", "token for username, else auto-generated")
	cmd.Flags().StringVarP(&setupFlags.org, "org", "o", "", "primary organization name")
	cmd.Flags().StringVarP(&setupFlags.bucket, "bucket", "b", "", "primary bucket name")
	cmd.Flags().StringVarP(&setupFlags.name, "name", "n", "", "config name, only required if you already have existing configs")
	cmd.Flags().StringVarP(&setupFlags.retention, "retention", "r", "", "Duration bucket will retain data. 0 is infinite. Default is 0.")
	cmd.Flags().BoolVarP(&setupFlags.force, "force", "f", false, "skip confirmation prompt")
	registerPrintOptions(opt.viper, cmd, &setupFlags.hideHeaders, &setupFlags.json)

	cmd.AddCommand(
		cmdSetupUser(f, opt),
	)
	return cmd
}

func cmdSetupUser(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("user", nil, true)
	cmd.RunE = setupUserF
	cmd.Short = "Setup instance with user, org, bucket"

	f.registerFlags(opt.viper, cmd, "token")
	cmd.Flags().StringVarP(&setupFlags.username, "username", "u", "", "primary username")
	cmd.Flags().StringVarP(&setupFlags.password, "password", "p", "", "password for username")
	cmd.Flags().StringVarP(&setupFlags.token, "token", "t", "", "token for username, else auto-generated")
	cmd.Flags().StringVarP(&setupFlags.org, "org", "o", "", "primary organization name")
	cmd.Flags().StringVarP(&setupFlags.bucket, "bucket", "b", "", "primary bucket name")
	cmd.Flags().StringVarP(&setupFlags.name, "name", "n", "", "config name, only required if you already have existing configs")
	cmd.Flags().StringVarP(&setupFlags.retention, "retention", "r", "", "Duration bucket will retain data. 0 is infinite. Default is 0.")
	cmd.Flags().BoolVarP(&setupFlags.force, "force", "f", false, "skip confirmation prompt")
	registerPrintOptions(opt.viper, cmd, &setupFlags.hideHeaders, &setupFlags.json)

	return cmd
}

func setupUserF(cmd *cobra.Command, args []string) error {
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
	dPath, dir := flags.filepath, filepath.Dir(flags.filepath)
	if dPath == "" || dir == "" {
		return errors.New("a valid configurations path must be provided")
	}
	localConfigSVC := config.NewLocalConfigSVC(dPath, dir)

	// check if setup is allowed
	client, err := newHTTPClient()
	if err != nil {
		return err
	}

	s := tenant.OnboardClientService{
		Client: client,
	}
	activeConfig := flags.config()
	allowed, err := s.IsOnboarding(context.Background())
	if err != nil {
		return fmt.Errorf("failed to determine if instance has been configured: %v", err)
	}
	if !allowed {
		return fmt.Errorf("instance at %q has already been setup", activeConfig.Host)
	}

	if err := validateNoNameCollision(localConfigSVC, setupFlags.name); err != nil {
		return err
	}

	req, err := onboardingRequest()
	if err != nil {
		return fmt.Errorf("failed to setup instance: %v", err)
	}

	result, err := s.OnboardInitialUser(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to setup instance: %v", err)
	}

	p := config.DefaultConfig
	p.Token = result.Auth.Token
	p.Org = result.Org.Name
	if setupFlags.name != "" {
		p.Name = setupFlags.name
	}
	if activeConfig.Host != "" {
		p.Host = activeConfig.Host
	}

	if _, err = localConfigSVC.CreateConfig(p); err != nil {
		return fmt.Errorf("failed to write config to path %q: %v", dPath, err)
	}

	fmt.Println(string(internal2.PromptWithColor(fmt.Sprintf("Config %s has been stored in %s.", p.Name, dPath), internal2.ColorCyan)))

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

// validateNoNameCollision asserts that there isn't already a local config with a given name.
func validateNoNameCollision(localConfigSvc config.Service, configName string) error {
	existingConfigs, err := localConfigSvc.ListConfigs()
	if err != nil {
		return fmt.Errorf("error checking existing configs: %v", err)
	}
	if len(existingConfigs) == 0 {
		return nil
	}

	// If there are existing configs then require that a name be
	// specified in order to distinguish this new config from what's
	// there already.
	if configName == "" {
		return errors.New("flag name is required if you already have existing configs")
	}
	if _, ok := existingConfigs[configName]; ok {
		return fmt.Errorf("config name %q already exists", configName)
	}

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
	if len(setupFlags.password) < internal2.MinPasswordLen {
		return nil, internal2.ErrPasswordIsTooShort
	}

	req := &influxdb.OnboardingRequest{
		User:            setupFlags.username,
		Password:        setupFlags.password,
		Token:           setupFlags.token,
		Org:             setupFlags.org,
		Bucket:          setupFlags.bucket,
		RetentionPeriod: influxdb.InfiniteRetention,
	}

	dur, err := internal2.RawDurationToTimeDuration(setupFlags.retention)
	if err != nil {
		return nil, err
	}
	if dur > 0 {
		req.RetentionPeriod = dur
	}
	return req, nil
}

func interactive() (req *influxdb.OnboardingRequest, err error) {
	ui := &input.UI{
		Writer: os.Stdout,
		Reader: os.Stdin,
	}
	req = new(influxdb.OnboardingRequest)
	fmt.Println(string(internal2.PromptWithColor(`Welcome to InfluxDB 2.0!`, internal2.ColorYellow)))
	if setupFlags.username != "" {
		req.User = setupFlags.username
	} else {
		req.User = internal2.GetInput(ui, "Please type your primary username", "")
	}
	if setupFlags.password != "" && len(setupFlags.password) >= internal2.MinPasswordLen {
		req.Password = setupFlags.password
	} else {
		req.Password = internal2.GetPassword(ui, false)
	}
	if setupFlags.token != "" {
		req.Token = setupFlags.token
		// else auto-generated by service
	}
	if setupFlags.org != "" {
		req.Org = setupFlags.org
	} else {
		req.Org = internal2.GetInput(ui, "Please type your primary organization name", "")
	}
	if setupFlags.bucket != "" {
		req.Bucket = setupFlags.bucket
	} else {
		req.Bucket = internal2.GetInput(ui, "Please type your primary bucket name", "")
	}

	if setupFlags.retention != "" {
		dur, err := internal2.RawDurationToTimeDuration(setupFlags.retention)
		if err != nil {
			return nil, err
		}
		req.RetentionPeriod = dur
	} else {
		for {
			rpStr := internal2.GetInput(ui, "Please type your retention period in hours.\r\nOr press ENTER for infinite.", strconv.Itoa(influxdb.InfiniteRetention))
			rp, err := strconv.Atoi(rpStr)
			if rp >= 0 && err == nil {
				req.RetentionPeriod = time.Duration(rp) * time.Hour
				break
			}
		}
	}

	if !setupFlags.force {
		if confirmed := internal2.GetConfirm(ui, func() string {
			rp := "infinite"
			if req.RetentionPeriod > 0 {
				rp = fmt.Sprintf("%d hrs", req.RetentionPeriod/time.Hour)
			}
			return fmt.Sprintf(`
You have entered:
  Username:          %s
  Organization:      %s
  Bucket:            %s
  Retention Period:  %s
`, req.User, req.Org, req.Bucket, rp)
		}); !confirmed {
			return nil, fmt.Errorf("setup was canceled")
		}
	}

	return req, nil
}
