package upgrade

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influx-cli/v2/config"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/internal"
	"github.com/tcnksm/go-input"
	"go.uber.org/zap"
)

func setupAdmin(ctx context.Context, v2 *influxDBv2, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	res, err := v2.onboardSvc.OnboardInitialUser(ctx, req)

	if err != nil {
		return nil, fmt.Errorf("onboarding error: %w", err)
	}
	return res, nil
}

func onboardingRequest(ui *input.UI, options *options) (*influxdb.OnboardingRequest, error) {
	if (options.force || len(options.target.password) > 0) && len(options.target.password) < internal.MinPasswordLen {
		return nil, internal.ErrPasswordIsTooShort
	}

	req := &influxdb.OnboardingRequest{
		User:                   options.target.userName,
		Password:               options.target.password,
		Org:                    options.target.orgName,
		Bucket:                 options.target.bucket,
		RetentionPeriodSeconds: influxdb.InfiniteRetention,
		Token:                  options.target.token,
	}

	if options.target.retention != "" {
		dur, err := internal.RawDurationToTimeDuration(options.target.retention)
		if err != nil {
			return nil, err
		}
		secs, nanos := math.Modf(dur.Seconds())
		if nanos > 0 {
			return nil, fmt.Errorf("retention policy %q is too precise, must be divisible by 1s", options.target.retention)
		}
		req.RetentionPeriodSeconds = int64(secs)
	}

	if options.force {
		return req, nil
	}

	fmt.Fprintln(ui.Writer, string(internal.PromptWithColor("Welcome to InfluxDB 2.0!", internal.ColorYellow)))
	if req.User == "" {
		req.User = internal.GetInput(ui, "Please type your primary username", "")
	}
	if req.Password == "" {
		req.Password = internal.GetPassword(ui, false)
	}
	if req.Org == "" {
		req.Org = internal.GetInput(ui, "Please type your primary organization name", "")
	}
	if req.Bucket == "" {
		req.Bucket = internal.GetInput(ui, "Please type your primary bucket name", "")
	}

	// Check the initial opts instead of the req to distinguish not-set from explicit 0 over the CLI.
	if options.target.retention == "" {
		infiniteStr := strconv.Itoa(influxdb.InfiniteRetention)
		for {
			rpStr := internal.GetInput(ui,
				"Please type your retention period in hours.\nOr press ENTER for infinite", infiniteStr)
			rp, err := strconv.Atoi(rpStr)
			if rp >= 0 && err == nil {
				req.RetentionPeriodSeconds = int64((time.Duration(rp) * time.Hour).Seconds())
				break
			}
		}
	}

	if confirmed := internal.GetConfirm(ui, func() string {
		rp := "infinite"
		if req.RetentionPeriodSeconds > 0 {
			rp = (time.Duration(req.RetentionPeriodSeconds) * time.Second).String()
		}
		return fmt.Sprintf(`
You have entered:
  Username:          %s
  Organization:      %s
  Bucket:            %s
  Retention Period:  %s
`, req.User, req.Org, req.Bucket, rp)
	}); !confirmed {
		return nil, errors.New("setup was canceled")
	}

	return req, nil
}

func saveLocalConfig(sourceOptions *optionsV1, targetOptions *optionsV2, log *zap.Logger) error {
	dPath, dir := targetOptions.cliConfigsPath, filepath.Dir(targetOptions.cliConfigsPath)
	if dPath == "" || dir == "" {
		return errors.New("a valid configurations path must be provided")
	}

	localConfigSVC := config.NewLocalConfigService(targetOptions.cliConfigsPath)
	p := config.DefaultConfig
	p.Token = targetOptions.token
	p.Org = targetOptions.orgName
	if sourceOptions.dbURL != "" {
		p.Host = sourceOptions.dbURL
	}
	if _, err := localConfigSVC.CreateConfig(p); err != nil {
		log.Error("failed to save CLI config", zap.String("path", dPath), zap.Error(err))
		return errors.New("failed to save CLI config")
	}
	log.Info("CLI config has been stored.", zap.String("path", dPath))

	return nil
}
