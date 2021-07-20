package upgrade

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/influxdata/influx-cli/v2/clients"
	"github.com/influxdata/influx-cli/v2/clients/setup"
	"github.com/influxdata/influx-cli/v2/config"
	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

func setupAdmin(ctx context.Context, v2 *influxDBv2, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	res, err := v2.onboardSvc.OnboardInitialUser(ctx, req)

	if err != nil {
		return nil, fmt.Errorf("onboarding error: %w", err)
	}
	return res, nil
}

func onboardingRequest(cli clients.CLI, options *options) (*influxdb.OnboardingRequest, error) {
	setupClient := setup.Client{CLI: cli}
	cliReq, err := setupClient.OnboardingRequest(&setup.Params{
		Username:  options.target.userName,
		Password:  options.target.password,
		AuthToken: options.target.token,
		Org:       options.target.orgName,
		Bucket:    options.target.bucket,
		Retention: options.target.retention,
		Force:     options.force,
	})
	if err != nil {
		return nil, err
	}
	req := influxdb.OnboardingRequest{
		User:   cliReq.Username,
		Org:    cliReq.Org,
		Bucket: cliReq.Bucket,
	}
	if cliReq.Password != nil {
		req.Password = *cliReq.Password
	}
	if cliReq.RetentionPeriodSeconds != nil {
		req.RetentionPeriodSeconds = *cliReq.RetentionPeriodSeconds
	}
	if cliReq.Token != nil {
		req.Token = *cliReq.Token
	}
	return &req, nil
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
