package upgrade

import "go.uber.org/zap"

func buildLogger(options *logOptions, verbose bool) (*zap.Logger, error) {
	config := zap.NewProductionConfig()

	config.Level = zap.NewAtomicLevelAt(options.logLevel)
	if verbose {
		config.Level.SetLevel(zap.DebugLevel)
	}

	config.OutputPaths = append(config.OutputPaths, options.logPath)
	config.ErrorOutputPaths = append(config.ErrorOutputPaths, options.logPath)

	log, err := config.Build()
	if err != nil {
		return nil, err
	}
	if verbose {
		log.Warn("--verbose is deprecated, use --log-level=debug instead")
	}
	return log, nil
}
