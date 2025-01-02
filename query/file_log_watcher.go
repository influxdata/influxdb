package query

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type FileLogWatcher struct {
	path     string
	currFile *os.File
	logger   *zap.Logger
	executor *Executor
}

func NewFileLogWatcher(e *Executor, path string, logger *zap.Logger) *FileLogWatcher {
	logFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		logger.Error("failed to open log file", zap.Error(err))
		return nil
	}

	existingCore := logger.Core()

	encoderConfig := zap.NewProductionEncoderConfig()

	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.Lock(logFile),
		zapcore.InfoLevel,
	)

	newCore := zapcore.NewTee(existingCore, fileCore)
	logger = zap.New(newCore)

	return &FileLogWatcher{
		logger:   logger,
		path:     path,
		currFile: logFile,
		executor: e,
	}
}

func (f *FileLogWatcher) GetLogger() *zap.Logger {
	return f.logger
}

func (f *FileLogWatcher) GetLogPath() string {
	return f.path
}

func (f *FileLogWatcher) FileChangeCapture() error {
	f.Close()

	logFile, err := os.OpenFile(f.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		f.logger.Error("failed to open log file", zap.Error(err))
		return nil
	}
	f.currFile = logFile
	existingCore := f.logger.Core()

	encoderConfig := zap.NewProductionEncoderConfig()

	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.Lock(logFile),
		zapcore.InfoLevel,
	)

	newCore := zapcore.NewTee(existingCore, fileCore)
	f.logger = zap.New(newCore)

	return nil
}

func (f *FileLogWatcher) Close() {
	if err := f.currFile.Sync(); err != nil {
		f.logger.Error("failed to sync log file", zap.Error(err))
		return
	}
	if err := f.currFile.Close(); err != nil {
		f.logger.Error("failed to close log file", zap.Error(err))
		return
	}
}
