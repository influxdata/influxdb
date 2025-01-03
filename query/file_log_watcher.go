package query

import (
	"os"
	"sync"

	l "github.com/influxdata/influxdb/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type FileLogWatcher struct {
	path            string
	currFile        *os.File
	logger          *zap.Logger
	formatterConfig string
	executor        *Executor
	mu              sync.Mutex
}

func NewFileLogWatcher(e *Executor, path string, logger *zap.Logger, format string) *FileLogWatcher {
	logFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		logger.Error("failed to open log file", zap.Error(err))
		return nil
	}

	existingCore := logger.Core()
	encoder, err := l.NewEncoder(format)
	if err != nil {
		logger.Error("failed to create log encoder", zap.Error(err), zap.String("format", format), zap.String("path", path))
		return nil
	}

	fileCore := zapcore.NewCore(
		encoder,
		zapcore.Lock(logFile),
		zapcore.InfoLevel,
	)

	newCore := zapcore.NewTee(existingCore, fileCore)
	logger = zap.New(newCore)

	return &FileLogWatcher{
		logger:          logger,
		path:            path,
		currFile:        logFile,
		executor:        e,
		formatterConfig: format,
		mu:              sync.Mutex{},
	}
}

func (f *FileLogWatcher) GetLogger() *zap.Logger {
	return f.logger
}

func (f *FileLogWatcher) GetLogPath() string {
	return f.path
}

func (f *FileLogWatcher) FileChangeCapture() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Close()

	logFile, err := os.OpenFile(f.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		f.logger.Error("failed to open log file", zap.Error(err), zap.String("path", f.path))
		return nil
	}
	f.currFile = logFile
	existingCore := f.logger.Core()
	encoder, err := l.NewEncoder(f.formatterConfig)
	if err != nil {
		return err
	}

	fileCore := zapcore.NewCore(
		encoder,
		zapcore.Lock(logFile),
		zapcore.InfoLevel,
	)

	newCore := zapcore.NewTee(existingCore, fileCore)
	f.logger = zap.New(newCore)

	return nil
}

func (f *FileLogWatcher) Close() {
	if err := f.currFile.Sync(); err != nil {
		f.logger.Error("failed to sync log file", zap.Error(err), zap.String("path", f.path))
		return
	}
	if err := f.currFile.Close(); err != nil {
		f.logger.Error("failed to close log file", zap.Error(err), zap.String("path", f.path))
		return
	}
}
