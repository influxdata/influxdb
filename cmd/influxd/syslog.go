// +build !windows,!nacl,!plan9

package main

import (
	"errors"
	"log/syslog"
	"strings"

	log "code.google.com/p/log4go"
)

type sysLogWriter chan *log.LogRecord

func (w sysLogWriter) LogWrite(rec *log.LogRecord) {
	w <- rec
}

func (w sysLogWriter) Close() {
	close(w)
}

func GetSysLogFacility(name string) (syslog.Priority, bool) {
	switch strings.ToLower(name) {
	case "syslog":
		return syslog.LOG_SYSLOG, true
	case "local0":
		return syslog.LOG_LOCAL0, true
	case "local1":
		return syslog.LOG_LOCAL1, true
	case "local2":
		return syslog.LOG_LOCAL2, true
	case "local3":
		return syslog.LOG_LOCAL3, true
	case "local4":
		return syslog.LOG_LOCAL4, true
	case "local5":
		return syslog.LOG_LOCAL5, true
	case "local6":
		return syslog.LOG_LOCAL6, true
	case "local7":
		return syslog.LOG_LOCAL7, true
	default:
		return syslog.LOG_SYSLOG, false
	}
}

func getWriter(writer *syslog.Writer, level string) func(string) error {
	switch level {
	case "DEBG", "TRAC", "FINE", "FNST":
		return writer.Debug
	case "INFO":
		return writer.Info
	case "WARN":
		return writer.Warning
	case "EROR":
		return writer.Err
	default:
		return writer.Crit
	}
}

func connectSyslogDaemon(priority syslog.Priority) (writer *syslog.Writer, err error) {
	logTypes := []string{"unixgram", "unix"}
	logPaths := []string{
		"/dev/log",        // unix socket for syslog on linux
		"/var/run/syslog", // unix socket for syslog on osx
	}
	var raddr string
	for _, network := range logTypes {
		for _, path := range logPaths {
			raddr = path
			writer, err = syslog.Dial(network, raddr, priority, "influxdb")
			if err != nil {
				continue
			} else {
				return
			}
		}
	}
	if err != nil {
		err = errors.New("cannot connect to Syslog Daemon")
	}
	return
}

func NewSysLogWriter(priority syslog.Priority) (w sysLogWriter, err error) {
	writer, err := connectSyslogDaemon(priority)
	if err != nil {
		return
	}
	w = sysLogWriter(make(chan *log.LogRecord, log.LogBufferLength))
	go func() {
		defer func() {
			if w != nil {
				w.Close()
			}
		}()
		for rec := range w {
			m := log.FormatLogRecord("(%S) %M", rec)
			getWriter(writer, rec.Level.String())(m)
		}
	}()
	return
}
