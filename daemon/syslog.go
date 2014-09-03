package main

import (
	"errors"
	"log/syslog"

	log "code.google.com/p/log4go"
)

// This log writer sends output to a socket
type SysLogWriter chan *log.LogRecord

// This is the SocketLogWriter's output method
func (w SysLogWriter) LogWrite(rec *log.LogRecord) {
	w <- rec
}

func (w SysLogWriter) Close() {
	close(w)
}

func GetWriter(writer *syslog.Writer, level string) func(string) error {
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

func Log(writer *syslog.Writer, level string, message string) {
	m := GetWriter(writer, level)
	m(message)
}

func connectSyslogDaemon() (writer *syslog.Writer, err error) {
	logTypes := []string{"unixgram", "unix"}
	logPaths := []string{"/dev/log", "/var/run/syslog"}
	var raddr string
	for _, network := range logTypes {
		for _, path := range logPaths {
			raddr = path
			writer, err = syslog.Dial(network, raddr, syslog.LOG_SYSLOG, "influxdb")
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


func NewSysLogWriter(facility int) (w SysLogWriter, err error) {
	writer, err := connectSyslogDaemon()
	if err != nil {
		return
	}
	w = SysLogWriter(make(chan *log.LogRecord, log.LogBufferLength))
	go func() {
		defer func() {
			if w != nil {
				w.Close()
			}
		}()
		for rec := range w {
			m := log.FormatLogRecord("(%S) %M", rec)
			Log(writer, rec.Level.String(), m)
		}
	}()
	return
}
