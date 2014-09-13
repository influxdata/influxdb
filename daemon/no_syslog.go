// +build windows nacl plan9

package main

import (
	"errors"

	log "code.google.com/p/log4go"
)

type sysLogWriter chan *log.LogRecord

func (w sysLogWriter) LogWrite(rec *log.LogRecord) {
	w <- rec
}

func (w sysLogWriter) Close() {
	close(w)
}

func GetSysLogFacility(name string) (int, bool) {
	return 0, false
}

func NewSysLogWriter(priority int) (w sysLogWriter, err error) {
	return nil, errors.New("syslog isn't implemented on windows yet.")
}
