// +build !linux !profile

package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	log "code.google.com/p/log4go"
)

func startProfiler(stoppable Stoppable) error {
	go waitForSignals(stoppable)
	return nil
}

func waitForSignals(stoppable Stoppable) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	for {
		sig := <-ch
		log.Info("Received signal: %s", sig.String())
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			stoppable.Stop()
			time.Sleep(time.Second)
			os.Exit(0)
		}
	}
}
