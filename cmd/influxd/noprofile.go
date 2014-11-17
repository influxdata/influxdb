// +build !linux !profile

package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	log "code.google.com/p/log4go"
)

func startProfiler(stopper Stopper) error {
	go waitForSignals(stopper)
	return nil
}

func waitForSignals(stopper Stopper) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	for {
		sig := <-ch
		log.Info("Received signal: %s", sig.String())
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			stopper.Stop()
			time.Sleep(time.Second)
			os.Exit(0)
		}
	}
}
