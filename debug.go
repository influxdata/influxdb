package raft

import (
	"log"
	"os"
)

//------------------------------------------------------------------------------
//
// Variables
//
//------------------------------------------------------------------------------

const (
	Debug = 1
	Trace = 2
)

var LogLevel int = 0
var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "", log.Lmicroseconds)
}

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Warnings
//--------------------------------------

// Prints to the standard logger. Arguments are handled in the manner of
// fmt.Print.
func warn(v ...interface{}) {
	logger.Print(v...)
}

// Prints to the standard logger. Arguments are handled in the manner of
// fmt.Printf.
func warnf(format string, v ...interface{}) {
	logger.Printf(format, v...)
}

// Prints to the standard logger. Arguments are handled in the manner of
// fmt.Println.
func warnln(v ...interface{}) {
	logger.Println(v...)
}

//--------------------------------------
// Basic debugging
//--------------------------------------

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Print.
func debug(v ...interface{}) {
	if LogLevel >= Debug {
		logger.Print(v...)
	}
}

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Printf.
func debugf(format string, v ...interface{}) {
	if LogLevel >= Debug {
		logger.Printf(format, v...)
	}
}

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Println.
func debugln(v ...interface{}) {
	if LogLevel >= Debug {
		logger.Println(v...)
	}
}

//--------------------------------------
// Trace-level debugging
//--------------------------------------

// Prints to the standard logger if trace debugging is enabled. Arguments
// are handled in the manner of fmt.Print.
func trace(v ...interface{}) {
	if LogLevel >= Trace {
		logger.Print(v...)
	}
}

// Prints to the standard logger if trace debugging is enabled. Arguments
// are handled in the manner of fmt.Printf.
func tracef(format string, v ...interface{}) {
	if LogLevel >= Trace {
		logger.Printf(format, v...)
	}
}

// Prints to the standard logger if trace debugging is enabled. Arguments
// are handled in the manner of debugln.
func traceln(v ...interface{}) {
	if LogLevel >= Trace {
		logger.Println(v...)
	}
}
