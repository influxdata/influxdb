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

var Debug bool = false
var Trace bool = false
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
// Basic debugging
//--------------------------------------

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Print.
func debug(v ...interface{}) {
	if Debug || Trace {
		logger.Print(v...)
	}
}

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Printf.
func debugf(format string, v ...interface{}) {
	if Debug || Trace {
		logger.Printf(format, v...)
	}
}

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of debugln.
func debugln(v ...interface{}) {
	if Debug || Trace {
		logger.Println(v...)
	}
}

//--------------------------------------
// Trace-level debugging
//--------------------------------------

// Prints to the standard logger if trace debugging is enabled. Arguments
// are handled in the manner of fmt.Print.
func trace(v ...interface{}) {
	if Trace {
		logger.Print(v...)
	}
}

// Prints to the standard logger if trace debugging is enabled. Arguments
// are handled in the manner of fmt.Printf.
func tracef(format string, v ...interface{}) {
	if Trace {
		logger.Printf(format, v...)
	}
}

// Prints to the standard logger if trace debugging is enabled. Arguments
// are handled in the manner of debugln.
func traceln(v ...interface{}) {
	if Trace {
		logger.Println(v...)
	}
}
