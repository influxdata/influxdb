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

// A flag stating if debug statements should be evaluated.
var Debug bool = false
var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "", log.Lmicroseconds)
}

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Print.
func debug(v ...interface{}) {
	if Debug {
		logger.Print(v...)
	}
}

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Printf.
func debugf(format string, v ...interface{}) {
	if Debug {

		logger.Printf(format, v...)
	}
}

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of debugln.
func debugln(v ...interface{}) {
	if Debug {
		logger.Println(v...)
	}
}
