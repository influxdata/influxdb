package raft

import (
	"log"
)

//------------------------------------------------------------------------------
//
// Variables
//
//------------------------------------------------------------------------------

// A flag stating if debug statements should be evaluated.
var Debug bool = false


//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Print.
func debug(v ...interface{}) {
	if Debug {
		log.Print(v...)
	}
}

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of fmt.Printf.
func debugf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format, v...)
	}
}

// Prints to the standard logger if debug mode is enabled. Arguments
// are handled in the manner of debugln.
func debugln(v ...interface{}) {
	if Debug {
		log.Println(v...)
	}
}

