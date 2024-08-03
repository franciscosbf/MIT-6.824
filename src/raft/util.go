package raft

import (
	"log"
	"os"
)

var debug bool

func init() {
	_, debug = os.LookupEnv("DEBUG")
}

func DPrintf(format string, a ...interface{}) {
	if !debug {
		return
	}

	log.Printf(format, a...)
}
