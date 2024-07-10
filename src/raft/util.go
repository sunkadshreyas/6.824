package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func customMinFunc(a int, b int ) int {
	if a < b {
		return a
	}
	return b
}
