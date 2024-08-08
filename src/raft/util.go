package raft

import (
	"log"
	"math/rand"
	"time"
)

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

func (rf *Raft) updateTerm(newTerm int) {
	if rf.currentTerm < newTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = newTerm
	rf.state = FollowerState
}

func (rf *Raft) resetElectionTimer() {
	ms := 150 + (rand.Int63() % 150)
	rf.electionTimestamp = time.Now()
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}
