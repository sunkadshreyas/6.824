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

func customMaxFunc(a int, b int ) int {
	if a > b {
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

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applierCond.Wait()
		}
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		logEntries := make([]LogEntry, commitIndex - lastApplied)
		copy(logEntries, rf.logs[lastApplied + 1 : commitIndex + 1])
		rf.mu.Unlock()

		for _, entry := range logEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command: entry.Command,
				CommandIndex: entry.Index,
				CommandTerm: entry.Term,
			}
		}

		rf.mu.Lock()
		if rf.lastApplied < commitIndex {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logs[len(rf.logs) - 1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}
