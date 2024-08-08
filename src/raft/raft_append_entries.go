package raft

type AppendEntriesArgs struct {
	Term int
	Entries []LogEntry
	PrevLogIndex int
	PrevLogTerm int
	LeaderID int
	CommitIndex int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// I am in greater term, so respond back with false message
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term >= rf.currentTerm {
		// incoming term is greater, update my term to the latest value
		rf.updateTerm(args.Term)
	}

	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		currLastLogIndex := rf.logs[len(rf.logs) - 1].Index
		prevLogIndex := customMinFunc(rf.nextIndex[peer] - 1, currLastLogIndex)
		prevLogTerm := rf.logs[prevLogIndex].Term

		args := &AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.Entries = make([]LogEntry, 0)
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = prevLogTerm
		args.CommitIndex = rf.commitIndex

		go rf.sendAppendEntries(peer, args)
	}
}
