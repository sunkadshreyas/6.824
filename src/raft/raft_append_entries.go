package raft

type AppendEntriesArg struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	// next index in log from which logs have to be appended
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.Term > rf.currentTerm {
		// Candidate with higher term is contesting election, so you step down
		rf.state = FollowerState
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.chanHeartBeat <- true

	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		// there is no log at PrevLogIndex, retry at LastLogIndex
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		// if log entry at prevIndex does not match prevLogTerm, then return false
		term := rf.logs[args.PrevLogIndex].Term
		for i := args.PrevLogIndex - 1; i >= 0 && rf.logs[i].Term == term; i-- {
			// keep iterating until the condition is met
			reply.NextIndex = i + 1
		}
	} else {
		remainLogs := rf.logs[args.PrevLogIndex + 1 : ]
		rf.logs = rf.logs[ : args.PrevLogIndex + 1]

		if rf.hasConflictLog(args.Entries, remainLogs) || len(remainLogs) < len(args.Entries) {
			rf.logs = append(rf.logs, args.Entries...)
		} else {
			rf.logs = append(rf.logs, remainLogs...)
		}

		reply.Success = true
		reply.NextIndex = args.PrevLogIndex

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = customMinFunc(args.LeaderCommit, rf.getLastLogIndex())
			go rf.applyLog()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != LeaderState || args.Term != rf.currentTerm {
		return ok
	}

	if rf.currentTerm < reply.Term {
		// New Leader has already been elected
		rf.currentTerm = reply.Term
		rf.state = FollowerState
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = reply.NextIndex
	}

	for i := rf.getLastLogIndex(); i > rf.commitIndex && rf.logs[i].Term == rf.currentTerm; i-- {
		count := 1

		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= i {
				count += 1
			}
		}

		if count > len(rf.peers) / 2 {
			rf.commitIndex = i
			go rf.applyLog()
			break
		}
	}

	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &AppendEntriesReply{}

	for peer := range rf.peers {
		
		if peer != rf.me && rf.state == LeaderState {
			
			args := &AppendEntriesArg {
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				LeaderCommit: rf.commitIndex,
			}

			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}

			if rf.nextIndex[peer] <= rf.getLastLogIndex() {
				args.Entries = rf.logs[rf.nextIndex[peer]:]
			}
		
			go rf.sendAppendEntries(peer, args, reply)
		}
	}
}

func (rf *Raft) hasConflictLog(logsFromLeader []LogEntry, logsFromLocal []LogEntry) bool {
	for i := 0; i < customMinFunc(len(logsFromLeader), len(logsFromLocal)); i++ {
		if logsFromLeader[i].Term != logsFromLocal[i].Term {
			return true
		}
	}
	return false
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.Command = rf.logs[i].Command
		msg.CommandIndex = i
		msg.CommandValid = true
		rf.chanApply <- msg
	}
	rf.lastApplied = rf.commitIndex
}
