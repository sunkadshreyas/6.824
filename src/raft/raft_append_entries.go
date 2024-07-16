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
	NextIndexToTry int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// logs would have changed
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndexToTry = rf.getLastLogIndex() + 1
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
		reply.NextIndexToTry = rf.getLastLogIndex() + 1
		return
	}

	sIndex := rf.logs[0].Index

	if args.PrevLogIndex >= sIndex && args.PrevLogTerm != rf.logs[args.PrevLogIndex - sIndex].Term {
		// if log entry at prevIndex does not match prevLogTerm, then return false
		// before returning indicate the last position where the logs matched
		term := rf.logs[args.PrevLogIndex - sIndex].Term
		for i := args.PrevLogIndex - 1; i >= sIndex; i-- {
			if rf.logs[i - sIndex].Term != term {
				reply.NextIndexToTry = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= sIndex - 1 {
		rf.logs = rf.logs[ :args.PrevLogIndex - sIndex + 1]
		rf.logs = append(rf.logs, args.Entries...)

		reply.Success = true
		reply.NextIndexToTry = args.PrevLogIndex + len(args.Entries)

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
		// New Leader has already been elected, revert back to Follower state
		rf.currentTerm = reply.Term
		rf.state = FollowerState
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.Success {
		if(len(args.Entries) > 0) {
			// nextIndex from where logs have to be appended
			rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
			// lastIndex until where the logs are matching
			rf.matchIndex[server] = args.Entries[len(args.Entries) - 1].Index
		}
	} else {
		// retry to append logs from NextIndexToTry, because there were conflicting logs
		rf.nextIndex[server] = customMinFunc(reply.NextIndexToTry, rf.getLastLogIndex())
	}

	sIndex := rf.logs[0].Index
	for i := rf.getLastLogIndex(); i > rf.commitIndex && rf.logs[i - sIndex].Term == rf.currentTerm; i-- {
		
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

	sIndex := rf.logs[0].Index
	for peer := range rf.peers {
		
		if peer != rf.me && rf.state == LeaderState {
			
			args := &AppendEntriesArg {
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				LeaderCommit: rf.commitIndex,
			}

			if args.PrevLogIndex >= sIndex {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex - sIndex].Term
			}

			if rf.nextIndex[peer] <= rf.getLastLogIndex() {
				args.Entries = rf.logs[rf.nextIndex[peer] - sIndex:]
			}
		
			go rf.sendAppendEntries(peer, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	sIndex := rf.logs[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.Command = rf.logs[i - sIndex].Command
		msg.CommandIndex = i
		msg.CommandValid = true
		rf.chanApply <- msg
	}
	rf.lastApplied = rf.commitIndex
}
