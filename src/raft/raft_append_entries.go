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
	// To decide from which point onwards to send the logs
	ConflictIndex int 
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.ConflictIndex = -1
	reply.Term = rf.currentTerm

	if !rf.IsNewTermValid(args.Term) {
		return
	}

	if rf.state == CandidateState {
		rf.state = FollowerState
	}

	rf.resetElectionTimer()

	prevLogIndex := args.PrevLogIndex - rf.logs[0].Index
	if prevLogIndex < 0 {
		reply.ConflictIndex = 0
		return
	}

	if prevLogIndex >= len(rf.logs) {
		return
	}

	if rf.logs[prevLogIndex].Term != args.PrevLogTerm {
		currTerm := rf.logs[prevLogIndex].Term
		var conflictIndex int
		for i := prevLogIndex; i > 0; i-- {
			if rf.logs[i - 1].Term != currTerm {
				conflictIndex = i
				break
			}
		}
		reply.ConflictIndex = conflictIndex + rf.logs[0].Index
		return
	}

	for _, entry := range args.Entries {
		logIndex := entry.Index - rf.logs[0].Index
		if logIndex >= len(rf.logs) || rf.logs[logIndex].Term != entry.Term {
			rf.logs = append([]LogEntry{}, append(rf.logs[:prevLogIndex + 1], args.Entries...)...)
			break
		}
	}

	reply.Success = true

	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = args.CommitIndex
		if args.CommitIndex - rf.logs[0].Index >= len(rf.logs) {
			rf.commitIndex = rf.getLastLogIndex()
		}
	}

	rf.applierCond.Signal()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if rf.IsReplyTermGreater(reply.Term) {
		return
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1

		for _, log := range rf.logs {
			index := log.Index
			count := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= index {
					count += 1
				}
			}
			if count > len(rf.peers) / 2 && index > rf.commitIndex && log.Term == rf.currentTerm {
				rf.commitIndex = index
			}
		}
	} else {
		rf.nextIndex[server] = customMaxFunc(1, reply.ConflictIndex - 1)
	}

	rf.applierCond.Signal()
}

func (rf *Raft) broadcastAppendEntries(fromHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if fromHeartbeat {
			rf.prepareAppendEntries(peer, true)
		} else {
			rf.broadcasterCond[peer].Signal()
		}
	}
}

func (rf *Raft) prepareAppendEntries(peer int, fromHeartbeat bool) {
	firstLog := rf.logs[0]
	nextIndexForPeer := rf.nextIndex[peer]
	if nextIndexForPeer > firstLog.Index {
		nextIndexForPeer = nextIndexForPeer - firstLog.Index
		prevLog := rf.logs[nextIndexForPeer - 1]
		args := AppendEntriesArgs{
			LeaderID: rf.me,
			Term: rf.currentTerm,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm: prevLog.Term,
			// Send the logs from the index until which logs have been committed
			Entries: rf.logs[nextIndexForPeer:],
			CommitIndex: rf.commitIndex,
		}
		if fromHeartbeat {
			go rf.sendAppendEntries(peer, &args)
		} else {
			rf.sendAppendEntries(peer, &args)
		}
	} else {
		args := InstallSnapshotArgs{
			Term : rf.currentTerm,
			LeaderID: rf.me,
			LastIncludedIndex: rf.logs[0].Index,
			LastIncludedTerm: rf.logs[0].Term,
			Offset: 0,
			Data: rf.persister.ReadSnapshot(),
			Done: true,
		}
		if fromHeartbeat {
			go rf.sendInstallSnapshot(peer, &args)
		} else {
			go rf.sendInstallSnapshot(peer, &args)
		}
	}
}

func (rf *Raft) broadcaster(peer int) {
	rf.broadcasterCond[peer].L.Lock()
	defer rf.broadcasterCond[peer].L.Unlock()

	for !rf.killed() {
		
		for !rf.IsReplicationNedded(peer) {
			rf.broadcasterCond[peer].Wait()
		}

		rf.prepareAppendEntries(peer, false)
	}
}

func (rf *Raft) IsReplicationNedded(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == LeaderState && rf.matchIndex[peer] < rf.getLastLogIndex()
}
