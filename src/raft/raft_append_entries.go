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

	reply.Success = false
	
	if args.Term < rf.currentTerm {
		// I am in greater term, so respond back with false message
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// incoming term is greater, update my term to the latest value
		rf.updateTerm(args.Term)
	}

	if rf.state == CandidateState {
		rf.state = FollowerState
	}

	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex >= len(rf.logs) {
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logs = rf.logs[:args.PrevLogIndex]
		return
	}

	for _, entry := range args.Entries {
		if entry.Index >= len(rf.logs) || rf.logs[entry.Index].Term != entry.Term {
			rf.logs = append([]LogEntry{}, append(rf.logs[:args.PrevLogIndex + 1], args.Entries...)...)
			break
		}
	}

	reply.Success = true

	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = customMinFunc(len(rf.logs) - 1, args.CommitIndex)
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
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		return
	}

	if reply.Success && len(args.Entries) > 0 {
		rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1

		for index := range rf.logs {
			count := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= index {
					count += 1
				}
			}
			if count > len(rf.peers) / 2 && index > rf.commitIndex && rf.logs[index].Term == rf.currentTerm {
				rf.commitIndex = index
			}
		}
	} else if reply.Success == false {
		rf.nextIndex[server] = customMaxFunc(0, rf.nextIndex[server] - 1)
	}

	rf.applierCond.Signal()
}

func (rf *Raft) broadcastHeartbeat(peer int) {
	prevLogIndex := customMinFunc(rf.nextIndex[peer] - 1, rf.getLastLogIndex())
	prevLogTerm := rf.logs[prevLogIndex].Term

	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.Entries = rf.logs[rf.nextIndex[peer]:]
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.CommitIndex = rf.commitIndex

	go rf.sendAppendEntries(peer, args)
}

func (rf *Raft) broadcaster(peer int) {
	rf.broadcasterCond[peer].L.Lock()
	defer rf.broadcasterCond[peer].L.Unlock()

	for rf.killed() == false {
		
		for rf.noNeedReplicating(peer) {
			rf.broadcasterCond[peer].Wait()
		}

		prevLogIndex := customMinFunc(rf.nextIndex[peer] - 1, rf.getLastLogIndex())
		prevLogTerm := rf.logs[prevLogIndex].Term

		args := &AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.Entries = rf.logs[rf.nextIndex[peer]:]
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = prevLogTerm
		args.CommitIndex = rf.commitIndex

		rf.sendAppendEntries(peer, args) 
	}
}

func (rf *Raft) noNeedReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state != LeaderState || rf.matchIndex[peer] >= rf.getLastLogIndex()
}
