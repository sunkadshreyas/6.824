package raft

type InstallSnapshotArgs struct {
	Term int
	LeaderID int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if !rf.IsNewTermValid(args.Term) {
		rf.mu.Unlock()
		return
	}

	if rf.state == CandidateState {
		rf.state = FollowerState
	}

	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	if !args.Done {
		rf.mu.Unlock()
		return
	}

	firstLogIndex := rf.logs[0].Index
	if firstLogIndex <= args.LastIncludedIndex {
		rf.logs = append([]LogEntry{}, LogEntry{
			Index: args.LastIncludedIndex,
			Term: args.LastIncludedTerm,
			Command: nil,
		})
	} else if firstLogIndex < args.LastIncludedIndex {
		trimLen := args.LastIncludedIndex - firstLogIndex
		rf.logs = append([]LogEntry{}, rf.logs[trimLen:]...)
		rf.logs[0].Command = nil
	}
	rf.persister.Save(rf.encodeState(), args.Data)
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotTerm: args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		return
	}

	if rf.currentTerm != args.Term || rf.state != LeaderState || args.LastIncludedIndex != rf.logs[0].Index {
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.persister.Save(rf.encodeState(), args.Data)
}
