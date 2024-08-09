package raft

import "sync/atomic"

type RequestVoteArgs struct {
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	upToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) 

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.resetElectionTimer()
	}
	
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, voteCount *int32) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !reply.VoteGranted {
		if reply.Term > rf.currentTerm {
			rf.updateTerm(reply.Term)
		}
		return
	}

	if atomic.AddInt32(voteCount, 1) > int32(len(rf.peers) / 2) && rf.state == CandidateState && rf.currentTerm == args.Term {
		rf.state = LeaderState
		for peer := range rf.peers {
			rf.nextIndex[peer] = rf.getLastLogIndex() + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		}
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			rf.broadcastHeartbeat(peer)
		}
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm += 1
	rf.state = CandidateState
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	lastLog := rf.logs[len(rf.logs) - 1]
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateID: rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm: lastLog.Term,
	}
	voteCount := int32(1)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendRequestVote(peer, &args, &voteCount)
	}
}
