package raft

func (rf *Raft) sendHeartBeats() {
	rf.leaderCommitIndex()
	rf.resetLeaderTicker()

	argsMap := rf.buildMsgArgsMap()
	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		if r, _, _ := rf.getMetaInfo(); r != Leader {
			return
		}
		go func(p int) {
			args := argsMap[p]
			switch args.MsgType {
			case HeartBeatCheck:
				rf.sendHeartBeatsMsg(p, args)
			case LogEntriesUpdate:
				rf.appendEntries(p, args, false)
			}
		}(p)
	}
}

func (rf *Raft) buildMsgArgsMap() map[int]*AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resMap := make(map[int]*AppendEntriesArgs)

	for pid := range rf.peers {
		if pid == rf.me {
			continue
		}
		var msgType = HeartBeatCheck
		entries := rf.logEntries[rf.nextIndex[pid]:]
		if len(entries) > 0 {
			msgType = LogEntriesUpdate
		}
		prevTerm, prevLogIndex := -1, rf.nextIndex[pid]-1
		if prevLogIndex >= 0 && prevLogIndex < len(rf.logEntries) {
			prevTerm = rf.logEntries[prevLogIndex].Term
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevTerm,
			MsgType:      msgType,
			Leader:       rf.me,
		}
		if msgType == LogEntriesUpdate {
			args.Entries = append([]*logEntry(nil), entries...)
		}
		resMap[pid] = args
	}
	return resMap
}

func (rf *Raft) sendHeartBeatsMsg(pid int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(pid, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.becomeFollower()
		rf.votedFor = -1
		Debug(dInfo, "leader:%+v become follower, current term:%+v", rf.me, rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleHeartbeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.Success = true
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.becomeFollower()
		rf.votedFor = -1
	}

	commitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		logEntriesLen := len(rf.logEntries)
		isCommitIdxNotMatch := args.LeaderCommit < logEntriesLen && rf.logEntries[args.LeaderCommit].Term != args.Term
		isLogNotMatch := args.PrevLogIndex >= logEntriesLen || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm
		if isCommitIdxNotMatch || isLogNotMatch {
			reply.Success = false
			return
		}
		rf.commitIndex = min(args.LeaderCommit, logEntriesLen-1)
		if commitIndex != rf.commitIndex {
			rf.applyCond.Broadcast()
		}
	}

	rf.resetTimerTicker()
	reply.Success = true
}
