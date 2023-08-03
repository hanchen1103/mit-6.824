package raft

type logEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Leader       int
	Entries      []*logEntry
	LeaderCommit int
	MsgType      MsgType
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.becomeFollower()
	rf.mu.Unlock()

	switch args.MsgType {
	case HeartBeatCheck:
		rf.handleHeartbeat(args, reply)
	case LogEntriesUpdate:
		rf.handleLogEntriesUpdate(args, reply)
	}
}

func (rf *Raft) sendUpdateLogMsg() {
	rf.resetLeaderTicker()
	successCh := make(chan bool, len(rf.peers)-1)
	argsMap := rf.buildMsgArgsMap()

	for p := range rf.peers {
		if p == rf.me {
			continue
		}

		go func(p int) {
			if role, _, _ := rf.getMetaInfo(); role != Leader {
				return
			}
			args := argsMap[p]
			successCh <- rf.appendEntries(p, args, true)
		}(p)
	}

	if role, _, _ := rf.getMetaInfo(); role != Leader {
		return
	}

	numSuccess := 1
	numFailure := 0
	for numSuccess <= (len(rf.peers)-1)/2 && numFailure < (len(rf.peers)-1)/2 {
		success := <-successCh
		if success {
			numSuccess++
		} else {
			numFailure++
		}
	}

	if numSuccess <= (len(rf.peers)-1)/2 {
		return
	}

	// Update commitIndex for all servers
	rf.leaderCommitIndex()
}

func (rf *Raft) leaderCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for N := rf.commitIndex + 1; N <= len(rf.logEntries); N++ {
		num := 1
		for i, matchIdx := range rf.matchIndex {
			if i != rf.me && matchIdx >= N && rf.logEntries[N].Term == rf.currentTerm {
				num += 1
			}
		}
		if num > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Broadcast()
			Debug(dCommit, "s%d leader commit index:%v", rf.me, rf.commitIndex)
		}
	}
}

func (rf *Raft) appendEntries(pid int, args *AppendEntriesArgs, retry bool) bool {
	for {
		if r, _, _ := rf.getMetaInfo(); r != Leader {
			return false
		}
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(pid, args, reply)

		rf.mu.Lock()
		Debug(dLeader, "s%d send append entries to s%d, rpc suc:%v, args log_len:%+v, leader_term:%+v, reply:%v", rf.me, pid, ok, len(args.Entries), rf.currentTerm, reply)
		if rf.role != Leader {
			rf.mu.Unlock()
			return false
		}

		if !ok {
			rf.mu.Unlock()
			return false
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.becomeFollower()
			rf.votedFor = -1
			rf.mu.Unlock()
			return false
		}

		if reply.Success {
			rf.nextIndex[pid] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[pid] = args.PrevLogIndex + len(args.Entries)
			rf.mu.Unlock()
			return true
		}

		if rf.nextIndex[pid] <= 0 {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndex[pid] = reply.ConflictIndex + 1
		if !retry {
			rf.mu.Unlock()
			return false
		}
		args.PrevLogIndex = rf.nextIndex[pid] - 1
		args.PrevLogTerm = rf.logEntries[args.PrevLogIndex].Term
		args.Entries = append([]*logEntry(nil), rf.logEntries[rf.nextIndex[pid]:]...)
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleLogEntriesUpdate(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.resetTimerTicker()
		rf.mu.Unlock()
	}()

	prevLogIndex, prevLogTerm := args.PrevLogIndex, args.PrevLogTerm

	if args.Term < rf.currentTerm || args.PrevLogIndex < 0 {
		reply.Success = false
		return
	}

	if rf.role == Leader && args.Term > rf.currentTerm {
		rf.becomeFollower()
		rf.currentTerm = args.Term
	}

	if cid, isFind := rf.findConflictIndex(prevLogIndex, prevLogTerm); isFind {
		reply.ConflictIndex = cid
		reply.Success = false
		return
	}

	index := prevLogIndex
	var i int
	for i = 0; i < len(args.Entries); i++ {
		index += 1
		if index >= len(rf.logEntries) || rf.logEntries[index].Term != args.Entries[i].Term {
			break
		}
	}

	if i < len(args.Entries) && index <= len(rf.logEntries) {
		// if there's a conflict, truncate the log and append the remaining entries
		rf.logEntries = append(rf.logEntries[:index], args.Entries[i:]...)
	}

	commitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.logEntries)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logEntries) - 1
		}
		if commitIndex != rf.commitIndex {
			rf.applyCond.Broadcast()
		}
		Debug(dCommit, "s%d follower commit index:%v, leader:%+v,leader_commit:%+v, leader_term:%+v", rf.me, rf.commitIndex, args.Leader, args.LeaderCommit, args.Term)
	}
	reply.Success = true
}

func (rf *Raft) applyLogEntries() {
	for {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.applyCond.Wait()
		}

		start := rf.lastApplied + 1
		if start > rf.commitIndex+1 {
			rf.mu.Unlock()
			continue
		}
		entries := make([]*logEntry, len(rf.logEntries[start:rf.commitIndex+1]))
		copy(entries, rf.logEntries[start:rf.commitIndex+1])
		rf.mu.Unlock()

		for i, entry := range entries {
			if entry.Command == nil {
				continue
			}
			appMsg := ApplyMsg{
				Command:      entry.Command,
				CommandValid: true,
				CommandIndex: start + i,
			}
			rf.mu.Lock()
			rf.applyCh <- appMsg
			rf.lastApplied = start + i
			Debug(dApply, "s%d apply msg:%+v, commit_index:%+v, last_applied:%v", rf.me, appMsg, rf.commitIndex, rf.lastApplied)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) findConflictIndex(prevLogIndex, prevLogTerm int) (int, bool) {
	if prevLogIndex >= len(rf.logEntries) {
		return len(rf.logEntries) - 1, true
	}
	if rf.logEntries[prevLogIndex].Term != prevLogTerm {
		var conflictIndex = 0
		for i := prevLogIndex; i >= 0; i-- {
			if rf.logEntries[i].Term == prevLogTerm {
				conflictIndex = i
				break
			}
		}
		return conflictIndex, true
	}
	return 0, false
}
