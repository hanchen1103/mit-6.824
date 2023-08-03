package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	Debug(dWarn, "s%d accept last heart beat time out, start election, current term:%+v", rf.me, rf.currentTerm)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.role = Candidate
	rf.resetTimerTicker()
	rf.mu.Unlock()

	for p := range rf.peers {
		if p == rf.me {
			continue
		}

		go func(p int) {
			rf.mu.Lock()
			if rf.role != Candidate {
				rf.mu.Unlock()
				return
			}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logEntries) - 1,
			}
			if len(rf.logEntries) > 0 {
				args.LastLogTerm = rf.logEntries[args.LastLogIndex].Term
			}
			rf.mu.Unlock()
			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(p, args, reply)
			//Debug(dVote, "s%d send request vote to s%d suc:%v", rf.me, p, ok)

			rf.mu.Lock()
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			if ok && reply.VoteGranted && rf.role == Candidate {
				rf.voteCount += 1
				if rf.voteCount > len(rf.peers)/2 {
					Debug(dLeader, "s%d become leader,current term:%+v", rf.me, rf.currentTerm)
					rf.role = Leader
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.logEntries)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					go rf.sendHeartBeats()
					return
				} else {
					rf.mu.Unlock()
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.becomeFollower()
					rf.votedFor = -1
					rf.resetTimerTicker()
				}
				rf.mu.Unlock()
				return
			}
		}(p)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	lastLogIdx := len(rf.logEntries) - 1
	var lastLogTerm int
	if lastLogIdx >= 0 {
		lastLogTerm = rf.logEntries[lastLogIdx].Term
	} else {
		lastLogTerm = -1
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.becomeFollower()
	}

	if !(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx)) {
		Debug(dInfo, "candidate s:%+v last log term:%+v, last log index:%+v, current s:%+v, last log term:%+v, last log index:%+v", args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.me, lastLogTerm, lastLogIdx)
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.becomeFollower()
		rf.resetTimerTicker()
	} else {
		reply.VoteGranted = false
	}
	return
}
