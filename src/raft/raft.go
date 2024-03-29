package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int
	votedFor    int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
	voteCount  int

	electionTicker *time.Ticker
	leaderTicker   *time.Ticker

	logEntries []*logEntry

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	batchBuffer int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.role == Leader

	if rf.role != Leader {
		rf.mu.Unlock()
		return index, term, false
	}

	entry := &logEntry{
		Command: command,
		Term:    term,
	}
	rf.logEntries = append(rf.logEntries, entry)

	index = len(rf.logEntries) - 1
	Debug(dClient, "s%d accept command:%v", rf.me, command)

	rf.mu.Unlock()
	go rf.sendUpdateLogMsg()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	go rf.leaderLoop()
	go rf.startElectionIfNotLeader()
}

func (rf *Raft) leaderLoop() {
	for {
		select {
		case <-rf.leaderTicker.C:
			if r, _, _ := rf.getMetaInfo(); r == Leader {
				go rf.sendHeartBeats()
			}
			rf.resetLeaderTicker()
		}
	}
}

func (rf *Raft) startElectionIfNotLeader() {
	for {
		select {
		case <-rf.electionTicker.C:
			if r, _, _ := rf.getMetaInfo(); r == Follower || r == Candidate {
				go rf.startElection()
			}
			rf.resetTimerTicker()
		}
	}
}

func (rf *Raft) becomeFollower() {
	rf.role = Follower
}

func (rf *Raft) resetTimerTicker() {
	rf.electionTicker.Reset(time.Duration(350+(rand.Int63()%300)) * time.Millisecond)
}

func (rf *Raft) resetLeaderTicker() {
	rf.leaderTicker.Reset(SendHeartBeatsInterval)
}

func (rf *Raft) getMetaInfo() (role Role, term int, commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role, rf.currentTerm, rf.commitIndex
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntries = make([]*logEntry, 0)
	rf.logEntries = append(rf.logEntries, &logEntry{})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.electionTicker = time.NewTicker(time.Duration(250+(rand.Int63()%300)) * time.Millisecond)
	rf.leaderTicker = time.NewTicker(SendHeartBeatsInterval)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.batchBuffer = 0
	//deadlock.Opts.DeadlockTimeout = time.Second * 10

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLogEntries()

	return rf
}
