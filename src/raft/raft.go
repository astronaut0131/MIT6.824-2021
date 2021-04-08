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
	"fmt"

	//	"go/ast"
	"math/rand"
	"log"
	"os"
	"strconv"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const ElectionTimeoutMin int = 400
const ElectionTimeoutMax int = 600
const HeartBeatTimeout int = 100
const RequestVoteTimeout int = 1000

type logTopic string
const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)
var debugStart time.Time
var debugVerbosity int

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type LogEntry struct {
	Command string
	Term int
}
type State int
const (
	Leader State = 0
	Follower State = 1
	Candidate State = 2
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor 	int
	log			[]LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	lastHeartBeat time.Time
	state       State
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
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
}

func (rf* Raft) AtLeastUpToDate(LastLogIndex int,LastLogTerm int) bool {
	lastIndex := len(rf.log) - 1
	return lastIndex < LastLogIndex ||
		(lastIndex == LastLogIndex && LastLogTerm >= rf.log[lastIndex].Term)
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote,"S%d: got RequestVote from %d",rf.me,args.CandidateID)
	currentTerm := rf.currentTerm
	rf.UpdateTermWithLock(args.Term,args.CandidateID)
	reply.Term = currentTerm
	reply.VoteGranted = false
	if args.Term < currentTerm {
		return
	}

	if rf.votedFor == args.CandidateID ||
		(rf.votedFor == -1 && rf.AtLeastUpToDate(args.LastLogIndex,args.LastLogTerm)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}


	if reply.VoteGranted {
		Debug(dVote,"S%d: grant RequestVote from %d",rf.me,args.CandidateID)
	} else {
		Debug(dVote,"S%d: reject RequestVote from %d",rf.me,args.CandidateID)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// reset election timer
	rf.lastHeartBeat = time.Now()
	rf.UpdateTermWithLock(args.Term,args.LeaderId)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	Debug(dDrop,"S%d: killed",rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) majority() int {
	return len(rf.peers) / 2 + 1
}


// should hold rf.mu before calling this function
func (rf *Raft) UpdateTermWithLock(term int,peerID int) {
	if rf.currentTerm >= term {
		return
	} else {
		Debug(dTerm,"S%d: learn higher term from %d, return to follower",rf.me,peerID)
		rf.currentTerm = term
		rf.state = Follower
		// empty voteFor in new term
		rf.votedFor = -1
	}
}

func (rf *Raft) startElection() {
	Debug(dVote,"S%d: pre start election",rf.me)
	rf.mu.Lock()
	if rf.state == Candidate || rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	Debug(dVote,"S%d: start election",rf.me)
	rf.currentTerm++
	rf.state = Candidate
	// reset election timer
	rf.lastHeartBeat = time.Now()
	// vote for itself
	rf.votedFor = rf.me
	count := 1
	finished := 1
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	for peerID := range rf.peers {
		if peerID != rf.me {
			go func(peerID int) {
				reply := &RequestVoteReply{}
				Debug(dVote,"S%d: sending RequestVote to %d",rf.me,peerID)
				ok := rf.sendRequestVote(peerID,args,reply)
				rf.mu.Lock()
				rf.UpdateTermWithLock(reply.Term,peerID)
				rf.mu.Unlock()
				mu.Lock()
				defer mu.Unlock()
				if ok && reply.VoteGranted {
					count++
				}
				finished++
				Debug(dVote,"S%d: got count %d, finished %d",rf.me,count,finished)
				cond.Broadcast()
			}(peerID)
		}
	}
	startTime := time.Now()
	timeout := time.Duration(RequestVoteTimeout)*time.Millisecond
	// timer
	// force finishing vote after certain timeout
	go func() {
		time.Sleep(timeout)
		cond.Broadcast()
	}()
	mu.Lock()
	for time.Since(startTime) < timeout && count < rf.majority() && finished != len(rf.peers) {
		cond.Wait()
	}
	votedCount := count
	mu.Unlock()

	rf.mu.Lock()
	Debug(dVote,"S%d: vote count %d",rf.me,votedCount)
	if votedCount >= rf.majority() && rf.state == Candidate {
		Debug(dLeader,"S%d: become leader for term %d",rf.me,rf.currentTerm)
		rf.state = Leader
		go rf.BroadcastHeartBeats()
	} else {
		Debug(dVote,"S%d: election failed",rf.me)
		rf.state = Follower
	}
	rf.mu.Unlock()
}

func (rf *Raft) BroadcastHeartBeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		for peerID := range rf.peers {
			if peerID != rf.me {
				go func(peerID int) {
					reply := &AppendEntriesReply{}
					//Debug(dLeader,"S%d: sending HeartBeat to %d",rf.me,peerID)
					rf.sendAppendEntries(peerID,args,reply)
					rf.mu.Lock()
					rf.UpdateTermWithLock(reply.Term,peerID)
					rf.mu.Unlock()
				}(peerID)
			}
		}
		timeout := time.Duration(HeartBeatTimeout) * time.Millisecond
		time.Sleep(timeout)
	}
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timeout := time.Duration(rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin) * time.Millisecond
		time.Sleep(timeout)
		rf.mu.Lock()
		isLeader := rf.state == Leader
		lastHeartBeat := rf.lastHeartBeat
		rf.mu.Unlock()
		if !isLeader && time.Since(lastHeartBeat) > timeout {
			rf.startElection()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		currentTerm:   0,
		votedFor:      -1,
		log:           nil,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     nil,
		matchIndex:    nil,
		lastHeartBeat: time.Now(),
		state:         Follower,
	}
	Debug(dInfo,"S%d: starting, majority is %d",rf.me,rf.majority())
	// Your initialization code here (2A, 2B, 2C).
	rf.log = append(rf.log,LogEntry{
		Command: "",
		Term:    0,
	})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
