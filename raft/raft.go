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
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labrpc"
)

// For debug: write log to file
var (
	outfile, _ = os.OpenFile("testlogfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger     = log.New(outfile, "", 0)
)

// timeout low bound and range
var timeoutLowBound = 150
var timeoutRange = 150

// import "bytes"
// import "cs350/labgob"

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

type logEntryWithTerm struct {
	Command interface{}
	Term    int
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

	// Persistent state on all servers (Updated on stable storage before responding to RPCs)
	currentTerm int                // latest term server has seen (initlially 0)
	votedFor    int                // candidateId that received vote in current term (or -1 if Candidiate; .me, leader; else follower)
	log         []logEntryWithTerm // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0)

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // index of the next log entry that need to be sent to each server (initlized to leader last log index + 1)
	matchIndex []int // index of heighest log entry known to be replicated on each server (initlized to leader last log index + 1)

	// state for election
	voteReceived int // # of votes received

	// for timeout
	electionTimeout   int       // election's time out in millisecond
	heartbeatTimeout  int       // heartbeat's time out in millisecond
	timeLastOperation time.Time // the time since last action

	// for apply channel (to reply)
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
	var isleader bool = rf.me == rf.votedFor
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	LeaderId    int  // the actual leader id for the upper term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// logger.Printf("RequestVote: server %d(%d) received RequestVoteRPC from server %d(%d).\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm
	reply.LeaderId = rf.votedFor

	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		// logger.Printf("RequestVote: server %d denied server %d.\n", rf.me, args.CandidateId)
		return
	}

	if len(rf.log) == 0 {
		// logger.Printf("RequestVote: server %d log is empty. \n", rf.me)
		rf.timeLastOperation = time.Now()

		// grant vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term

		// resets its election timeout
		rand.Seed(time.Now().UnixNano())
		rf.electionTimeout = rand.Intn(timeoutLowBound) + timeoutRange

		return

		// logger.Printf("RequestVote: server %d grant vote to server %d.\n", rf.me, args.CandidateId)
	} else if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
		// grant vote if candidate's log is at least as up-to-date as receiver's log
		rf.timeLastOperation = time.Now()

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term

		// resets its election timeout
		rand.Seed(time.Now().UnixNano())
		rf.electionTimeout = rand.Intn(timeoutLowBound) + timeoutRange

		// logger.Printf("RequestVote: server %d grant vote to server %d.\n", rf.me, args.CandidateId)

		return
	} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1 {
		rf.timeLastOperation = time.Now()

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term

		// resets its election timeout
		rand.Seed(time.Now().UnixNano())
		rf.electionTimeout = rand.Intn(timeoutLowBound) + timeoutRange

		// logger.Printf("RequestVote: server %d grant vote to server %d.\n", rf.me, args.CandidateId)
		return
	} else {
		reply.VoteGranted = false
		// logger.Printf("RequestVote: server %d denied server %d.\n", rf.me, args.CandidateId)
		return
	}
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

	if !ok {
		logger.Printf("sendRequestVote: Server %d Call(\"Raft%d.RequestVote\") failed.\n", args.CandidateId, server)
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.VoteGranted {
		logger.Printf("sendRequestVote: Server %d received vote from %d\n", args.CandidateId, server)
		rf.voteReceived += 1
	} else {
		rf.currentTerm = reply.Term
		rf.votedFor = reply.LeaderId
		rf.timeLastOperation = time.Now()
	}

	return ok
}

type AppendEntriesArgs struct {
	Term         int                // leader's term
	LeaderId     int                // so follower can redirect clients
	PrevLogIndex int                // index of log entry immediately preceding new ones
	PrevLogTerm  int                // term of prevLogIndex entry
	Entries      []logEntryWithTerm // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int                // leader's commitIndex
}

type AppendEntriesReply struct {
	Term     int  // currentTerm, for leader to update itself
	LeaderId int  // the actual leader id for the upper term, for candidate to update itself
	Success  bool // true if follower contained entry matching prevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// logger.Printf("AppendEntries from %d: server %d received AppendEntriesRPC from server %d.\n", rf.me, args.LeaderId)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		logger.Printf("AppendEntries from %d: server %d convert back to follower. Term = %d \n", args.LeaderId, rf.me, args.Term)
		rf.votedFor = args.LeaderId
		return
	}

	reply.Term = rf.currentTerm
	reply.LeaderId = rf.votedFor

	if args.Term < rf.currentTerm {
		reply.Success = false
		logger.Printf("AppendEntries from %d: server %d denied server %d. Reason: 1.\n", args.LeaderId, rf.me, args.LeaderId)
		return
	}

	if args.LeaderId != rf.votedFor {
		reply.Success = false
		logger.Printf("AppendEntries from %d: server %d denied server %d. Not my leader.\n", args.LeaderId, rf.me, args.LeaderId)
		return
	}

	rf.timeLastOperation = time.Now()

	// 2. Reply false if log doesn't contain an entry at prevLogIndex ...
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Success = false
		logger.Printf("AppendEntries from %d: server %d denied server %d. Reason 2. \n", args.LeaderId, rf.me, args.LeaderId)
		logger.Printf("                 PrevLogIndex = %d. rf.log = %v", args.PrevLogIndex, rf.log)
		return
	}

	// 2.1 ... whose term matches prevLogTerm
	if 0 <= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		logger.Printf("AppendEntries from %d: server %d denied server %d. Reason 2.1. \n", args.LeaderId, rf.me, args.LeaderId)
		logger.Printf("                 PrevLogIndex = %d. rf.log = %v", args.PrevLogIndex, rf.log)
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it.
	if 0 <= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		logger.Printf("AppendEntries from %d: server %d deleting log after log[%d] \n", args.LeaderId, rf.me, args.PrevLogIndex)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	rf.log = append(rf.log, args.Entries...)
	logger.Printf("AppendEntries from %d: server %d has appended log. \n               %v \n", args.LeaderId, rf.me, rf.log)

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		N := min(args.LeaderCommit, args.PrevLogIndex+1)

		logger.Printf("        rf.commitIndex now = %d \n", N)

		for i := rf.commitIndex + 1; i <= N; i++ {
			logger.Printf("Commit: server %d commiting log %d\n", rf.me, i)
			rf.commitIndex = i
			applymessage := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- applymessage
			logger.Printf("                ApplyMsg = %v\n", applymessage)
			// logger.Printf("                ApplyMsg's Command = %v\n", rf.log[N].Command)
		}

		rf.commitIndex = N
	}

	if len(args.Entries) == 0 {
		// heartbeat messages
		reply.Success = true
		logger.Printf("AppendEntries from %d: server %d's current log: %v \n", args.LeaderId, rf.me, rf.log)
		return
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		// logger.Printf("sendAppendEntries from %d: Server %d Call(\"Raft%d.AppendEntries\") failed.\n", args.LeaderId, server)
		return ok
	}

	if reply.Success {
		// logger.Printf("sendAppendEntries from %d: server %d's request to server %d succeed.\n", args.LeaderId, server)
		rf.mu.Lock()
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

		rf.commit_checker()
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			logger.Printf("AppendEntries: candidate %d's term is higher than mine (%d).\n", server, args.LeaderId)
			rf.currentTerm = reply.Term
			rf.votedFor = reply.LeaderId
			rf.timeLastOperation = time.Now()
			rf.mu.Unlock()
			return ok
		}

		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server] -= 1
		}

		rf.mu.Unlock()

	}

	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.me == rf.votedFor

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	// adding the log
	newLog := logEntryWithTerm{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, newLog)

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	rf.mu.Lock()
	initial_sleeptime := time.Duration(rf.electionTimeout) * time.Millisecond
	rf.mu.Unlock()

	time.Sleep(initial_sleeptime)

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()

		// check if we are leader
		if rf.votedFor == rf.me {

			// if leader, send out HeartBeat RPC
			logger.Printf("Ticker: server %d is leader. Sending AppendEntryRPC. --------------------\n", rf.me)
			logger.Printf("        leader server %d log: %v \n", rf.me, rf.log)
			logger.Printf("        leader server %d nextIndex: %v \n", rf.me, rf.nextIndex)

			// args_temp := AppendEntriesArgs{
			// 	Term:         rf.currentTerm,
			// 	LeaderId:     rf.me,
			// 	PrevLogIndex: len(rf.log) - 2,
			// 	LeaderCommit: rf.commitIndex,
			// }

			// deep copy fields
			TermCopy := rf.currentTerm
			LeaderIdCopy := rf.me
			logCopy := []logEntryWithTerm{}
			logCopy = append(logCopy, rf.log...)
			nextIndexCopy := rf.nextIndex
			commitIndexCopy := rf.commitIndex

			rf.mu.Unlock()

			for peer_index, _ := range rf.peers {
				if peer_index != rf.me {
					reply := AppendEntriesReply{}

					// make copy of the args_temp
					args := AppendEntriesArgs{
						Term:         TermCopy,
						LeaderId:     LeaderIdCopy,
						PrevLogIndex: nextIndexCopy[peer_index] - 1,
						LeaderCommit: commitIndexCopy,
					}

					if len(logCopy) > 1 {
						args.PrevLogTerm = logCopy[args.PrevLogIndex].Term
					}

					// send the appropriate log
					logIndexToSent := nextIndexCopy[peer_index]
					// logger.Printf("        logIndexToSent_%d = %d len(logCopy) = %v \n", peer_index, logIndexToSent, len(logCopy))
					if len(logCopy) > 1 && logIndexToSent < len(logCopy) {
						args.Entries = []logEntryWithTerm{logCopy[logIndexToSent]}
					}

					go rf.sendAppendEntries(peer_index, &args, &reply)
				}
			}
		} else if rf.votedFor == -1 {
			rf.mu.Unlock()
			// Candidate
			rf.startElection()
		} else {
			// Follower
			// check if time out
			deadline := rf.timeLastOperation.Add(time.Millisecond * time.Duration(rf.heartbeatTimeout))
			if time.Now().After(deadline) {
				logger.Printf("Server %d timed out.\n", rf.me)
				// if timeout, become candiate
				rf.votedFor = -1
			}
			rf.mu.Unlock()
		}

		// for rf.commitIndex > rf.lastApplied {
		// 	rf.lastApplied += 1
		// 	// TODO: apply log[lastApplied] to state machine
		// }
		time.Sleep(time.Millisecond * time.Duration(timeoutLowBound/2))
	}
}

func (rf *Raft) startElection() bool {
	logger.Printf("Server %d started election.\n", rf.me)

	rf.mu.Lock()
	rf.timeLastOperation = time.Now()

	rf.currentTerm += 1
	rf.voteReceived = 1

	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(timeoutLowBound) + timeoutRange // random number between 150 and 300

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	if len(rf.log) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}

	// get election timeout
	deadline := rf.timeLastOperation.Add(time.Millisecond * time.Duration(rf.electionTimeout))

	rf.mu.Unlock()

	for peer_index, _ := range rf.peers {
		if peer_index != rf.me {
			reply := RequestVoteReply{}

			go rf.sendRequestVote(peer_index, &args, &reply)
		}
	}

	for time.Now().Before(deadline) {
		rf.mu.Lock()
		if rf.currentTerm == args.Term && rf.voteReceived > (len(rf.peers)/2) {
			// if we are still on the same term and got enough votes, become the leader
			logger.Printf("server %d is now the leader for term %d.\n", rf.me, rf.currentTerm)
			logger.Printf("          server %d has log: %v \n", rf.me, rf.log)
			rf.votedFor = rf.me

			lastLogIndex := len(rf.log)
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = lastLogIndex
				rf.matchIndex[i] = 0
			}

			rf.mu.Unlock()
			return true
		}
		rf.mu.Unlock()
	}

	logger.Printf("server %d election timeout for term %d.\n", rf.me, args.Term)
	return false
}

func (rf *Raft) commit_checker() {
	overHalf := len(rf.peers) / 2

	logger.Printf("commit_checker: commitIndex = %d \n", rf.commitIndex)
	logger.Printf("                matchIndex = %v\n", rf.matchIndex)

	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] >= N,
	// and log[N].term == currentTerm:
	// set comitIndex = N
	for N := rf.commitIndex + 1; N < len(rf.log); N++ {
		counter := 1
		for _, index := range rf.matchIndex {
			if index >= N {
				counter++
			}
			if counter > overHalf {
				logger.Printf("                %d\n", index)
				break
			}
		}

		if counter > overHalf && rf.log[N].Term == rf.currentTerm {
			for i := rf.commitIndex + 1; i <= N; i++ {
				logger.Printf("Commit_checker: server %d commiting log %d\n", rf.me, i)
				rf.commitIndex = i
				applymessage := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
				rf.applyCh <- applymessage
				logger.Printf("                ApplyMsg = %v\n", applymessage)
				// logger.Printf("                ApplyMsg's Command = %v\n", rf.log[N].Command)
			}

			break
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// Persisitent state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []logEntryWithTerm{{nil, 0}}

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile state on leaders
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(timeoutLowBound) + timeoutRange
	rf.heartbeatTimeout = rand.Intn(timeoutLowBound) + timeoutRange

	rf.timeLastOperation = time.Now()

	// for the apply channel
	rf.applyCh = applyCh

	// send out the dummy log for testing
	applymessage := ApplyMsg{
		CommandValid: true,
		Command:      rf.log[0].Command,
		CommandIndex: 0,
	}
	rf.applyCh <- applymessage

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
