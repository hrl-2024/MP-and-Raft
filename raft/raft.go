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
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"bytes"

	"cs350/labgob"
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
	Index   int
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
	voteReceived      int  // # of votes received
	myElectionStarted bool // indicate if this server has started a election

	// for timeout
	electionTimeout   int       // election's time out in millisecond
	heartbeatTimeout  int       // heartbeat's time out in millisecond
	timeLastOperation time.Time // the time since last action

	// for apply channel (to reply)
	applyCh chan ApplyMsg

	applyChBuffer chan ApplyMsg
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
	rf.persister.SaveRaftState(rf.encodeState())
	// fmt.Printf("                                                                    persist: saving state for server %d\n", rf.me)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntryWithTerm
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		logger.Fatal("                                                                    readPersist: decode error")
	} else {
		fmt.Printf("                                                                    readPersist:---------restoring server %d stats\n", rf.me)
		fmt.Printf("                                                                                         currentTerm = %d votedFor = %d log = %v\n", currentTerm, votedFor, log)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		fmt.Printf("                                                                                         rf: currentTerm = %d votedFor = %d log = %v\n", rf.currentTerm, rf.votedFor, rf.log)
		rf.persist()
		rf.timeLastOperation = time.Now()
	}
}

// For 2D
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // Term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Success  bool // indicating if Installment is successful
	Term     int  // currentTerm, for leader to update itself
	LeaderId int  // the currentTerm's LeaderIs
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		fmt.Printf("InstallSnapshot: args.Term (%d) < mine(%d) \n", args.Term, rf.currentTerm)
		reply.LeaderId = rf.votedFor
		return
	}

	if args.Term > rf.currentTerm {
		// convert back to follower
		fmt.Printf("InstallSnapshot: %d convert back to follower by server %d \n", rf.me, args.LeaderId)
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.timeLastOperation = time.Now()
		rf.persist()
	}

	// already snapshotted
	// if args.LastIncludedIndex < rf.log[0].Index {
	// 	fmt.Printf("InstallSnapshot: server %d already snapshotted log %d. rf.log[0].Index = %d \n", rf.me, args.LastIncludedIndex, rf.log[0].Index)
	// 	reply.Success = true
	// 	rf.timeLastOperation = time.Now()
	// 	return
	// }

	// if args.LastIncludedIndex > rf.log[len(rf.log)-1].Index {
	// discard any existing or partial snapshot with a smaller index
	rf.log = []logEntryWithTerm{{nil, args.LastIncludedTerm, args.LastIncludedIndex}}

	rf.applyChBuffer <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.commitIndex = args.LastIncludedIndex
	rf.timeLastOperation = time.Now()

	reply.Success = true

	// Save state and snapshot
	state_data := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(state_data, args.Data)

	fmt.Printf("InstallSnapshot: server %d's log[0] = %v\n", rf.me, rf.log[0])
	// }
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	if !ok {
		fmt.Printf("sendInstallSnapshot: Server %d Call(\"Raft%d.InstallSnapshot\") failed.\n", args.LeaderId, server)
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		fmt.Printf("sendInstallSnapshot: server %d convert back to follower. Now, term = %d. Leader = %d\n", rf.me, reply.Term, reply.LeaderId)
		rf.currentTerm = reply.Term
		if reply.LeaderId != -1 {
			rf.votedFor = reply.LeaderId
		} else {
			rf.votedFor = server
		}
		// resets its election timeout
		rand.Seed(time.Now().UnixNano())
		rf.heartbeatTimeout = rand.Intn(timeoutLowBound) + timeoutRange
		rf.timeLastOperation = time.Now()
		rf.persist()
	} else if reply.Success {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		fmt.Printf("sendInstallSnapshot: leader %d's nextIndex[%d] = %d. matchIndex[%d] = %d\n", rf.me, server, rf.nextIndex[server], server, rf.matchIndex[server])
	}

	return ok
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log[0].Index {
		fmt.Printf(`Snapshot: Server %d(%d) rejects snapshotting %d. 
						Reason: %d <= %d (snapShotLastIndex)`, rf.me, rf.currentTerm, index,
			index, rf.log[0].Index)
		return
	} else if index > rf.commitIndex {
		fmt.Printf(`Snapshot: Server %d(%d) rejects snapshotting %d. 
						Reason: %d > %d (commitIndex)`, rf.me, rf.currentTerm, index,
			index, rf.commitIndex)
		return
	}

	fmt.Printf("Snapshot: Server %d(%d) is snapshotting %d.\n", rf.me, rf.currentTerm, index)

	// Trim log until index
	rf.trimLogUntilIndex(index)

	// Save state and snapshot
	state_data := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(state_data, snapshot)

	fmt.Printf("          Server %d(%d)'s first log now = %v. snaplastIndex = %d. LastTerm = %d.\n", rf.me, rf.currentTerm, rf.log[0], rf.log[0].Index, rf.log[0].Term)
}

func (rf *Raft) trimLogUntilIndex(index int) {
	rf.log = rf.log[index-rf.log[0].Index:]
	rf.log[0].Command = nil
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
	Term              int  // currentTerm, for candidate to update itself
	LeaderId          int  // the actual leader id for the upper term, for candidate to update itself
	VoteGranted       bool // true means candidate received vote
	FailDueToElection bool // true means the receipient is in election, reach it later.
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("RequestVote: server %d(%d) received RequestVoteRPC from server %d(%d).\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		reply.LeaderId = rf.votedFor
		fmt.Printf("RequestVote: server %d rejected server %d. Reason: candidate's term <= mine.\n", rf.me, args.CandidateId)
		return
	}

	if args.Term > rf.currentTerm && rf.votedFor == rf.me {
		// convert back to follower
		fmt.Printf("RequestVote: server %d step down by server %d.\n", rf.me, args.CandidateId)
		reply.VoteGranted = false
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.timeLastOperation = time.Now()
		rf.persist()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			// if candidate's log is at least as updated as receiver's log, grant vote
			if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
				// If the logs have last entries with different terms, then the log with the later term is more up-to-date
				rf.timeLastOperation = time.Now()

				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.currentTerm = args.Term

				// resets its election timeout
				rand.Seed(time.Now().UnixNano())
				rf.heartbeatTimeout = rand.Intn(timeoutLowBound) + timeoutRange

				fmt.Printf("RequestVote: server %d grant vote to server %d. Reason: Prev log term is bigger.\n", rf.me, args.CandidateId)

				rf.persist()

				return
			}
			if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
				if args.LastLogIndex >= rf.log[len(rf.log)-1].Index {
					rf.timeLastOperation = time.Now()

					reply.VoteGranted = true
					rf.votedFor = args.CandidateId
					rf.currentTerm = args.Term

					// resets its election timeout
					rand.Seed(time.Now().UnixNano())
					rf.heartbeatTimeout = rand.Intn(timeoutLowBound) + timeoutRange

					fmt.Printf("RequestVote: server %d grant vote to server %d. Reason: same term, log index is at least uptodate \n", rf.me, args.CandidateId)

					rf.persist()
					return
				} else {
					reply.VoteGranted = false
					reply.Term = rf.currentTerm
					reply.LeaderId = rf.votedFor
					fmt.Printf("RequestVote: server %d rejected server %d. Reason: same term, but index shorter\n", rf.me, args.CandidateId)
					return
				}
			} else {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				reply.LeaderId = rf.votedFor
				fmt.Printf("RequestVote: server %d rejected server %d. Reason: Log's term less. \n", rf.me, args.CandidateId)
				return
			}
		}
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

	rf.mu.Lock()
	if args.Term != rf.currentTerm && !rf.myElectionStarted {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		fmt.Printf("sendRequestVote: Server %d Call(\"Raft%d.RequestVote\") failed.\n", args.CandidateId, server)
		return ok
	}

	if reply.VoteGranted {
		rf.mu.Lock()
		if args.Term == rf.currentTerm && rf.myElectionStarted {
			fmt.Printf("sendRequestVote: Server %d received vote for term %d from %d\n", args.CandidateId, args.Term, server)
			rf.voteReceived += 1
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			fmt.Printf("sendRequestVote: server %d convert back to follower. Now, term = %d. Leader = %d\n", rf.me, reply.Term, reply.LeaderId)
			rf.currentTerm = reply.Term
			rf.votedFor = reply.LeaderId
			// resets its election timeout
			rand.Seed(time.Now().UnixNano())
			rf.heartbeatTimeout = rand.Intn(timeoutLowBound) + timeoutRange
			rf.timeLastOperation = time.Now()
			rf.persist()
		}
		fmt.Printf("sendRequestVote: server %d (term = %d, Leader = %d) rejected server %d.\n", server, reply.Term, reply.LeaderId, args.CandidateId)
		if reply.Term >= rf.currentTerm && reply.LeaderId == -1 {
			rf.currentTerm = reply.Term
			rf.votedFor = server
			rf.myElectionStarted = false
			rf.persist()
		}

		rf.mu.Unlock()
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

	ConflictingTerm      int // the term of the conflicting entry
	FirstIndexForArgTerm int // first index of the conflicting entry term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("AppendEntries from %d: server %d received AppendEntriesRPC from server %d.\n", rf.me, args.LeaderId)

	// if rf.myElectionStarted {
	// 	// gotta wait
	// 	reply.Success = false
	// 	fmt.Printf("    AppendEntries from %d: server %d in election. Gotta wait. \n", args.LeaderId, rf.me)
	// 	return
	// }

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		fmt.Printf("    AppendEntries from %d: server %d convert back to follower. Term = %d \n", args.LeaderId, rf.me, args.Term)
		rf.timeLastOperation = time.Now()
		rf.votedFor = args.LeaderId
		rf.persist()
		return
	}

	reply.Term = rf.currentTerm
	reply.LeaderId = rf.votedFor

	if args.Term < rf.currentTerm {
		reply.Success = false
		fmt.Printf("    AppendEntries from %d: server %d denied server %d. Reason: args.Term < currentTerm.\n", args.LeaderId, rf.me, args.LeaderId)
		return
	}

	if args.LeaderId != rf.votedFor {
		reply.Success = false
		fmt.Printf("AppendEntries from %d: server %d denied server %d. Not my leader.\n", args.LeaderId, rf.me, args.LeaderId)
		return
	}

	rf.timeLastOperation = time.Now()

	// For snapshot, if out of range, reply false. (Leader should send installSnapshotRPC again.)
	if args.PrevLogIndex < rf.log[0].Index {
		reply.Success = false

		fmt.Printf("    AppendEntries from %d: server %d denied server %d. Reason: PrevLogIndex smaller than snapshotted index.\n", args.LeaderId, rf.me, args.LeaderId)
		return
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex ...
	if args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
		reply.Success = false

		// reply.FirstIndexForArgTerm = rf.log[len(rf.log)-1].Index

		fmt.Printf("    AppendEntries from %d: server %d denied server %d. Reason: no log at PrevlogIndex.\n", args.LeaderId, rf.me, args.LeaderId)
		return
	}

	// 2.1 ... whose term matches prevLogTerm
	fmt.Printf("    AppendEntries %d->%d: args.PrevLogIndex=%d. rf.log[0].Index = %d\n", args.LeaderId, rf.me, args.PrevLogIndex, rf.log[0].Index)
	if 0 <= args.PrevLogIndex && rf.log[args.PrevLogIndex-rf.log[0].Index].Term != args.PrevLogTerm {
		reply.Success = false

		reply.ConflictingTerm = rf.log[args.PrevLogIndex-rf.log[0].Index].Term
		// find the reply.FirstIndexForArgTerm
		i := args.PrevLogIndex - rf.log[0].Index
		for i > 0 && rf.log[i].Term == reply.ConflictingTerm {
			i--
		}
		reply.FirstIndexForArgTerm = i + 1 + rf.log[0].Index

		fmt.Printf("    AppendEntries from %d: server %d denied server %d. Reason: Term mismatch at log[PrevlogIndex].\n", args.LeaderId, rf.me, args.LeaderId)
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it.
	if 0 <= args.PrevLogIndex && rf.log[args.PrevLogIndex-rf.log[0].Index].Term == args.PrevLogTerm {
		fmt.Printf("    AppendEntries from %d: server %d deleting log after log[%d] \n", args.LeaderId, rf.me, args.PrevLogIndex)
		rf.log = rf.log[:args.PrevLogIndex-rf.log[0].Index+1]
	}

	rf.log = append(rf.log, args.Entries...)
	fmt.Printf("    AppendEntries from %d: server %d has appended log. \n               %v \n", args.LeaderId, rf.me, rf.log)
	rf.persist()

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		N := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

		fmt.Printf("        rf.commitIndex now = %d. LeaderCommit = %d. Last new index = %d. Last log's index = %d \n", N, args.LeaderCommit, args.PrevLogIndex+len(args.Entries), rf.log[len(rf.log)-1].Index)

		for i := rf.commitIndex + 1; i <= N; i++ {
			if i < rf.log[0].Index {
				continue
			}
			fmt.Printf("Commit: server %d committing log %d\n", rf.me, i)
			rf.commitIndex = i
			applymessage := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.log[0].Index].Command,
				CommandIndex: i,
			}
			rf.applyChBuffer <- applymessage
			fmt.Printf("                ApplyMsg = %v\n", applymessage)
			// fmt.Printf("                ApplyMsg's Command = %v\n", rf.log[N].Command)
		}
	}

	if len(args.Entries) == 0 {
		// heartbeat messages
		reply.Success = true
		fmt.Printf("    AppendEntries from %d: server %d's current log: %v \n", args.LeaderId, rf.me, rf.log)
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

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		fmt.Printf("sendAppendEntries: Server %d Call(\"Raft%d.AppendEntries\") failed.\n", args.LeaderId, server)
		return ok
	}

	if reply.Success {
		// fmt.Printf("sendAppendEntries from %d: server %d's request to server %d succeed.\n", args.LeaderId, server)
		rf.mu.Lock()
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

		fmt.Printf("sendAppendEntries: %d(%d): matchIndex for server %d is now %d. args.PrevLogIndex = %d\n", rf.me, rf.currentTerm, server, rf.matchIndex[server], args.PrevLogIndex)

		rf.commit_checker()
		rf.mu.Unlock()
	} else {
		if reply.LeaderId == -1 {
			// this mean that server is in election. Don't trust its votedFor and step down.
			return ok
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm && reply.LeaderId != -1 {
			fmt.Printf("sendAppendEntries: candidate %d's term is higher than mine (%d).\n", server, args.LeaderId)
			rf.currentTerm = reply.Term
			rf.votedFor = reply.LeaderId
			rf.timeLastOperation = time.Now()
			rf.persist()
			rf.mu.Unlock()
			return ok
		}

		if rf.nextIndex[server] > 1 && reply.FirstIndexForArgTerm != 0 {
			// fmt.Printf("sendAppendEntries: nextIndex for server %d decreased from %d to %d\n", server, rf.nextIndex[server], reply.FirstIndexForArgTerm)
			rf.nextIndex[server] = reply.FirstIndexForArgTerm
		} else if rf.nextIndex[server] > 1 {
			rf.nextIndex[server] = 1
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
	index := rf.log[len(rf.log)-1].Index + 1
	term := rf.currentTerm
	isLeader := rf.me == rf.votedFor && !rf.myElectionStarted

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	// adding the log
	newLog := logEntryWithTerm{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log = append(rf.log, newLog)
	rf.persist()

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
			logLn1 := fmt.Sprintf("Ticker: server %d is leader for term %d. Sending AppendEntryRPC. --------------------\n", rf.me, rf.currentTerm)
			logLn2 := fmt.Sprintf("        leader server %d log: %v \n", rf.me, rf.log)
			logLn3 := fmt.Sprintf("        leader server %d nextIndex: %v \n", rf.me, rf.nextIndex)
			fmt.Printf(logLn1 + logLn2 + logLn3)

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
			nextIndexCopy := []int{}
			nextIndexCopy = append(nextIndexCopy, rf.nextIndex...)
			commitIndexCopy := rf.commitIndex

			rf.mu.Unlock()

			for peer_index, _ := range rf.peers {
				rf.mu.Lock()
				if rf.votedFor != rf.me {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()

				if peer_index != rf.me {
					reply := AppendEntriesReply{}

					// make copy of the args_temp
					args := AppendEntriesArgs{
						Term:         TermCopy,
						LeaderId:     LeaderIdCopy,
						PrevLogIndex: nextIndexCopy[peer_index] - 1,
						LeaderCommit: commitIndexCopy,
					}

					if args.PrevLogIndex >= logCopy[0].Index {
						// if len(logCopy) > 1 {
						// 	args.PrevLogTerm = logCopy[args.PrevLogIndex-logCopy[0].Index].Term
						// }

						args.PrevLogTerm = logCopy[args.PrevLogIndex-logCopy[0].Index].Term

						// send the appropriate log
						logIndexToSent := nextIndexCopy[peer_index] - logCopy[0].Index
						// fmt.Printf("        logIndexToSent_%d = %d len(logCopy) = %v \n", peer_index, logIndexToSent, len(logCopy))
						if len(logCopy) > 1 && logIndexToSent < len(logCopy) {
							args.Entries = logCopy[logIndexToSent:]
						}

						go rf.sendAppendEntries(peer_index, &args, &reply)

					} else {
						// Send snapshot RPC to the lagger so it can catch up
						rf.mu.Lock()
						SnapshotArgs := InstallSnapshotArgs{
							Term:              rf.currentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.log[0].Index,
							LastIncludedTerm:  rf.log[0].Term,
							Data:              rf.persister.ReadSnapshot(),
							Done:              true,
						}

						SnapshotReply := InstallSnapshotReply{}
						go rf.sendInstallSnapshot(peer_index, &SnapshotArgs, &SnapshotReply)

						fmt.Printf("sendInstallSnapshot: server %d --> %d. LastIncludedIndex = %d. LastIncludedTerm = %d \n", rf.me, peer_index, SnapshotArgs.LastIncludedIndex, SnapshotArgs.LastIncludedTerm)

						rf.mu.Unlock()
					}
				}
			}
		} else if rf.votedFor == -1 {
			term := rf.currentTerm + 1
			rf.mu.Unlock()
			// Candidate
			rf.startElection(term)
		} else {
			// Follower
			// check if time out
			deadline := rf.timeLastOperation.Add(time.Millisecond * time.Duration(rf.heartbeatTimeout))
			if time.Now().After(deadline) && !rf.myElectionStarted {
				fmt.Printf("Server %d timed out.\n", rf.me)
				// if timeout, become candiate
				rf.votedFor = -1
				rf.persist()
			}
			rf.mu.Unlock()
		}

		// for rf.commitIndex > rf.lastApplied {
		// 	rf.lastApplied += 1
		// 	// TODO: apply log[lastApplied] to state machine
		// }
		time.Sleep(time.Millisecond * time.Duration(timeoutLowBound-50))
	}
}

func (rf *Raft) startElection(term int) bool {
	rf.mu.Lock()
	if rf.currentTerm >= term && rf.votedFor != -1 {
		rf.mu.Unlock()
		return false
	}
	fmt.Printf("Server %d started election for term %d.\n", rf.me, term)
	rf.timeLastOperation = time.Now()

	rf.currentTerm += 1
	rf.myElectionStarted = true
	rf.voteReceived = 1

	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(timeoutLowBound) + timeoutRange // random number between 150 and 300

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	args.LastLogIndex = rf.log[len(rf.log)-1].Index
	args.LastLogTerm = rf.log[len(rf.log)-1].Term

	// get election timeout
	deadline := rf.timeLastOperation.Add(time.Millisecond * time.Duration(rf.electionTimeout))

	for peer_index, _ := range rf.peers {
		if peer_index != rf.me {
			reply := RequestVoteReply{}

			go rf.sendRequestVote(peer_index, &args, &reply)
		}
	}

	rf.mu.Unlock()

	for time.Now().Before(deadline) {
		rf.mu.Lock()
		if (rf.currentTerm > term && rf.votedFor != -1) || !rf.myElectionStarted {
			fmt.Printf("      server %d election for term %d failed. currentTerm = %d. votedFor = %d\n", rf.me, args.Term, rf.currentTerm, rf.votedFor)
			rf.myElectionStarted = false
			rf.mu.Unlock()
			return false
		}
		if rf.currentTerm == args.Term && rf.voteReceived > (len(rf.peers)/2) {
			// if we are still on the same term and got enough votes, become the leader
			fmt.Printf("server %d is now the leader for term %d.\n", rf.me, rf.currentTerm)
			fmt.Printf("          server %d has log: %v \n", rf.me, rf.log)
			rf.votedFor = rf.me

			lastLogIndex := rf.log[len(rf.log)-1].Index + 1
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = lastLogIndex
				rf.matchIndex[i] = 0
			}

			rf.myElectionStarted = false
			rf.persist()

			// resets its election timeout
			rand.Seed(time.Now().UnixNano())
			rf.heartbeatTimeout = rand.Intn(timeoutLowBound) + timeoutRange
			rf.timeLastOperation = time.Now()
			rf.mu.Unlock()
			return true
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	fmt.Printf("server %d election timeout for term %d.\n", rf.me, term)
	rf.myElectionStarted = false
	rf.mu.Unlock()

	return false
}

func (rf *Raft) commit_checker() {
	overHalf := len(rf.peers) / 2

	fmt.Printf("commit_checker: %d commitIndex = %d \n", rf.me, rf.commitIndex)
	fmt.Printf("                %d matchIndex = %v\n", rf.me, rf.matchIndex)

	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] >= N,
	// and log[N].term == currentTerm:
	// set comitIndex = N
	for N := rf.commitIndex + 1; N < rf.log[len(rf.log)-1].Index+1; N++ {
		counter := 1
		for _, index := range rf.matchIndex {
			if index >= N {
				counter++
			}
			if counter > overHalf {
				// fmt.Printf("                %d\n", index)
				break
			}
		}

		if counter > overHalf && rf.log[N-rf.log[0].Index].Term == rf.currentTerm {
			fmt.Printf("                %d commit_checker: N = %d\n", rf.me, N)

			for i := rf.commitIndex + 1; i <= N; i++ {
				if i < rf.log[0].Index {
					continue
				}
				fmt.Printf("Commit_checker: server %d committing log %d\n", rf.me, i)
				rf.commitIndex = i
				applymessage := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i-rf.log[0].Index].Command,
					CommandIndex: i,
				}
				rf.applyChBuffer <- applymessage
				fmt.Printf("                ApplyMsg = %v\n", applymessage)
				// fmt.Printf("                ApplyMsg's Command = %v\n", rf.log[N].Command)
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
	rf.log = []logEntryWithTerm{{nil, 0, 0}}

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile state on leaders
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// for election
	rf.myElectionStarted = false

	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(timeoutLowBound) + timeoutRange
	rf.heartbeatTimeout = rand.Intn(timeoutLowBound) + timeoutRange

	rf.timeLastOperation = time.Now()

	// for the apply channel
	rf.applyCh = applyCh

	// For snapshot

	rf.applyChBuffer = make(chan ApplyMsg, 1000)

	// send out the dummy log for testing
	applymessage := ApplyMsg{
		CommandValid: true,
		Command:      0,
		CommandIndex: 0,
	}
	rf.applyChBuffer <- applymessage

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
	}

	if rf.me == rf.votedFor {
		rf.votedFor = -1
		rf.persist()
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.sendBufferToApplyCh()

	return rf
}

func (rf *Raft) sendBufferToApplyCh() {
	for !rf.killed() {
		rf.applyCh <- <-rf.applyChBuffer
	}
}
