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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}


type RaftState int
const (
	FollowerState	RaftState = 0
	CandidateState	RaftState = 1
	LeaderState		RaftState = 2
)

type LogType struct {
	command		interface{}
	logTerm		int
}

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
	haveMessagePeriod	bool
	state			RaftState
	applyCh			chan ApplyMsg

	currentTerm		int
	votedFor		int
	logs			[]LogType

	commitIndex		int

	nextIndex		[]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LeaderState
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("(RequestVote handler false, term outdated) %v(term: %v)->%v(term: %v)",
				args.CandidateId, args.Term, rf.me, rf.currentTerm)
		return
	}

	if rf.currentTerm < args.Term || rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.currentTerm < args.Term {
			// If RPC request or response contains term T > currentTerm: 
			// set currentTerm = T, convert to follower
			rf.state = FollowerState
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
		}

		reply.Term = rf.currentTerm
		lastLogIndex := len(rf.logs)-1
		lastLogTerm := rf.logs[lastLogIndex].logTerm
		// election restriction
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			DPrintf("(RequestVote handler true) %v(term: %v, log index: %v, log term: %v)->%v(term: %v, log index: %v, log term: %v), ",
				args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm,
				rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)
			return
		}

		reply.VoteGranted = false
		DPrintf("(RequestVote handler false, log outdated) %v(term: %v, log index: %v, log term: %v)->%v(term: %v, log index: %v, log term: %v), ",
			args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm,
			rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("(RequestVote handler false, already voted %v) %v(term: %v)->%v(term: %v)",
				rf.votedFor, args.CandidateId, args.Term, rf.me, rf.currentTerm)
}


type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]interface{}
	LeaderCommit	int
}

type AppendEntriesReply	struct {
	Term		int
	Success		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm <= args.Term {
		rf.haveMessagePeriod = true
		rf.state = FollowerState
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId

		reply.Term = args.Term

		prevLogIndex := args.PrevLogIndex

		if prevLogIndex < len(rf.logs) && 
					(args.PrevLogTerm == rf.logs[prevLogIndex].logTerm || rf.logs[prevLogIndex].logTerm == rf.currentTerm) {

			for i := 0; i < len(args.Entries); i++ {
				if prevLogIndex + 1 + i < len(rf.logs){
					rf.logs[prevLogIndex+1+i].command = args.Entries[i]
					rf.logs[prevLogIndex+1+i].logTerm = args.Term
				} else {
					rf.logs = append(rf.logs, LogType{args.Entries[i], args.Term})
				}
			}

			rf.logs = rf.logs[0:prevLogIndex+len(args.Entries)+1]

			commitBefore := rf.commitIndex 
			rf.commitIndex = prevLogIndex + len(args.Entries)
			if args.LeaderCommit < rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
			}

			for i := commitBefore+1; i <= rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{true, rf.logs[i].command, i}
				DPrintf("\033[37m(applyMsg)role: %v, index: %v\033[0m", rf.me, i)
			}

			reply.Success = true
			if len(args.Entries) == 0{
				DPrintf("\033[35m(AppendEntries handler succeed)heartbeat %v(term:%v) -> %v(commit:%v), len(logs): %v\033[0m", 
					args.LeaderId, args.Term, rf.me, rf.commitIndex, len(rf.logs))
			} else {
				DPrintf("\033[35m(AppendEntries handler succeed len(entries): %v) %v(term:%v) -> %v(commit:%v), len(logs): %v\033[0m", 
					len(args.Entries), args.LeaderId, args.Term, rf.me, rf.commitIndex, len(rf.logs))

			}
			return
		}

		// log inconsistency
		logIndex := rf.commitIndex
		logTerm := rf.logs[logIndex].logTerm
		DPrintf("(AppendEntries handler inconsistency) %v(prevIndex: %v, prevTerm: %v) -> %v(prevIndex: %v, prevTerm: %v), len(entries): %v", 
			args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.me, logIndex, logTerm, len(args.Entries))
		reply.Success = false
	} else {
		// term outdated
		DPrintf("(AppendEntries handler term outdated) %v(term: %v) -> %v(term: %v)", 
			args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
	}
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

func (rf *Raft) sendAppendEntries(prevLogIndex int){
	serverTotal := len(rf.peers)
	successNum := 1
	isCommit := false

	rf.mu.Lock()
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	for i := 0; i < serverTotal; i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			args := AppendEntriesArgs{}
			args.Term = term
			args.LeaderId = rf.me
			args.LeaderCommit = leaderCommit 
			args.Entries = make([]interface{}, 1)

			for {
				rf.mu.Lock()
				args.PrevLogIndex = rf.nextIndex[i]-1
				if args.PrevLogIndex > prevLogIndex {
					rf.mu.Unlock()
					return 
				}
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].logTerm
				args.Entries[0] = rf.logs[args.PrevLogIndex+1].command
				rf.mu.Unlock()

				reply := AppendEntriesReply{}

				ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)

				if !ok {
					// ???
					return
				}

				rf.mu.Lock()
				if reply.Success {
					if args.PrevLogIndex + 2 > rf.nextIndex[i] {
						rf.nextIndex[i] = args.PrevLogIndex + 2
						DPrintf("(sendAppendEntries)not heart beat +2 nextIndex[%v]:%v, obj.PrevLogIndex: %v",
							i, rf.nextIndex[i], prevLogIndex)
					}

					if prevLogIndex == args.PrevLogIndex {
						successNum++

						if !isCommit && successNum > serverTotal / 2 {
							for i := rf.commitIndex + 1; i <= prevLogIndex + 1; i++ {
								rf.applyCh <- ApplyMsg{true, rf.logs[i].command, i}
								DPrintf("\033[37m(applyMsg leader)role: %v, index: %v\033[0m", 
									rf.me, i)
							}
							rf.commitIndex = prevLogIndex + 1
							isCommit = true
						}

						rf.mu.Unlock()
						return 
					}

				}
				if rf.state != LeaderState {
					rf.mu.Unlock()
					return 
				}
				
				if reply.Success == false {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = FollowerState
						DPrintf("(sendAppendEntries fail) %v becomes follower, term: %v, %v->%v", rf.me, rf.currentTerm, rf.me, i)
						rf.mu.Unlock()
						return 
					} else {
						DPrintf("(sendAppendEntries fail) %v->%v preLogIndex: %v, nextIndex: %v", rf.me, i, rf.nextIndex[i]-1, rf.nextIndex[i])
						rf.nextIndex[i] = args.PrevLogIndex
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}

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
	rf.mu.Lock()
	if rf.state != LeaderState {
		isLeader = false
	}
	rf.mu.Unlock()
	if !isLeader {
		return index, term, false
	}

	rf.mu.Lock()
	term = rf.currentTerm
	prevLogIndex := len(rf.logs)-1

	rf.logs = append(rf.logs, LogType{command, term})
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	DPrintf("\033[37m(Start)leaderId: %v, start: %v, commitIndex: %v, command: %v\033[0m",
		rf.me, prevLogIndex + 1, commitIndex, command)

	go rf.sendAppendEntries(prevLogIndex)
	index = prevLogIndex+1

	return index, term, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


const (
	electionTimeoutLeft		int = 500
	electionTimeoutLength	int = 150
	heartbeatsInterval		int = 100
)
func (rf *Raft) waitElectionTimeout() {
	rf.mu.Lock()
	rf.haveMessagePeriod = false
	rf.mu.Unlock()

	electionTimeoutMs := electionTimeoutLeft + rand.Intn(electionTimeoutLength)
	time.Sleep(time.Millisecond * time.Duration(electionTimeoutMs))

	rf.mu.Lock()
	if rf.haveMessagePeriod == false {
		rf.state = CandidateState
	}
	rf.mu.Unlock()
	DPrintf("%v electionTimeout, state: %v, term: %v", rf.me, rf.state, rf.currentTerm)
}

func (rf *Raft) voteForLeader() {
	cond := sync.NewCond(new(sync.Mutex))
	cond.L.Lock()
	defer cond.L.Unlock()

	args := RequestVoteArgs{}

	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentTerm++

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logs)-1
	args.LastLogTerm = rf.logs[rf.commitIndex].logTerm
	rf.mu.Unlock()
	DPrintf("(%v voteForLeader), state: %v, term: %v", rf.me, rf.state, rf.currentTerm)

	getVote := 1
	serverTotal := len(rf.peers)
	for i := 0; i < serverTotal; i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)

			if !ok  {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.VoteGranted {
				getVote++
				if getVote > serverTotal / 2 && rf.state == CandidateState {
					rf.state = LeaderState
					for i := 0; i < serverTotal; i++ {
						rf.nextIndex[i] = rf.commitIndex+1
					}
					cond.Signal()
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FollowerState
				}
			}
		}(i)
	}

	go func() {
		electionTimeoutMs := electionTimeoutLeft + rand.Intn(electionTimeoutLength)
		time.Sleep(time.Millisecond * time.Duration(electionTimeoutMs))
		cond.Signal()
	} ()
	cond.Wait()

	// 这里可以不加锁, 但为了-race不报错, 加锁(因为只是log)
	rf.mu.Lock()
	DPrintf("%v:get %v votes(total %v, votedFor %v), state: %v",
		rf.me, getVote, serverTotal, rf.votedFor, rf.state)
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeats() {
	DPrintf("(sendHeartbeats) leader: %v, term: %v", rf.me, rf.currentTerm)
	rf.mu.Lock()
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	serverTotal := len(rf.peers)
	for i := 0; i < serverTotal; i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()
			curPrevLogIndex := rf.nextIndex[i]-1
			rf.mu.Unlock()

			args := AppendEntriesArgs{}
			args.Term = term
			args.LeaderId = rf.me
			args.LeaderCommit = leaderCommit
			for {
				rf.mu.Lock()
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].logTerm 

				args.Entries = make([]interface{}, 0)

				if curPrevLogIndex < args.PrevLogIndex {
					rf.mu.Unlock()
					return 
				}

				if curPrevLogIndex > args.PrevLogIndex {
					args.Entries = append(args.Entries, rf.logs[args.PrevLogIndex+1].command)
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}

				ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)

				if !ok {
					return 
				}

				rf.mu.Lock()
				if rf.state != LeaderState {
					rf.mu.Unlock()
					return 
				}

				if reply.Success == false {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = FollowerState
						DPrintf("(sendAppendEntries fail) %v becomes follower, term: %v, %v->%v", rf.me, rf.currentTerm, rf.me, i)
						rf.mu.Unlock()
						return 
					} else {
						DPrintf("(sendAppendEntries fail) %v->%v preLogIndex: %v, nextIndex: %v", rf.me, i, rf.nextIndex[i]-1, rf.nextIndex[i])
						rf.nextIndex[i] = args.PrevLogIndex
						rf.mu.Unlock()
						continue
					}
				} else {
					if curPrevLogIndex <= args.PrevLogIndex {
						rf.mu.Unlock()
						return 
					} 

					if args.PrevLogIndex + 2 > rf.nextIndex[i] {
						rf.nextIndex[i] = args.PrevLogIndex + 2
						rf.mu.Unlock()
						continue
					} 

					if rf.nextIndex[i] > curPrevLogIndex + 1 {
						rf.mu.Unlock()
						return 
					}
				} 
				rf.mu.Unlock()
			}

		}(i)
	}	

	time.Sleep(time.Millisecond * time.Duration(heartbeatsInterval))
}

func (rf *Raft) bgRoutine() {
	for {
		if rf.killed() {
			time.Sleep(time.Millisecond * time.Duration(500))
			continue
		}

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch(state) {
			case FollowerState:
				rf.waitElectionTimeout()
			case CandidateState:
				rf.voteForLeader()
			case LeaderState:
				rf.sendHeartbeats()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FollowerState
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.applyCh = applyCh
	rf.logs = make([]LogType, 1)
	rf.logs[0].logTerm = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	go rf.bgRoutine()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
