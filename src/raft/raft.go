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

import "bytes"
import "../labgob"


var (
	redFormat		string = "\033[35m"
	whiteFormat		string = "\033[37m"
	blueFormat		string = "\033[1;34m"
	warnFormat		string = "\033[1;33m"
	defaultFormat	string = "\033[0m"
)

const (
	electionTimeoutMin		int = 500
	electionTimeoutLength	int = 150
	heartbeatsInterval		int = 100
)

func getElectionTimeout() int {
	return electionTimeoutMin + rand.Intn(electionTimeoutLength)
}

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

type RaftState	int32 
const (
	FollowerState	RaftState = 0
	CandidateState	RaftState = 1
	LeaderState		RaftState = 2
)

type LogType struct {
	Command		interface{}
	LogTerm		int
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
	state			RaftState
	applyCh			chan ApplyMsg

	currentTerm		int
	votedFor		int
	logs			[]LogType

	commitIndex		int

	nextIndex		[]int
	matchIndex		[]int

	haveMessagePeriod	bool
	newLogConds			[]*sync.Cond
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm	int
	var votedFor	int
	var logs		[]LogType
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) != nil {
		DPrintf(warnFormat+"====================== (readPersist) error ==========================="+defaultFormat)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

type RequestVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	Term			int
	VoteGranted		bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("(RequestVote handler false, term outdated %v(term: %v) -> %v(term: %v)",
			args.CandidateId, args.Term, rf.me, rf.currentTerm)

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term || rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		needPersist := false

		if rf.currentTerm < args.Term {
			rf.state = FollowerState
			rf.currentTerm = args.Term
			rf.votedFor = -1

			needPersist = true
		}

		reply.Term = rf.currentTerm 
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].LogTerm
		// election restriction
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			DPrintf("(RequestVote handler true) %v(term: %v, len(logs): %v, log term: %v) -> %v(term: %v, len(logs): %v, log term: %v)",
				args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)

			rf.haveMessagePeriod = true

			rf.votedFor = args.CandidateId

			reply.VoteGranted = true
			rf.persist()
			return
		}
		
		if needPersist {
			rf.persist()
		}

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("(RequestVote handler false) %v(term: %v, len(logs): %v, log term: %v) -> %v(term: %v, len(logs): %v, log term: %v)",
			args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)
		return 
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false 
	DPrintf("(RequestVote handler false, already vote %v) %v(term %v) -> %v(term %v)",
		rf.votedFor, args.CandidateId, args.Term, rf.me, rf.currentTerm)
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogType
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm <= args.Term {

		needPersist := false

		rf.haveMessagePeriod = true

		rf.state = FollowerState
		if rf.currentTerm != args.Term || rf.votedFor != args.LeaderId {
			needPersist = true
			rf.currentTerm = args.Term
			rf.votedFor = args.LeaderId
		}

		reply.Term = rf.currentTerm

		prevLogIndex := args.PrevLogIndex
		if prevLogIndex < len(rf.logs) && args.PrevLogTerm == rf.logs[prevLogIndex].LogTerm {

			entryConflict := false
			for i := 0; i < len(args.Entries); i++ {
				if prevLogIndex + 1 + i < len(rf.logs) {
					if entryConflict || rf.logs[prevLogIndex+1+i].LogTerm != args.Entries[i].LogTerm {
						rf.logs[prevLogIndex+1+i] = args.Entries[i]
						entryConflict = true
						needPersist = true
					}
				} else {
					rf.logs = append(rf.logs, args.Entries[i])
					needPersist = true
				}
			}
			if entryConflict {
				rf.logs = rf.logs[0:prevLogIndex + len(args.Entries) + 1]
			}


			commitIndex := args.PrevLogIndex + len(args.Entries)

			for {
				if commitIndex + 1 <= len(rf.logs)-1 && rf.logs[commitIndex+1].LogTerm == args.Term {
					commitIndex++
				} else {
					break
				}
			}

			if args.LeaderCommit < commitIndex {
				commitIndex = args.LeaderCommit
			}


			for i := rf.commitIndex+1; i <= commitIndex; i++ {
				rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, i}
				DPrintf(whiteFormat+"(applyMsg) role: %v, index: %v, command: %v"+defaultFormat,
					rf.me, i, rf.logs[i].Command)
			}
			rf.commitIndex = commitIndex

			if needPersist {
				rf.persist()
			}
			
			reply.Success = true
			DPrintf(redFormat+"(AppendEntries handler succeed, len(entries): %v), %v(term: %v) -> %v(commit: %v), len(logs): %v, leaderCommit: %v, args.PrevLogIndex: %v, args.PrevLogTerm: %v"+defaultFormat,
					len(args.Entries), args.LeaderId, args.Term, rf.me, rf.commitIndex, len(rf.logs), args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm)
			return 
		}

		// log inconsistency
		// arr[start:end]: [start, end)
		if prevLogIndex < len(rf.logs) {
			DPrintf(redFormat+"(AppendEntries handler inconsistency) %v(prevIndex: %v, prevTerm: %v) -> %v(prevIndex: %v, prevTerm: %v), len(entries): %v"+defaultFormat,
				args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.me, args.PrevLogIndex, rf.logs[args.PrevLogIndex].LogTerm, len(args.Entries))
			rf.logs = rf.logs[0:prevLogIndex]
			needPersist = true
		} else {
			curPrevLogIndex := len(rf.logs)-1
			curPrevLogTerm := rf.logs[curPrevLogIndex].LogTerm
			DPrintf(redFormat+"(AppendEntries handler inconsistency, len not enough) %v(prevIndex: %v, prevTerm: %v) -> %v(lastIndex: %v, lastTerm: %v), len(entries): %v"+defaultFormat,
				args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.me, curPrevLogIndex, curPrevLogTerm, len(args.Entries))
		}

		if needPersist {
			rf.persist()
		}

		reply.Success = false
	} else {
		// term outdated
		DPrintf(redFormat+"(AppendEntries handler term outdated) %v(term %v) -> %v(term %v)"+defaultFormat,
			args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

func (rf *Raft) informAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.newLogConds[i].L.Lock()
			rf.newLogConds[i].Signal()
			rf.newLogConds[i].L.Unlock()
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
	if rf.state == LeaderState {
		term = rf.currentTerm
		index = len(rf.logs)
		isLeader = true

		rf.logs = append(rf.logs, LogType{command, term})
		rf.persist()

		DPrintf(warnFormat+"(Start) leaderId: %v, start: %v, commitIndex: %v, command: %v"+defaultFormat,
			rf.me, index, rf.commitIndex, command)
	} else {
		isLeader = false		
	}
	rf.mu.Unlock()

	if isLeader{
		go rf.informAppendEntries()
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTimeout() {
	rf.mu.Lock()
	DPrintf("(electionTimeout begin) follower: %v, term: %v", rf.me, rf.currentTerm)
	rf.haveMessagePeriod = false
	rf.mu.Unlock()
	electionTimeoutMs := getElectionTimeout()
	
	time.Sleep(time.Millisecond * time.Duration(electionTimeoutMs))

	rf.mu.Lock()
	if rf.haveMessagePeriod == false {
		rf.state = CandidateState
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		DPrintf("(electionTimeout end) candidate: %v, term: %v", rf.me, rf.currentTerm)
	} else {
		DPrintf("(electionTimeout end) follower: %v, term: %v", rf.me, rf.currentTerm)	
	}
	rf.mu.Unlock()
}

func (rf *Raft) voteForLeader() {
	rf.mu.Lock()
	DPrintf("(voteForLeader begin) Candidate: %v voteForLead, term: %v", rf.me, rf.currentTerm)
	rf.haveMessagePeriod = false
	rf.mu.Unlock()

	cond := sync.NewCond(new(sync.Mutex))
	getVote := 1
	alreadyInform := false
	serverTotal := len(rf.peers)

	go func() {
		electionTimeoutMs := getElectionTimeout()
		
		for i := 0; i < 4; i++ {
			time.Sleep(time.Millisecond * time.Duration(electionTimeoutMs / 4))
			
			rf.mu.Lock()
			if rf.state != CandidateState {
				alreadyInform = true
				rf.mu.Unlock()

				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
				return
			}
			rf.mu.Unlock()
		}
		cond.L.Lock()
		cond.Broadcast()
		cond.L.Unlock()
	}()

	go func(){
		rf.mu.Lock()
		if rf.state != CandidateState {
			rf.mu.Unlock()
			return
		}

		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.logs) - 1
		args.LastLogTerm = rf.logs[args.LastLogIndex].LogTerm
		rf.mu.Unlock()

		for i := 0; i < serverTotal; i++ {
			if i == rf.me {
				continue
			}
			go func(i int){
				reply := RequestVoteReply{}
				ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				if reply.VoteGranted {
					getVote++
					if !alreadyInform && rf.currentTerm == args.Term && 
							getVote > serverTotal / 2 {
						rf.state = LeaderState
						for i := 0; i < serverTotal; i++ {
							rf.nextIndex[i] = len(rf.logs)
							rf.matchIndex[i] = 0
						}
						alreadyInform = true
						rf.mu.Unlock()

						cond.L.Lock()
						cond.Broadcast()
						cond.L.Unlock()
						return 
					}
					rf.mu.Unlock()
					return 
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = FollowerState

						rf.persist()
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}()



	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()

	rf.mu.Lock()
	if rf.state != LeaderState {
		DPrintf("(voteForLeader) %v didn't get enough votes, %v / %v, term: %v, state: %v",
			rf.me, getVote, serverTotal, rf.currentTerm, rf.state)

		if rf.haveMessagePeriod {
			rf.state = FollowerState
		} else {
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.state = CandidateState

			rf.persist()
		}
	} else {
		DPrintf("(voteForLeader) %v gets %v votes(total: %v), becomes leader, term: %v",
			rf.me, getVote, serverTotal, rf.currentTerm)
	}
	alreadyInform = true
	rf.mu.Unlock()
}


func (rf *Raft) computeCommitIndex(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	commitIndexLeft := rf.commitIndex
	for rf.logs[commitIndexLeft].LogTerm != term {
		commitIndexLeft++
		if len(rf.logs) == commitIndexLeft {
			return
		}
	}

	updateCommitIndex := 0
	serverTotal := len(rf.peers)
	for i := commitIndexLeft; i < len(rf.logs); i++ {
		num := 1

		for j := 0; j < serverTotal; j++ {
			if j == rf.me {
				continue
			}

			if rf.matchIndex[j] >= i {
				num++
			}
		}

		if num > serverTotal / 2 {
			updateCommitIndex = i
		}
	}

	if updateCommitIndex > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= updateCommitIndex; i++ {
			rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, i}
			DPrintf(whiteFormat+"(applyMsg leader) role: %v, index: %v, command: %v"+defaultFormat,
				rf.me, i, rf.logs[i].Command)
		} 
		rf.commitIndex = updateCommitIndex
	}
}

func (rf *Raft) solveAppendEntriesReply(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.currentTerm != args.Term {
		return true
	}

	if reply.Success {
		if args.PrevLogIndex + len(args.Entries) > rf.matchIndex[i] {
			rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		
			if len(args.Entries) > 0 && rf.logs[args.PrevLogIndex + 1].LogTerm == rf.currentTerm && 
					rf.commitIndex < rf.matchIndex[i] {
				go rf.computeCommitIndex(args.Term)
			}
		}
		if args.PrevLogIndex + len(args.Entries) + 1 > rf.nextIndex[i] {
			rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
		}

		DPrintf("(solveAppendEntriesReply) %v -> %v, len(args.Entries): %v, args.PrevLogIndex: %v, matchIndex: %v",
			rf.me, i, len(args.Entries), args.PrevLogIndex, rf.matchIndex[i])


		if len(rf.logs) - 1 > rf.matchIndex[i] {
			return true
		}
		return false
	} else {
		if args.Term < reply.Term {
			DPrintf("(solveAppendEntriesReply term outdated) %v(argsTerm: %v, currentTerm: %v) -> %v(term: %v)",
				rf.me, args.Term, rf.currentTerm, i,  reply.Term)

			if rf.currentTerm < reply.Term{
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FollowerState

				rf.persist()
			}
		} else {
			DPrintf("(solveAppendEntriesReply log inconsistency) %v -> %v, args.PrevLogIndex: %v, args.PrevLogTerm: %v",
				rf.me, i, args.PrevLogIndex, args.PrevLogTerm)
			rf.nextIndex[i] = args.PrevLogIndex
			if rf.nextIndex[i] < rf.matchIndex[i] + 1 {
				rf.nextIndex[i] = rf.matchIndex[i] + 1
			} 
		}
		return true
	}
}

type InformComplete struct {

}

func (rf *Raft) loopSendAppendEntries(i int, term int) {

	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != term || rf.state != LeaderState {
			rf.mu.Unlock()
			break
		}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm

		args.Entries = make([]LogType, 0)
		if args.PrevLogIndex + 1 < len(rf.logs) {
			sendLogIndex := rf.nextIndex[i]
			
			sendLogIndexLeft := rf.nextIndex[i]
			for rf.logs[sendLogIndexLeft].LogTerm == rf.logs[sendLogIndex].LogTerm {
				sendLogIndexLeft--

				if sendLogIndexLeft == rf.matchIndex[i] {
					break
				}
			}
			sendLogIndexLeft++

			sendLogIndexRight := rf.nextIndex[i]
			for rf.logs[sendLogIndexRight].LogTerm == rf.logs[sendLogIndex].LogTerm {
				sendLogIndexRight++
				if sendLogIndexRight == len(rf.logs) {
					break
				}					
			}
			sendLogIndexRight--
			for i := sendLogIndexLeft; i <= sendLogIndexRight; i++ {
				args.Entries = append(args.Entries, rf.logs[i])
			}
			args.PrevLogIndex = sendLogIndexLeft-1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm
		}

		args.LeaderCommit = rf.commitIndex
		DPrintf("(loopSendAppendEntries) %v -> %v, args.PrevLogIndex: %v, len(entries): %v",
			rf.me, i, args.PrevLogIndex, len(args.Entries))
		rf.mu.Unlock()

		rpcTimer := time.NewTimer(time.Millisecond * time.Duration(heartbeatsInterval))
		informChannel := make(chan InformComplete)
		go func() {
			reply := AppendEntriesReply{}
			ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
			if ok == false {
				rf.newLogConds[i].L.Lock()
				rf.newLogConds[i].Signal()
				rf.newLogConds[i].L.Unlock()
				return
			}

			continueSend := rf.solveAppendEntriesReply(i, &args, &reply)
			if continueSend {
				informChannel <- InformComplete{}
			}
		}()
		
		select {
		case <- informChannel:
			break
		case <- rpcTimer.C:
			break
		}

		rf.mu.Lock()
		if len(rf.logs) - 1 >= rf.nextIndex[i] {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		rf.newLogConds[i].L.Lock()
		rf.newLogConds[i].Wait()
		rf.newLogConds[i].L.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesToAllPeers() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.loopSendAppendEntries(i, term)
	}

	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		DPrintf("(sendHeartbeats) leader: %v", rf.me)
		go func() {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				go func(i int) {
					args := AppendEntriesArgs{}
					args.LeaderId = rf.me
					args.Entries = make([]LogType, 0)
					rf.mu.Lock()
					if rf.currentTerm != term {
						rf.mu.Unlock()
						return
					}
					args.Term = term
					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm
					args.LeaderCommit = rf.commitIndex
					rf.mu.Unlock()

					reply := AppendEntriesReply{}
					ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
					if !ok {
						return
					}

					rf.solveAppendEntriesReply(i, &args, &reply)
				}(i)

			}
		}()

		time.Sleep(time.Millisecond * time.Duration(heartbeatsInterval))
	}
}

func (rf *Raft) bgRoutine() {	
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch(state) {
			case FollowerState:
				rf.electionTimeout()
			case CandidateState:
				rf.voteForLeader()
			case LeaderState:
				rf.sendAppendEntriesToAllPeers()
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
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogType, 1)
	rf.logs[0].LogTerm = 0

	rf.commitIndex = 0
	
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.newLogConds = make([]*sync.Cond, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.newLogConds[i] = sync.NewCond(new(sync.Mutex))
	}

	DPrintf(warnFormat+"%v starts"+defaultFormat, rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	go rf.bgRoutine()

	return rf
}
