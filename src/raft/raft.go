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
import "log"


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
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int
	CommandTerm	 	int	
	Snapshot		[]byte
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
	lastApplied		int

	nextIndex		[]int
	matchIndex		[]int

	haveMessagePeriod	bool
	newLogConds			[]*sync.Cond
	applyCond			sync.Cond

	logIndexBefore	int 
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
		log.Fatalf(warnFormat+"====================== (readPersist) error ==========================="+defaultFormat)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		if len(rf.logs) == 1 {
			rf.logs = logs
		} else {
			for i := 1; i < len(logs); i++ {
				rf.logs = append(rf.logs, logs[i])
			}
		}
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
		DPrintf("(RequestVote handler false, term outdated) %v(term: %v) -> %v(term: %v)",
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
		local_args_lastLogIndex := rf.logIndex_global2local(args.LastLogIndex)
		local_lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[local_lastLogIndex].LogTerm
		// election restriction
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && local_lastLogIndex <= local_args_lastLogIndex) {
			DPrintf("(RequestVote handler true) %v(term: %v, LastLogIndex: %v, LastLogTerm: %v) -> %v(LastLogIndex: %v, LastLogTerm: %v)",
				args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, 
				rf.logIndex_local2global(local_lastLogIndex), lastLogTerm)

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
		DPrintf("(RequestVote handler false, log not new) %v(term: %v, LastLogIndex: %v, LastLogTerm: %v) -> %v(LastLogIndex: %v, LastLogTerm: %v",
			args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, 
			rf.logIndex_local2global(local_lastLogIndex), lastLogTerm)
		return
	}

	DPrintf("(RequestVote handler false, already vote: %v) %v(term: %v) -> %v(term: %v)",
		rf.votedFor, args.CandidateId, args.Term, rf.me, rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
		
		local_args_prevLogIndex := rf.logIndex_global2local(args.PrevLogIndex)
		if local_args_prevLogIndex < len(rf.logs) && args.PrevLogTerm == rf.logs[local_args_prevLogIndex].LogTerm {

			entryConflict := false
			for i := 0; i < len(args.Entries); i++ {
				if local_args_prevLogIndex + 1 + i < len(rf.logs) {
					if entryConflict || rf.logs[local_args_prevLogIndex + 1 + i].LogTerm != args.Entries[i].LogTerm {
						rf.logs[local_args_prevLogIndex+1+i] = args.Entries[i]
						entryConflict = true
						needPersist = true
					}
				} else {
					rf.logs = append(rf.logs, args.Entries[i])
					needPersist = true
				}
			}
			if entryConflict {
				// arr[start:end]: [start, end)
				rf.logs = rf.logs[0: local_args_prevLogIndex+len(args.Entries)+1]
			}
			
			local_commitIndex := local_args_prevLogIndex + len(args.Entries)
			for {
				if local_commitIndex + 1 <= len(rf.logs) - 1 && rf.logs[local_commitIndex + 1].LogTerm == args.Term {
					local_commitIndex++
				} else {
					break
				}
			}

			global_commitIndex := rf.logIndex_local2global(local_commitIndex)
			if args.LeaderCommit < global_commitIndex {
				global_commitIndex = args.LeaderCommit
			}

			if rf.commitIndex < global_commitIndex {
				rf.commitIndex = global_commitIndex

				rf.applyCond.L.Lock()
				rf.applyCond.Broadcast()
				rf.applyCond.L.Unlock()
			}

			if needPersist {
				rf.persist()
			}

			reply.Success = true
			DPrintf(redFormat+"(AppendEntries handler true, len(entries): %v), %v(term: %v) -> "+
				"%v(commit: %v, lastLogIndex: %v), leaderCommit: %v, args.PrevLogIndex: %v, args.PrevLogTerm: %v"+defaultFormat,
				len(args.Entries), args.LeaderId, args.Term, rf.me, rf.commitIndex, 
				rf.logIndex_local2global(len(rf.logs)-1), args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm)
			return
		}

		// log inconsistency
		if local_args_prevLogIndex < len(rf.logs) {
			DPrintf(redFormat+"(AppendEntries handler log inconsistency) %v(prevIndex: %v, prevTerm: %v) -> "+
				"%v(prevIndex: %v, prevTerm: %v), len(entries): %v"+defaultFormat,
				args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.me, args.PrevLogIndex, 
				rf.logs[local_args_prevLogIndex].LogTerm, len(args.Entries))
			rf.logs = rf.logs[0: local_args_prevLogIndex]
			needPersist = true
		} else {
			global_lastLogIndex := rf.logIndex_local2global(len(rf.logs) - 1)
			lastLogTerm := rf.logs[len(rf.logs)-1].LogTerm
			DPrintf(redFormat+"(AppendEntries handler log inconsistency, len not enough) %v(prevIndex: %v, prevTerm: %v) -> "+
				"%v(lastIndex: %v, lastTerm: %v), len(entries): %v"+defaultFormat,
				args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.me, global_lastLogIndex, lastLogTerm, len(args.Entries))
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
			rf.newLogConds[i].Broadcast()
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
		index = rf.logIndex_local2global(len(rf.logs))
		isLeader = true

		rf.logs = append(rf.logs, LogType{command, term})
		rf.persist()

		DPrintf(warnFormat+"(Start) leaderId: %v, start: %v, commitIndex: %v, command: %v"+defaultFormat,
			rf.me, index, rf.commitIndex, command)
	} else {
		isLeader = false
	}
	rf.mu.Unlock()

	if isLeader {
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
	DPrintf("(voteForLeader begin) Candidate: %v voteForLeader, term: %v", rf.me, rf.currentTerm)
	rf.haveMessagePeriod = false
	voteTerm := rf.currentTerm
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

	go func() {
		rf.mu.Lock()
		if rf.currentTerm != voteTerm || rf.state != CandidateState {
			rf.mu.Unlock()
			return
		}

		args := RequestVoteArgs{}
		args.Term = voteTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.logIndex_local2global(len(rf.logs) - 1)
		args.LastLogTerm = rf.logs[len(rf.logs)-1].LogTerm
		rf.mu.Unlock()

		for i := 0; i < serverTotal; i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				reply := RequestVoteReply{}
				ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				if reply.VoteGranted {
					getVote++
					if !alreadyInform && rf.currentTerm == voteTerm &&
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
	// 只能commit当前term的日志, 所以传进来term, 防止在两次加锁之间term转换了
	rf.mu.Lock()
	defer rf.mu.Unlock()

	local_commitIndexLeft := rf.logIndex_global2local(rf.commitIndex)
	for rf.logs[local_commitIndexLeft].LogTerm != term {
		local_commitIndexLeft++
		if len(rf.logs) == local_commitIndexLeft {
			return
		}
	}

	local_updateCommitIndex := 0
	serverTotal := len(rf.peers)
	for i := local_commitIndexLeft; i < len(rf.logs); i++ {
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
			local_updateCommitIndex = i
		}
	}

	global_updateCommitIndex := rf.logIndex_local2global(local_updateCommitIndex)
	if global_updateCommitIndex > rf.commitIndex {
		rf.commitIndex = global_updateCommitIndex

		rf.applyCond.L.Lock()
		rf.applyCond.Broadcast()
		rf.applyCond.L.Unlock()
	}
}

func (rf *Raft) solveAppendEntriesReply(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term {
		return false
	}

	if reply.Success {
		if len(args.Entries) == 0 && rf.logIndex_local2global(len(rf.logs)-2) >= rf.matchIndex[i] {
			rf.newLogConds[i].L.Lock()
			rf.newLogConds[i].Broadcast()
			rf.newLogConds[i].L.Unlock()
		}

		if args.PrevLogIndex + len(args.Entries) > rf.matchIndex[i] {
			rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)

			if rf.matchIndex[i] > rf.commitIndex && len(args.Entries) > 0 &&
					rf.logs[rf.logIndex_global2local(rf.matchIndex[i])].LogTerm == rf.currentTerm {
				go rf.computeCommitIndex(args.Term)
			}
		}

		if args.PrevLogIndex + len(args.Entries) + 1 > rf.nextIndex[i] {
			rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
		}

		DPrintf("(solveAppendEntriesReply) %v -> %v, len(args.Entries): %v, args.PrevLogIndex: %v, matchIndex: %v",
			rf.me, i, len(args.Entries), args.PrevLogIndex, rf.matchIndex[i])

		return true
	} else {
		if args.Term < reply.Term {
			DPrintf("(solveAppendEntriesReply term outdated) %v(argsTerm: %v, currentTerm: %v) -> %v(term: %v)",
				rf.me, args.Term, rf.currentTerm, i, reply.Term)

			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FollowerState

				rf.persist()
			}
		} else {
			rf.nextIndex[i] = args.PrevLogIndex
			if rf.nextIndex[i] < rf.matchIndex[i] + 1 {
				rf.nextIndex[i] = rf.matchIndex[i] + 1
			}
			DPrintf("(solveAppendEntriesReply log inconsistency) %v -> %v, args.PrevLogIndex: %v, args.PrevLogTerm: %v"+
					"nextIndex: %v, matchIndex: %v", rf.me, i, args.PrevLogIndex, args.PrevLogTerm, 
					rf.nextIndex[i], rf.matchIndex[i])
		}
		return false
	}
}

func (rf *Raft) solveInstallSnapshotReply(i int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term {
		return false
	}

	if reply.Term == args.Term {
		if args.LastIncludedIndex > rf.matchIndex[i] {
			rf.matchIndex[i] = args.LastIncludedIndex
		}

		if args.LastIncludedIndex + 1 > rf.nextIndex[i] {
			rf.nextIndex[i] = args.LastIncludedIndex + 1
		}

		DPrintf("(solveInstallSnapshotReply) %v -> %v, LastIncludedIndex: %v, LastIncludedTerm: %v",
			rf.me, i, args.LastIncludedIndex, args.LastIncludedTerm)
		return true
	} else {
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FollowerState

			rf.persist()
		}
		return false
	}
}

func (rf *Raft) loopSendAppendEntries(i int, term int) {
	beforeSendSuccess := false
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != term || rf.state != LeaderState{
			rf.mu.Unlock()
			return
		}

		local_prevLogIndex := rf.logIndex_global2local(rf.nextIndex[i] - 1)
		if local_prevLogIndex < 0 {
			// send InstallSnapshot RPC
			args := InstallSnapshotArgs{
				Term				: rf.currentTerm,
				LeaderId			: rf.me,
				LastIncludedIndex	: rf.logIndexBefore,
				LastIncludedTerm	: rf.logs[0].LogTerm,
				Data				: rf.persister.ReadSnapshot(),
			}
			DPrintf("(loopSendSnapshot) %v -> %v, LastIncludedIndex: %v, LastIncludedTerm: %v",
					rf.me, i, args.LastIncludedIndex, args.LastIncludedTerm)
			rf.mu.Unlock()

			rpcTimer := time.NewTimer(time.Millisecond * time.Duration(heartbeatsInterval))
			informChannel := make(chan bool)
			go func() {
				reply := InstallSnapshotReply{}
				ok := rf.peers[i].Call("Raft.InstallSnapshot", &args, &reply)
				if ok == false {
					return
				}

				success := rf.solveInstallSnapshotReply(i, &args, &reply)
				informChannel <- success
			}()
			
			select {
			case beforeSendSuccess =<- informChannel:
			case <- rpcTimer.C:
				beforeSendSuccess = false
			}
		} else {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.logs[local_prevLogIndex].LogTerm

			local_matchIndex := rf.logIndex_global2local(rf.matchIndex[i])
			if local_prevLogIndex + 1 < len(rf.logs) {
				local_sendLogIndex := local_prevLogIndex + 1

				local_sendLogIndexLeft := local_sendLogIndex
				for rf.logs[local_sendLogIndexLeft].LogTerm == rf.logs[local_sendLogIndex].LogTerm {
					local_sendLogIndexLeft--

					if local_sendLogIndexLeft == local_matchIndex {
						break
					}
				}
				local_sendLogIndexLeft++

				if beforeSendSuccess {
					for i := local_sendLogIndexLeft; i < len(rf.logs); i++ {
						args.Entries = append(args.Entries, rf.logs[i])
					}
				} else {
					args.Entries = append(args.Entries, rf.logs[local_sendLogIndexLeft])
				}
				args.PrevLogIndex = rf.logIndex_local2global(local_sendLogIndexLeft - 1)
				args.PrevLogTerm = rf.logs[local_sendLogIndexLeft - 1].LogTerm

				rf.nextIndex[i] = args.PrevLogIndex
				if rf.nextIndex[i] <= rf.matchIndex[i] {
					rf.nextIndex[i] = rf.matchIndex[i] + 1
				}
			}

			args.LeaderCommit = rf.commitIndex
			DPrintf("(loopSendAppendEntries) %v -> %v, args.PrevLogIndex: %v, len(entries): %v",
				rf.me, i, args.PrevLogIndex, len(args.Entries))
			rf.mu.Unlock()

			rpcTimer := time.NewTimer(time.Millisecond * time.Duration(heartbeatsInterval))
			informChannel := make(chan bool)
			go func() {
				reply := AppendEntriesReply{}
				ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				if ok == false {
					return
				}

				success := rf.solveAppendEntriesReply(i, &args, &reply)
				informChannel <- success
			}()

			select {
			case beforeSendSuccess =<- informChannel:
			case <- rpcTimer.C:
				beforeSendSuccess = false
			}
		}

		rf.mu.Lock()
		if rf.logIndex_local2global(len(rf.logs) - 2) >= rf.matchIndex[i] || rf.currentTerm != term {
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
			return
		}
		rf.mu.Unlock()
		DPrintf("(sendHeartbeats) leader: %v, term: %v", rf.me, term)
		go func() {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				go func(i int) {
					args := AppendEntriesArgs{}
					args.LeaderId = rf.me
					rf.mu.Lock()
					if rf.currentTerm != term {
						rf.mu.Unlock()
						return
					}
					args.Term = term
					args.PrevLogIndex = rf.nextIndex[i] - 1
					local_args_prevLogIndex := rf.logIndex_global2local(args.PrevLogIndex)
					args.PrevLogTerm = rf.logs[local_args_prevLogIndex].LogTerm
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


func (rf *Raft) applyMsgRoutine() {
	for !rf.killed() {
		var applySnapshot 	bool
		var snapshot 		[]byte 
		var logs 			[]LogType

		rf.mu.Lock()

		local_lastApplied := rf.logIndex_global2local(rf.lastApplied)
		global_beginApplied := rf.lastApplied + 1
		lastIncludeIndex := rf.logIndexBefore
		if local_lastApplied < rf.logIndexBefore {
			applySnapshot = true
			snapshot = make([]byte, rf.persister.SnapshotSize())
			copy(snapshot, rf.persister.ReadSnapshot())
			rf.lastApplied = rf.logIndexBefore
		} else {
			applySnapshot = false
			local_commitIndex := rf.logIndex_global2local(rf.commitIndex)
			for i := local_lastApplied + 1; i <= local_commitIndex; i++ {
				logs = append(logs, rf.logs[i])
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

		if applySnapshot {
			rf.applyCh <- ApplyMsg {
				CommandValid	: false,
				Snapshot		: snapshot,
			}
			if _, isLeader := rf.GetState(); isLeader {
				DPrintf(whiteFormat+"(applySnapshot leader) role: %v, lastIncludeIndex: %v"+defaultFormat,
					rf.me, lastIncludeIndex)
			} else {
				DPrintf(whiteFormat+"(applySnapshot) role: %v, lastIncludeIndex: %v"+defaultFormat,
					rf.me, lastIncludeIndex)
			}
		} else {
			for index, log := range logs {
				rf.applyCh <- ApplyMsg {
					CommandValid	: true,
					Command			: log.Command,
					CommandIndex	: global_beginApplied + index,
					CommandTerm		: log.LogTerm,
				}
				if _, isLeader := rf.GetState(); isLeader {
					DPrintf(whiteFormat+"(applyMsg leader) role: %v, index: %v, command: %v"+defaultFormat,
						rf.me, global_beginApplied + index, log.Command)
				} else {
					DPrintf(whiteFormat+"(applyMsg) role: %v, index: %v, command: %v"+defaultFormat,
						rf.me, global_beginApplied + index, log.Command)
				}
			}
		}

		rf.mu.Lock()
		if rf.lastApplied != rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		rf.applyCond.L.Lock()
		rf.applyCond.Wait()
		rf.applyCond.L.Unlock()
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
	rf.lastApplied = 0
	
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.newLogConds = make([]*sync.Cond, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.newLogConds[i] = sync.NewCond(new(sync.Mutex))
	}

	rf.applyCond.L = new(sync.Mutex)

	rf.initial_logIndexBefore()

	DPrintf(warnFormat+"%v starts"+defaultFormat, rf.me)

	// initialize from state persisted before a crash
	rf.readSnapshot(rf.persister.ReadSnapshot(), &rf.logs)
	rf.readPersist(persister.ReadRaftState())
	
	go rf.bgRoutine()
	
	go rf.applyMsgRoutine()

	return rf
}