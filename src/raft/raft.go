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
	electionTimeoutLeft		int = 500
	electionTimeoutLength	int = 150
	heartbeatsInterval		int = 100
	waitSendAppendEntries 	int = 70
	deadExamWaitInternal	int = 500
)

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

type RaftState	int 
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
	haveMessagePeriod		bool
	state			RaftState
	applyCh			chan ApplyMsg

	currentTerm		int 
	votedFor		int
	logs			[]LogType

	commitIndex		int

	nextIndex		[]int
	matchIndex		[]int
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
		}
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId

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
					needPersist == true
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
			DPrintf(redFormat+"(AppendEntries handler succeed, len(entries): %v), %v(term: %v) -> %v(commit: %v), len(logs): %v, leaderCommit: %v"+defaultFormat,
					len(args.Entries), args.LeaderId, args.Term, rf.me, rf.commitIndex, len(rf.logs), args.LeaderCommit)
			return 
		}

		// log inconsistency
		// arr[start:end]: [start, end)
		if prevLogIndex <= len(rf.logs) {
			rf.logs = rf.logs[0:prevLogIndex]
		}
		curPrevLogIndex := args.PrevLogIndex
		if len(rf.logs)-1 < curPrevLogIndex{
			curPrevLogIndex = len(rf.logs) - 1
		}
		curPrevLogTerm := rf.logs[curPrevLogIndex].LogTerm
		DPrintf(redFormat+"(AppendEntries handler inconsistency) %v(prevIndex: %v, prevTerm: %v) -> %v(prevIndex: %v, prevTerm: %v), len(entries): %v"+defaultFormat,
			args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.me, curPrevLogIndex, curPrevLogTerm, len(args.Entries))
		reply.Success = false
	} else {
		// term outdated
		DPrintf(redFormat+"(AppendEntries handler term outdated) %v(term %v) -> %v(term %v)"+defaultFormat,
			args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}


func (rf *Raft) sendAppendEntries(prevLogIndex int, term int) {
	serverTotal := len(rf.peers)
	successNum := 1
	isCommit := false

	for i := 0; i < serverTotal; i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			args := AppendEntriesArgs{}
			args.Term = term
			args.LeaderId = rf.me
			args.PrevLogIndex = prevLogIndex

			rf.mu.Lock()
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return 
			}
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm
			args.Entries = make([]LogType, 1)
			args.Entries[0] = rf.logs[args.PrevLogIndex+1]
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()

			for {
				rf.mu.Lock()
				DPrintf(redFormat+"(sendAppendEntries, index: %v, len(entries): %v) %v -> %v, leaderCommit: %v, matchIndex: %v"+defaultFormat,
					args.PrevLogIndex+1, len(args.Entries), rf.me, i, args.LeaderCommit, rf.matchIndex[i])
				rf.mu.Unlock()
				
				reply := AppendEntriesReply{}
				ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				
				if !ok {
					time.Sleep(time.Millisecond * time.Duration(waitSendAppendEntries))
					
					rf.mu.Lock()
					if rf.currentTerm > args.Term || rf.killed() {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					continue
				}

				rf.mu.Lock()
				if rf.currentTerm != args.Term {
					DPrintf(blueFormat+"(sendAppendEntries, already outdated) %v(term: %v -> %v) -> %v"+defaultFormat,
								rf.me, args.Term, rf.currentTerm, i)
					rf.mu.Unlock()
					return 
				}

				if reply.Success {

					DPrintf(blueFormat+"(sendAppendEntries, index: %v, len(entries): %v) %v -> %v, success"+defaultFormat,
								args.PrevLogIndex+1, len(args.Entries), rf.me, i)
								
					if args.PrevLogIndex + len(args.Entries) + 1 > rf.nextIndex[i] {
						rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
					}

					if args.PrevLogIndex + len(args.Entries) > rf.matchIndex[i] {
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					}

					if args.PrevLogIndex + len(args.Entries) >= prevLogIndex + 1 || rf.matchIndex[i] >= prevLogIndex + 1 {
						successNum++ 
						if !isCommit && successNum > serverTotal / 2 {
							if prevLogIndex + 1 > rf.commitIndex {
								for i := rf.commitIndex + 1; i <= prevLogIndex + 1; i++ {
									DPrintf(whiteFormat+"(applyMsg leader) role: %v, index: %v, command: %v, args.PrevLogIndex: %v, prevLogIndex: %v, len(args.Entries): %v"+defaultFormat,
										rf.me, i, rf.logs[i].Command, args.PrevLogIndex, prevLogIndex, len(args.Entries))
									rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, i}
								}

								rf.commitIndex = prevLogIndex + 1
								DPrintf(whiteFormat+"(sendAppendEntries) leader: %v, rf.commitIndex: %v"+defaultFormat,
									rf.me, rf.commitIndex)
							}
							isCommit = true
						}
						
						rf.mu.Unlock()
						return 
					} else {
						args.PrevLogIndex = args.PrevLogIndex + len(args.Entries)
						if args.PrevLogIndex < rf.matchIndex[i] {
							args.PrevLogIndex = rf.matchIndex[i]
						}
						args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm

						curLogIndexLeft := args.PrevLogIndex + 1
						args.Entries = make([]LogType, 0)
						for i := curLogIndexLeft; i <= prevLogIndex + 1; i++ {
							args.Entries = append(args.Entries, rf.logs[i])
						}

						args.LeaderCommit = rf.commitIndex
						rf.mu.Unlock()
						continue
					}
				} else {
					if reply.Term > args.Term {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = FollowerState 
							
							rf.persist()
						}
						DPrintf("(sendAppendEntries fail) %v becomes follower, %v(term: %v) -> %v(term: %v)", 
								rf.me, rf.me, args.Term, i, reply.Term)
						rf.mu.Unlock()
						return 
					} else {
						// log inconsistency

						if prevLogIndex + 1 <= rf.matchIndex[i] {
							successNum++ 
							if !isCommit && successNum > serverTotal / 2 {
								if  prevLogIndex + 1 > rf.commitIndex {
									for i := rf.commitIndex + 1; i <= prevLogIndex + 1; i++ {
										DPrintf(whiteFormat+"(applyMsg leader) role: %v, index: %v, command: %v, args.PrevLogIndex: %v, prevLogIndex: %v, len(args.Entries): %v"+defaultFormat,
											rf.me, i, rf.logs[i].Command, args.PrevLogIndex, prevLogIndex, len(args.Entries))
										rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, i}
									}

									rf.commitIndex = prevLogIndex + 1
									DPrintf(whiteFormat+"(sendAppendEntries) leader: %v, rf.commitIndex: %v"+defaultFormat,
										rf.me, rf.commitIndex)
								}
								isCommit = true
							}
							rf.mu.Unlock()
							return 
						}

						sendIndexLeft := args.PrevLogIndex
						sendIndexRight := sendIndexLeft
						sendLogTerm := rf.logs[sendIndexRight].LogTerm

						for {
							if sendIndexLeft <= rf.matchIndex[i] || rf.logs[sendIndexLeft].LogTerm != sendLogTerm {
								break
							}
							sendIndexLeft--
						}
						sendIndexLeft++

						for {
							if sendIndexRight > prevLogIndex + 1 || rf.logs[sendIndexRight].LogTerm != sendLogTerm {
								break
							}
							sendIndexRight++
						}
						sendIndexRight--

						args.Entries = make([]LogType, 0)
						for i := sendIndexLeft; i <= sendIndexRight; i++ {
							args.Entries = append(args.Entries, rf.logs[i])
						}
						args.PrevLogIndex = sendIndexLeft - 1
						args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm
						args.LeaderCommit = rf.commitIndex
						rf.mu.Unlock()
						continue
					}
				}
				
			}
		} (i)
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
	prevLogIndex := len(rf.logs) - 1

	rf.logs = append(rf.logs, LogType{command, term})
	rf.persist()

	commitIndex := rf.commitIndex
	DPrintf(warnFormat+"(Start) leaderId: %v, start: %v, commitIndex: %v, command: %v"+defaultFormat,
			rf.me, prevLogIndex+1, commitIndex, command)
	rf.mu.Unlock()

	go rf.sendAppendEntries(prevLogIndex, term)
	index = prevLogIndex + 1

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
	DPrintf("%v electionTimeout, state: %v, term: %v", rf.me, rf.state, rf.currentTerm)
	rf.mu.Unlock()
}

func (rf *Raft) voteForLeader() {
	cond := sync.NewCond(new(sync.Mutex))
	
	rf.mu.Lock()
	rf.currentTerm++
	voteTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.persist()

	DPrintf("(%v voteForLeader), state: %v, term: %v", rf.me, rf.state, rf.currentTerm)
	rf.mu.Unlock()

	go func() {
		electionTimeoutMs := electionTimeoutLeft + rand.Intn(electionTimeoutLength) / 2

		for i := 0; i < 5; i++ {
			DPrintf("(voteForLeader) %v sleep %v", rf.me, electionTimeoutMs)
			time.Sleep(time.Millisecond * time.Duration(electionTimeoutMs / 5))
			DPrintf("(voteForLeader) %v wakeup(sleep %v)", rf.me, electionTimeoutMs)

			rf.mu.Lock()
			if (voteTerm == rf.currentTerm && rf.state == LeaderState) || i == 4 {
				rf.mu.Unlock()

				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
				return 
			}
			rf.mu.Unlock()
		}
	}()

	getVote := 1
	serverTotal := len(rf.peers)
	
	go func() {
		for i := 0; i < serverTotal; i++ {
			if i == rf.me {
				continue
			}

			go func(i int) {
				args := RequestVoteArgs{}
				rf.mu.Lock()
				args.Term = rf.currentTerm
				if voteTerm != rf.currentTerm {
					rf.mu.Unlock()
					return 
				}
				args.CandidateId = rf.me
				args.LastLogIndex = len(rf.logs) - 1
				args.LastLogTerm = rf.logs[args.LastLogIndex].LogTerm
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)

				if !ok {
					return 
				}

				rf.mu.Lock()
				if reply.VoteGranted {
					getVote++
					if rf.currentTerm == args.Term && getVote > serverTotal / 2 { 
						if rf.state == CandidateState {
							rf.state = LeaderState
							for i := 0; i < serverTotal; i++ {
								rf.nextIndex[i] = len(rf.logs)
								rf.matchIndex[i] = 0
							}
							rf.mu.Unlock()

							cond.L.Lock()
							cond.Broadcast()
							cond.L.Unlock()
							return 
						} else {
							rf.mu.Unlock()

							cond.L.Lock()
							cond.Broadcast()
							cond.L.Unlock()
							return
						}
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


	rf.mu.Lock()
	if rf.currentTerm == voteTerm && rf.state == LeaderState {
		DPrintf(warnFormat+"wow! %v before sleep is leader"+defaultFormat, rf.me)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	DPrintf("(voteForLeader) %v wait", rf.me)
	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()

	rf.mu.Lock()
	DPrintf("%v: get %v votes(total %v, votedFor %v), state: %v, term: %v",
		rf.me, getVote, serverTotal, rf.votedFor, rf.state, rf.currentTerm)
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeats() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			args := AppendEntriesArgs{}
			rf.mu.Lock()
			if rf.state != LeaderState {
				rf.mu.Unlock()

				return
			}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i]-1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].LogTerm
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			DPrintf("(sendHeartbeats) %v -> %v", rf.me, i)
			reply := AppendEntriesReply{}
			ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Success {
				if rf.currentTerm == args.Term {
					if args.PrevLogIndex > rf.matchIndex[i] {
						rf.matchIndex[i] = args.PrevLogIndex
					}

					if args.PrevLogIndex + 1 > rf.nextIndex[i] {
						rf.nextIndex[i] = args.PrevLogIndex + 1
					}
				}
			} else {
				if rf.currentTerm < args.Term {
					rf.currentTerm = args.Term
					rf.votedFor = -1
					rf.state = FollowerState		
					
					rf.persist()
				}
			}
		}(i)
	}

	time.Sleep(time.Millisecond * time.Duration(heartbeatsInterval))
}


func (rf *Raft) bgRoutine() {
	for {
		if rf.killed() {
			time.Sleep(time.Millisecond + time.Duration(deadExamWaitInternal))
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().Unix())

	rf.state = FollowerState
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.applyCh = applyCh
	rf.logs = make([]LogType, 1)
	rf.logs[0].LogTerm = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.bgRoutine()



	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf(warnFormat+"role: %v starts, len(logs): %v"+defaultFormat, rf.me, len(rf.logs))
	rf.mu.Unlock()
	return rf
}
