package raft


import "bytes"
import "../labgob"
import "log"


/* discard log entries [1, index)
 * logIndexBefore = index - 1
 * local_index: 日志在logs的索引; global_index: 日志在包含snapshot下的索引
 * logs[local_index]: local_index = 0 ==> global_index = logsIndexBegin + local_index
 */

// 安装snapshot的index必须小于lastApplied

func (rf *Raft) logIndex_global2local(global_index int) int {
	return global_index - rf.logIndexBefore
}

func (rf *Raft) logIndex_local2global(local_index int) int {
	return local_index + rf.logIndexBefore
}


func (rf *Raft) initial_logIndexBefore() {
	rf.logIndexBefore = 0
}

// snapshot: [0, end_index]
func (rf *Raft) Snapshot(end_index int, maxraftstate int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if end_index < rf.logIndexBefore {
		return
	}
	if end_index < rf.lastApplied {
		return
	}

	if maxraftstate > rf.persister.RaftStateSize() {
		return
	}

	snapshotLogs := []LogType{}
	rf.ReadSnapshot(rf.persister.ReadSnapshot(), &snapshotLogs)
	local_endIndex := rf.logIndex_global2local(end_index)
	for i := 1; i <= local_endIndex; i++ {
		snapshotLogs = append(snapshotLogs, rf.logs[i])
	}

	logs := []LogType{}
	logs = append(logs, LogType{
		LogTerm	: rf.logs[local_endIndex].LogTerm,
	})

	for i := local_endIndex + 1; i < len(rf.logs); i++ {
		logs = append(logs, rf.logs[i])
	}
	rf.logs = logs
	rf.logIndexBefore = end_index

	w_snapshot := new(bytes.Buffer)
	e_snapshot := labgob.NewEncoder(w_snapshot)
	e_snapshot.Encode(snapshotLogs)
	data_snapshot := w_snapshot.Bytes()
	rf.saveStateAndSnapshot(data_snapshot)
}


func (rf *Raft) saveStateAndSnapshot(data_snapshot []byte) {
	w_persist := new(bytes.Buffer)
	e_persist := labgob.NewEncoder(w_persist)

	e_persist.Encode(rf.currentTerm)
	e_persist.Encode(rf.votedFor)
	e_persist.Encode(rf.logs)

	data_persist := w_persist.Bytes()

	rf.persister.SaveStateAndSnapshot(data_persist, data_snapshot)
}


func (rf *Raft) ReadSnapshot(data []byte, storelogs *[]LogType) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var logs	[]LogType
	if d.Decode(&logs) != nil {
		log.Fatalf(warnFormat+"=========================== (readSnapshot) error =============================")
	} else {
		*storelogs = logs
	}
}


type InstallSnapshotArgs struct {
	Term				int
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Data				[]byte
}

type InstallSnapshotReply struct {
	Term	int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	logs := []LogType{}
	logs = append(logs, LogType {
		LogTerm	: args.LastIncludedTerm,
	})

	local_lastIncludedIndex := rf.logIndex_global2local(args.LastIncludedIndex)
	if local_lastIncludedIndex >= 0 {
		if rf.logs[local_lastIncludedIndex].LogTerm == args.LastIncludedTerm {
			for i := local_lastIncludedIndex + 1; i < len(rf.logs); i++ {
				logs = append(logs, rf.logs[i])
			}
		}
	}
	rf.logs = logs
	rf.logIndexBefore = args.LastIncludedIndex

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex

		rf.applyCond.L.Lock()
		rf.applyCond.Broadcast()
		rf.applyCond.L.Unlock()
	}

	if rf.currentTerm != args.Term || rf.votedFor != args.LeaderId {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId

		rf.persist()
	}
	reply.Term = rf.currentTerm
}