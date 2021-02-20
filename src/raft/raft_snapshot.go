package raft

import (
	"6.824/labgob"
	"bytes"
)

/* discard log entries [1, index)
 * logIndexBefore = index - 1
 * local_index: 日志在logs的索引; global_index: 日志在包含snapshot下的索引
 * logs[local_index]: local_index = 0 ==> global_index = logsIndexBefore + local_index
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
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm != args.Term || rf.votedFor != args.LeaderId {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.state = FollowerState

		rf.persist()
	}
	rf.mu.Unlock()
	DPrintf(redLightFormat+"(InstallSnapshot handler) %v -> %v, lastIncludedIndex: %v, lastIncludedTerm: %v"+defaultFormat,
		args.LeaderId, rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.applyCh <- ApplyMsg {
		CommandValid	: false,
		SnapshotValid	: true,
		Snapshot		: args.Data,
		SnapshotTerm	: args.LastIncludedTerm,
		SnapshotIndex	: args.LastIncludedIndex,
	}
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) SaveStateAndSnapshot(snapshot []byte) {
	w_persist := new(bytes.Buffer)
	e_persist := labgob.NewEncoder(w_persist)

	e_persist.Encode(rf.currentTerm)
	e_persist.Encode(rf.votedFor)
	e_persist.Encode(rf.logs)
	e_persist.Encode(rf.logIndexBefore)

	data_persist := w_persist.Bytes()
	rf.persister.SaveStateAndSnapshot(data_persist, snapshot)
} 

func (rf *Raft) readSnapshot() {
	data_snapshot := rf.persister.ReadSnapshot()
	if data_snapshot == nil || len(data_snapshot) < 1 {
		return
	}

	rf.applyCh <- ApplyMsg {
		CommandValid	: false,
		SnapshotValid	: true,
		Snapshot		: data_snapshot,
		SnapshotTerm	: rf.logs[0].LogTerm,
		SnapshotIndex	: rf.logIndexBefore,
	}
}