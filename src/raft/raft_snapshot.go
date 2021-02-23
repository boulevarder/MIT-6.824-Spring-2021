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


// 更改状态
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm != args.Term || rf.votedFor != args.LeaderId {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.persist()
	}
	rf.state = FollowerState
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	replace_logs := []LogType{}
	replace_logs = append(replace_logs, LogType {
		LogTerm : args.LastIncludedTerm,
	})
	local_lastIncludedIndex := rf.logIndex_global2local(args.LastIncludedIndex)
	if local_lastIncludedIndex < len(rf.logs) && rf.logs[local_lastIncludedIndex].LogTerm == args.LastIncludedTerm {
		for i := local_lastIncludedIndex + 1; i < len(rf.logs); i++ {
			replace_logs = append(replace_logs, rf.logs[i])
		}
	}
	rf.logs = replace_logs
	rf.logIndexBefore = args.LastIncludedIndex
	rf.lastApplied = rf.logIndexBefore
	if rf.commitIndex < rf.lastApplied {
		rf.commitIndex = rf.lastApplied
	}
	rf.SaveStateAndSnapshot(args.Data)
	rf.snapshotAlreadyApply = false

	go func() {
		rf.informApplyCh <- true
	}()
	DPrintf(blueFormat+"(InstallSnapshot handler) role: %v, lastIncludedIndex: %v"+defaultFormat,
		rf.me, args.LastIncludedIndex)
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
	rf.snapshotAlreadyApply = true
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