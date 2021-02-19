package raft


import "bytes"
import "../labgob"
import "log"



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

}