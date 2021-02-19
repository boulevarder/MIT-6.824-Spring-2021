package raft


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
	if local_lastIncludedIndex >= 0 && local_lastIncludedIndex < len(rf.logs) {
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