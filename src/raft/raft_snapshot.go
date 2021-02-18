package raft


import "bytes"
import "../labgob"




/* discard log entries [1, index)
 * logIndexBefore = index - 1
 * local_index: 日志在logs的索引; global_index: 日志在包含snapshot下的索引
 * logs[local_index]: local_index = 0 ==> global_index = logsIndexBegin + local_index
 */


func (rf *Raft) logIndex_global2local(global_index int) int {
	return global_index - rf.logIndexBefore
}

func (rf *Raft) logIndex_local2global(local_index int) int {
	return local_index + rf.logIndexBefore
}


func (rf *Raft) initial_logIndexBefore() {
	rf.logIndexBefore = 0
}

func (rf *Raft) readSnapshot(data []byte, store *[]LogType) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var logs	[]LogType
	if d.Decode(&logs) != nil {
		DPrintf(warnFormat+"====================== (readSnapShot) error ==========================="+defaultFormat)
	} else {
		*store = logs
	}
}

func (rf *Raft) saveSnapshot(stateLogs []LogType, snapShotLogs []LogType) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(snapShotLogs)

	data := w.Bytes()

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)

	e2.Encode(rf.currentTerm)
	e2.Encode(rf.votedFor)
	e2.Encode(stateLogs)

	data2 := w2.Bytes()
	rf.persister.SaveStateAndSnapshot(data2, data)
}

func (rf *Raft) createSnapShot(index int) {
	var snapShotLogs []LogType
	rf.readSnapshot(rf.persister.ReadSnapshot(), &snapShotLogs)
	for i := 1; i + rf.logIndexBefore < index; i++ {
		snapShotLogs = append(snapShotLogs, rf.logs[i])
	}
	
	var replace_logs []LogType
	replace_logs = append(replace_logs, LogType{
		LogTerm	: rf.logs[index-1].LogTerm,
	})

	for i := index; i < len(rf.logs); i++ {
		replace_logs = append(replace_logs, rf.logs[i])
	}

	rf.saveSnapshot(replace_logs, snapShotLogs)
	rf.logs = replace_logs
	rf.logIndexBefore = index - 1
} 