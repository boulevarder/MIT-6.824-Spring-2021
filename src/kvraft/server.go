package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"

	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int
const (
	GetType		OpType = 0
	PutType		OpType = 1
	AppendType	OpType = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key		string
	Value	string
	Type	OpType
	Client_id	int
	Command_id	int
}

type CommandMark struct {
	Client_id	int
	Command_id	int
}

type CommandState bool
const (
	Apply		CommandState = true
	NotApply	CommandState = false
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap					map[string] string
	clientMinCommand		map[int] int
	clientCommandIdState	map[int] map[int] CommandState

	lastApplied		int

	waitApplyCond	sync.Cond
	informApplyCond	sync.Cond

	waitIndexToMark	map[int] CommandMark
}

func (kv *KVServer) sendGetOp(command Op) (Err, string) {
	kv.mu.Lock()
	index, startTerm, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader, ""
	}
	kv.waitIndexToMark[index] = CommandMark {
		Client_id	: 1,
		Command_id	: 1,
	}
	kv.mu.Unlock()

	DPrintf(redFormat + "(solveGetOp begin) role: %v, type: %v, index: %v, term: %v, command: %v"+defaultFormat,
			kv.me, command.Type, index, startTerm, command)
	go func() {
		for !kv.killed() {
			time.Sleep(time.Millisecond * time.Duration(100))

			kv.mu.Lock()
			mark, exist := kv.waitIndexToMark[index]

			if !exist {
				kv.mu.Unlock()
				return
			}

			if kv.lastApplied == index || mark.Client_id == -1 {
				kv.mu.Unlock()
				kv.informOp()
				continue
			}


			if kv.lastApplied < index {
				raftTerm, _ := kv.rf.GetState()

				if raftTerm != startTerm {
					kv.waitIndexToMark[index] = CommandMark {
						Client_id : -1,
					}
					DPrintf(whiteFormat+"(solveGetOp term outdated) role: %v, index: %v"+defaultFormat,
						kv.me, index)
					kv.mu.Unlock()
					kv.informOp()
					continue
				}
			}
			kv.mu.Unlock()
		}
	}()

	for !kv.killed() {
		kv.waitApply()

		kv.mu.Lock()
		if kv.waitIndexToMark[index].Client_id == -1 {
			delete(kv.waitIndexToMark, index)
			kv.mu.Unlock()
			kv.informApply()
			return ErrWrongLeader, ""
		}

		if kv.lastApplied < index {
			kv.mu.Unlock()
			kv.informApply()
			continue
		}

		if kv.lastApplied == index {
			delete(kv.waitIndexToMark, index)
			value, exist := kv.kvMap[command.Key]
			kv.mu.Unlock()
			kv.informApply()
			if exist {
				return OK, value
			} else {
				return ErrNoKey, ""
			}
		}
		kv.mu.Unlock()
	}
	return ErrWrongLeader, ""
	
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op {
		Key		: args.Key,
	}

	err, value := kv.sendGetOp(command)
	reply.Err = err
	reply.Value = value
}

func (kv *KVServer) waitPutAppendResult(client_id int, command_id int) Err {
	for !kv.killed() {
		kv.mu.Lock()
		state, exist := kv.clientCommandIdState[client_id][command_id]
		kv.mu.Unlock()

		if exist {
			if state == Apply {
				return OK
			} else {
				kv.waitApply()
				continue
			}
		} else {
			return ErrWrongLeader
		}
	}
	return ErrWrongLeader
}

func (kv *KVServer) sendPutAppendOp(command Op) Err {
	index, startTerm, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	client_id := command.Client_id
	command_id := command.Command_id
	kv.clientCommandIdState[client_id][command_id] = NotApply
	kv.waitIndexToMark[index] = CommandMark {
		Client_id	: client_id,
		Command_id	: command_id,
	}
	kv.mu.Unlock()

	DPrintf(redFormat + "(solvePutAppendOp begin) role: %v, type: %v, index: %v, term: %v, command: %v"+defaultFormat,
			kv.me, command.Type, index, startTerm, command)

	go func() {
		for !kv.killed() {
			time.Sleep(time.Millisecond * time.Duration(100))
			
			kv.mu.Lock()
			_, exist := kv.waitIndexToMark[index]
			state, commandExist := kv.clientCommandIdState[client_id][command_id]
			if !exist {
				kv.mu.Unlock()
				return
			}

			if kv.lastApplied == index || (commandExist && state == Apply) ||
					kv.waitIndexToMark[index].Client_id == -1 {
				kv.mu.Unlock()
				
				kv.informOp()
				continue
			}

			if kv.lastApplied < index {
				raftTerm, _ := kv.rf.GetState()
				if raftTerm != startTerm {
					kv.waitIndexToMark[index] = CommandMark {
						Client_id : -1,
					}
					kv.mu.Unlock()
					DPrintf(whiteFormat+"(solvePutAppend term outdated) role: %v, index: %v"+defaultFormat,
						kv.me, index)
					kv.informOp()
					continue
				}
			}
			kv.mu.Unlock()
		}
	}()

	for !kv.killed() {
		kv.waitApply()

		kv.mu.Lock()
		if kv.waitIndexToMark[index].Client_id == -1 {
			delete(kv.waitIndexToMark, index)

			state, exist := kv.clientCommandIdState[client_id][command_id]
			if exist && state == NotApply {
				delete(kv.clientCommandIdState[client_id], command_id)
			}

			kv.mu.Unlock()
			kv.informApply()
			return ErrWrongLeader
		}

		state, exist := kv.clientCommandIdState[client_id][command_id]
		if exist && state == Apply {
			delete(kv.waitIndexToMark, index)
			kv.mu.Unlock()

			return OK
		}

		if kv.lastApplied < index {
			kv.mu.Unlock()
			kv.informApply()
			continue
		}

		if kv.lastApplied == index {
			delete(kv.waitIndexToMark, index)
			
			kv.mu.Unlock()
			kv.informApply()
			return OK
		}
		kv.mu.Unlock()
	}
	return ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op {
		Key		: args.Key,
		Value	: args.Value,
		Client_id	: args.Client_id,
		Command_id	: args.Command_id,
	}
	switch(args.Op) {
	case "Put":
		command.Type = PutType
	case "Append":
		command.Type = AppendType
	}

	kv.mu.Lock()
	client_id := command.Client_id
	command_id := command.Command_id
	kv.createClientInfo(client_id, command_id)
	_, exist := kv.clientCommandIdState[client_id][command_id]
	if exist {
		kv.mu.Unlock()
		reply.Err = kv.waitPutAppendResult(client_id, command_id)
	} else {
		reply.Err = kv.sendPutAppendOp(command)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string] string)
	kv.clientMinCommand = make(map[int] int)
	kv.clientCommandIdState = make(map[int] map[int]CommandState)

	kv.lastApplied = 0
	kv.waitApplyCond.L = new(sync.Mutex)
	kv.informApplyCond.L = new(sync.Mutex)
	kv.waitIndexToMark = make(map[int] CommandMark)

	go kv.wakeupRoutine()
	go kv.receiveApplyMsgRoutine()
	return kv
}

func (kv *KVServer) applySnapshot(apply *raft.ApplyMsg) {
	if kv.rf.CondInstallSnapshot(apply.SnapshotTerm, apply.SnapshotIndex, apply.Snapshot) {
		kv.mu.Lock()
		kv.readSnapshot(apply.Snapshot)
		kv.lastApplied = apply.SnapshotIndex
		kv.mu.Unlock()
	}
}

func (kv *KVServer) createClientInfo(client_id int, command_id int) {
	min_command, client_exist := kv.clientMinCommand[client_id]
	if client_exist == false {
		kv.clientMinCommand[client_id] = command_id
		kv.clientCommandIdState[client_id] = make(map[int] CommandState)
		min_command = command_id
	}

	if command_id - min_command > 10 {
		for i := min_command; i < command_id - 10; i++ {
			delete(kv.clientCommandIdState[client_id], i)
		}
		kv.clientMinCommand[client_id] = command_id - 10
	}
}

func (kv *KVServer) applyCommand(apply *raft.ApplyMsg) {
	kv.mu.Lock()
	if apply.CommandIndex <= kv.lastApplied {
		kv.mu.Unlock()
		return
	}

	command := apply.Command.(Op)

	if _, isLeader := kv.rf.GetState(); isLeader {
		DPrintf(blueFormat+"(applyMsg leader) role: %v, commandIndex: %v"+defaultFormat,
			kv.me, apply.CommandIndex)
	} else {
		DPrintf(blueFormat+"(applyMsg) role: %v, commandIndex: %v"+defaultFormat,
			kv.me, apply.CommandIndex)
	}

	commandIndex := apply.CommandIndex
	client_id := command.Client_id
	command_id := command.Command_id
	if command.Type != GetType {
		kv.createClientInfo(client_id, command_id)
	
		state, exist := kv.clientCommandIdState[client_id][command_id]
		if !exist || state == NotApply {
			kv.clientCommandIdState[client_id][command_id] = Apply

			switch(command.Type) {
			case PutType:
				kv.kvMap[command.Key] = command.Value
			case AppendType:
				kv.kvMap[command.Key] += command.Value
			}
		}
	}
	kv.lastApplied = commandIndex

	commandMark, existWaitOp := kv.waitIndexToMark[commandIndex]

	if existWaitOp && (commandMark.Client_id != client_id ||
			commandMark.Command_id != command_id) {
		commandMark.Client_id = -1
	}

	for existWaitOp && commandMark.Client_id == client_id && 
			commandMark.Command_id == command_id {
		kv.mu.Unlock()

		kv.informOp()
		kv.waitOp()

		kv.mu.Lock()
		commandMark, existWaitOp = kv.waitIndexToMark[commandIndex]
	}
	kv.mu.Unlock()
}

func (kv *KVServer) receiveApplyMsgRoutine() {
	for apply := range kv.applyCh {
		if kv.killed() {
			return
		}

		if apply.SnapshotValid {
			kv.applySnapshot(&apply)
		} else if apply.CommandValid {
			kv.applyCommand(&apply)
		}
	}
}

func (kv *KVServer) wakeupRoutine() {
	for !kv.killed() {
		time.Sleep(time.Millisecond * time.Duration(50))
		kv.mu.Lock()
		if _, exist := kv.waitIndexToMark[kv.lastApplied]; exist {
			kv.informOp()
		} else {
			kv.informApply()
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) informApply() {
	kv.informApplyCond.L.Lock()
	kv.informApplyCond.Broadcast()
	kv.informApplyCond.L.Unlock()
}

func (kv *KVServer) waitApply() {
	kv.waitApplyCond.L.Lock()
	kv.waitApplyCond.Wait()
	kv.waitApplyCond.L.Unlock()
}

func (kv *KVServer) informOp() {
	kv.waitApplyCond.L.Lock()
	kv.waitApplyCond.Broadcast()
	kv.waitApplyCond.L.Unlock()
}

func (kv *KVServer) waitOp() {
	kv.informApplyCond.L.Lock()
	kv.informApplyCond.Wait()
	kv.informApplyCond.L.Unlock()
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap			map[string] string
	var clientMinCommand map[int] int
	var clientCommandIdState map[int] map[int]CommandState
	if d.Decode(&kvMap) != nil || 
		d.Decode(&clientMinCommand) != nil ||
		d.Decode(&clientCommandIdState) != nil {
		log.Fatalf("======================== (readSnapshot) error =========================")
	} else {
		kv.kvMap = kvMap
		kv.clientMinCommand = clientMinCommand
		kv.clientCommandIdState = clientCommandIdState
	}
}