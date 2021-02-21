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

type ValueState struct {
	value	string
	exist	bool
}

type OpState struct {
	client_id	int
	command_id	int
	valueCh		chan ValueState
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap				map[string] string
	clientLastCommand	map[int] int
	indexToOpState		map[int] OpState

	persister			*raft.Persister
}

func (kv *KVServer) sendToRaft(op Op) (Err, string) {
	index, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, ""
	}

	kv.mu.Lock()
	client_id := op.Client_id
	command_id := op.Command_id
	valueCh := make(chan ValueState)
	
	kv.indexToOpState[index] = OpState {
		client_id	: client_id,
		command_id	: command_id,
		valueCh		: valueCh,
	}
	kv.mu.Unlock()
	DPrintf(blueFormat+"role: %v, command: %v, type: %v, command_id: %v"+defaultFormat, kv.me, op, op.Type, command_id)

	informCh := make(chan bool)

	go func() {
		for !kv.killed() {
			time.Sleep(100 * time.Millisecond)

			kv.mu.Lock()
			_, exist := kv.indexToOpState[index]
			if !exist {
				kv.mu.Unlock()
				return
			}

			raftTerm, _ := kv.rf.GetState()
			if raftTerm != startTerm {
				opstate := kv.indexToOpState[index]
				opstate.client_id = -1
				kv.indexToOpState[index] = opstate
				kv.mu.Unlock()
				close(informCh)
				return
			}
			if command_id <= kv.clientLastCommand[client_id] {
				if op.Type == GetType {
					opstate := kv.indexToOpState[index]
					opstate.client_id = -1
					kv.indexToOpState[index] = opstate
				}
				kv.mu.Unlock()
				close(informCh)
				return
			}
			kv.mu.Unlock()
		}
	}()

	for !kv.killed() {
		var res ValueState
		select {
		case <-informCh:
		case res =<-valueCh:
		}

		kv.mu.Lock()
		if command_id <= kv.clientLastCommand[client_id] {
			if op.Type != GetType {
				delete(kv.indexToOpState, index)
				kv.mu.Unlock()
				DPrintf(blueFormat+"role: %v, command: %v, type: %v, ok"+defaultFormat, kv.me, op, op.Type)
				return OK, ""
			} else {
				if kv.indexToOpState[index].client_id != -1 {
					delete(kv.indexToOpState, index)
					kv.mu.Unlock()
					if res.exist == true {
						return OK, res.value
					} else {
					DPrintf(blueFormat+"role: %v, command: %v, type: %v, errnokey"+defaultFormat, kv.me, op, op.Type)
						return ErrNoKey, ""
					}
				}
			}
		}

		if kv.indexToOpState[index].client_id == -1 {
			delete(kv.indexToOpState, index)
			kv.mu.Unlock()
			DPrintf(blueFormat+"role: %v, command: %v, type: %v, errwrongleader"+defaultFormat, kv.me, op, op.Type)
			return ErrWrongLeader, ""
		}
		kv.mu.Unlock()
	}
	return ErrWrongLeader, ""
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op {
		Key			: args.Key,
		Client_id 	: args.Client_id,
		Command_id	: args.Command_id,
	}

	err, value := kv.sendToRaft(command)
	reply.Err = err
	reply.Value = value
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
	err, _ := kv.sendToRaft(command)
	reply.Err = err
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
	kv.clientLastCommand = make(map[int] int)
	kv.indexToOpState = make(map[int] OpState)

	kv.persister = persister

	go kv.receiveApplyMsgRoutine()
	return kv
}

func (kv *KVServer) applySnapshot(apply *raft.ApplyMsg) {
	if kv.rf.CondInstallSnapshot(apply.SnapshotTerm, apply.SnapshotIndex, apply.Snapshot) {
		kv.mu.Lock()
		kv.readSnapshot(apply.Snapshot)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) judgeAlreadyApply(client_id int, command_id int) bool {
	lastCommand_id, exist := kv.clientLastCommand[client_id]
	if !exist || lastCommand_id < command_id {
		kv.clientLastCommand[client_id] = command_id
		return true
	}
	return false
}

func (kv *KVServer) applyCommand(apply *raft.ApplyMsg) {
	kv.mu.Lock()
	op := apply.Command.(Op)
	client_id := op.Client_id
	command_id := op.Command_id
	if kv.judgeAlreadyApply(client_id, command_id) == false && op.Type != GetType {
		kv.mu.Unlock()
		return
	}
	var value string
	var exist bool
	switch(op.Type) {
	case PutType:
		kv.kvMap[op.Key] = op.Value
	case AppendType:
		kv.kvMap[op.Key] += op.Value
	case GetType:
		value, exist = kv.kvMap[op.Key]
	}

	opstate, exist := kv.indexToOpState[apply.CommandIndex]
	if !exist || opstate.client_id != client_id || opstate.command_id != command_id {
		if exist {
			opstate := kv.indexToOpState[apply.CommandIndex]
			opstate.client_id = -1
			kv.indexToOpState[apply.CommandIndex] = opstate
			close(opstate.valueCh)
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	opstate.valueCh <- ValueState {
		value	: value,
		exist	: exist,
	}
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
			if kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.mu.Lock()
				snapshot := kv.snapshot()
				index := apply.CommandIndex
				kv.mu.Unlock()
				kv.rf.Snapshot(index, snapshot)
			}
		}
	}
}


func (kv *KVServer) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kvMap)
	e.Encode(kv.clientLastCommand)

	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap	map[string] string
	var clientLastCommand map[int] int
	if d.Decode(&kvMap) != nil ||
		d.Decode(&clientLastCommand) != nil {
		log.Fatalf("error")
	} else {
		kv.kvMap = kvMap
		kv.clientLastCommand = clientLastCommand
	}
}