package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType	int
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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap	map[string] string

	applyIndex 		int

	haveOpWait		bool
	waitIndex		int
	waitTerm		int

	waitApplyCond	sync.Cond
	informApplyCond	sync.Cond	
}


func (kv *KVServer) sendToRaft_waitCommit(command Op) (Err, string) {
	index, startTerm, isLeader := kv.rf.Start(command)

	if isLeader == false {
		return ErrWrongLeader, ""
	}

	isGetType := (command.Type == GetType)

	kv.mu.Lock()
	for kv.haveOpWait {
		kv.mu.Unlock()
		
		kv.waitApply()

		kv.mu.Lock()
	}
	kv.haveOpWait = true
	kv.waitIndex = index
	kv.waitTerm = startTerm
	kv.mu.Unlock()

	go func() {
		time.Sleep(time.Millisecond * time.Duration(100))
		// partition
		kv.mu.Lock()
		if kv.haveOpWait && kv.waitIndex == index {
			kv.waitTerm = -1
		}
		kv.mu.Unlock()
	}()

	for {
		kv.waitApply()

		kv.mu.Lock()
		if kv.waitTerm == -1 {
			kv.haveOpWait = false
			kv.mu.Unlock()

			kv.informOp()
			return ErrWrongLeader, ""
		}

		if kv.waitTerm == startTerm && kv.waitIndex == kv.applyIndex {
			var value string
			var keyExist bool
			if isGetType {
				value, keyExist = kv.kvMap[command.Key]
			}
			kv.haveOpWait = false
			kv.mu.Unlock()

			kv.informOp()
			
			if isGetType {
				if keyExist {
					return OK, value
				} else {
					return ErrNoKey, ""
				}
			} else {
				return OK, ""
			}
		}

		kv.mu.Unlock()
	}


} 

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op {
		Key		: args.Key,
		Type	: GetType,
	}

	err, value := kv.sendToRaft_waitCommit(command)
	
	reply.Err = err
	reply.Value = value
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op {
		Key		: args.Key,
		Value	: args.Value,
	}

	switch(args.Op) {
	case "Put":
		command.Type = PutType
	case "Append":
		command.Type = AppendType
	}

	err, _ := kv.sendToRaft_waitCommit(command)

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

func (kv *KVServer) receiveApplyMsg() {
	for apply := range kv.applyCh {
		if apply.CommandValid == false {
			continue
		}
		DPrintf("apply, role: %v, commandIndex: %v", kv.me, apply.CommandIndex)

		kv.mu.Lock()

		for kv.haveOpWait && apply.CommandIndex > kv.waitIndex {
			kv.waitOp()

			kv.mu.Unlock()

			kv.informOp()

			kv.mu.Lock()
		}
		
		command := apply.Command.(Op)
		switch(command.Type) {
		case PutType:
			kv.kvMap[command.Key] = command.Value
		case AppendType:
			kv.kvMap[command.Key] += command.Value
		case GetType:
		}

		if apply.CommandIndex > kv.applyIndex {
			kv.applyIndex = apply.CommandIndex
		}
		if apply.CommandIndex == kv.waitIndex {
			kv.informOp()
		}

		kv.mu.Unlock()
	}
}


func (kv *KVServer) wakeupRoutine() {
	for !kv.killed() {
		time.Sleep(time.Millisecond * time.Duration(50))

		kv.mu.Lock()
		if kv.haveOpWait && kv.waitIndex == kv.applyIndex {
			kv.informOp()
		}

		if !kv.haveOpWait || kv.applyIndex < kv.waitIndex {
			kv.informApply()
		}
		kv.mu.Unlock()
	}
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

	kv.waitApplyCond.L = new(sync.Mutex)
	kv.informApplyCond.L = new(sync.Mutex)

	
	go kv.receiveApplyMsg()
	go kv.wakeupRoutine()

	return kv
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

func (kv *KVServer) waitOp() {
	kv.informApplyCond.L.Lock()
	kv.informApplyCond.Wait()
	kv.informApplyCond.L.Unlock()
}

func (kv *KVServer) informOp() {
	kv.waitApplyCond.L.Lock()
	kv.waitApplyCond.Broadcast()
	kv.waitApplyCond.L.Unlock()
}