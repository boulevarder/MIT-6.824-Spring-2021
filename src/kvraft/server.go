package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"

	"strconv"
)

var (
	redFormat		string = "\033[35m"
	whiteFormat		string = "\033[37m"
	blueFormat		string = "\033[1;34m"
	warnFormat		string = "\033[1;33m"
	defaultFormat	string = "\033[0m"
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
	InvalidType	OpType = 3
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
	kvMap				map[string] string

	applyIndex 			int

	waitIndexMapTerm	map[int] int

	waitApplyCond		sync.Cond
	informApplyCond		sync.Cond
	
	identifyToResult 	map[string] Result
}

type Result struct {
	Value		string
	Err			Err
	Exist 		bool
}

func (kv *KVServer) waitResult(identify string) (Err, string) {
	for {
		kv.mu.Lock()
		result, keyExist := kv.identifyToResult[identify]
		kv.mu.Unlock()

		if keyExist {
			if keyExist {
				switch(result.Err){
				case OK:
					return OK, result.Value
				case ErrNoKey:
					return ErrNoKey, ""
				}
			} else {
				kv.waitApply()
				continue
			}
		} else {
			return ErrWrongLeader, ""
		}
	}
}

func (kv *KVServer) sendToRaft_waitCommit(command Op, identify string) (Err, string) {
	// 进来时是加锁的
	index, startTerm, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader, ""
	}

	kv.identifyToResult[identify] = Result {
		Exist : false, 
	}
	kv.waitIndexMapTerm[index] = startTerm
	kv.mu.Unlock()

	DPrintf(redFormat + "(sendToRaft begin) role: %v, type: %v, index: %v, term: %v"+defaultFormat,
			kv.me, command.Type, index, startTerm)
	go func() {
		for !kv.killed() {
			time.Sleep(time.Millisecond * time.Duration(100))

			kv.mu.Lock()

			_, exist := kv.waitIndexMapTerm[index]
			
			if !exist {
				kv.mu.Unlock()
				
				time.AfterFunc(3 * time.Second, func() {
					kv.mu.Lock()
					if _, exist := kv.identifyToResult[identify]; exist {
						delete(kv.identifyToResult, identify)
					}
					kv.mu.Unlock()
				})
				return
			}

			if kv.applyIndex == index {
				kv.mu.Unlock()

				kv.informOp()
				continue
			}
			
			if kv.applyIndex < index {
				invalidCommand := Op {
					Type : InvalidType,
				}

				raftIndex, _, _ := kv.rf.Start(invalidCommand)
				if raftIndex <= index {
					kv.waitIndexMapTerm[index] = -1
					kv.mu.Unlock()

					kv.informOp()
					continue
				}
			} else {
				log.Fatal(warnFormat+"(sendToRaft_waitCommit timer) role: %v, kv.applyIndex > index, never happend"+defaultFormat,
						kv.me)
			}
			kv.mu.Unlock()
		}
	}()

	isGetType := (command.Type == GetType)

	for !kv.killed() {
		kv.waitApply()

		kv.mu.Lock()
		if kv.applyIndex < index {
			kv.mu.Unlock()

			kv.informApply()
			continue
		}

		mapTerm, exist := kv.waitIndexMapTerm[index]

		if !exist {
			log.Fatalf(warnFormat+"(sendToRaft_waitCommit) role: %v, not exist in waitIndexMapTerm", kv.me)
		}

		if kv.applyIndex == index {
			delete(kv.waitIndexMapTerm, index)
			if mapTerm == startTerm {
				var value string
				var keyExist bool
				if isGetType {
					value, keyExist = kv.kvMap[command.Key]
				}
				kv.mu.Unlock()

				kv.informApply()

				if isGetType {
					if keyExist {
						DPrintf(redFormat+"(sendToRaft, get succeed) role: %v, index: %v"+defaultFormat, kv.me, index)
						kv.identifyToResult[identify] = Result {
							Value	: value,
							Err		: OK,
							Exist	: true,
						}
						return OK, value
					} else {
						DPrintf(redFormat+"(sendToRaft, get nokey) role: %v, index: %v "+defaultFormat, kv.me, index)
						kv.identifyToResult[identify] = Result {
							Value	: value,
							Err		: ErrNoKey,
							Exist	: true,
						}
						return ErrNoKey, ""
					}
				} else {
					DPrintf(redFormat+"(sendToRaft, put succeed), role: %v, index: %v"+defaultFormat, kv.me, index)
					kv.identifyToResult[identify] = Result {
						Err		: OK,
						Exist	: true,
					}
					return OK, ""
				} 
			} else if mapTerm == -1 {
				DPrintf(redFormat+"(sendToRaft, log truncated) role: %v, index: %v, startTerm: %v"+defaultFormat,
					kv.me, index, startTerm)
				delete(kv.identifyToResult, identify)
				return ErrWrongLeader, ""
			} else {
				log.Fatalf(warnFormat+"(error, sendToRaft) role: %v, mapTerm: %v != startTerm: %v"+defaultFormat,
					kv.me, mapTerm, startTerm)
			}
		} else if kv.applyIndex < index {
			kv.mu.Unlock()
			kv.informApply()
			continue
		} else {
			log.Fatal(warnFormat+"(error, sendToRaft) role: %v, kv.applyIndex: %v > index: %v"+defaultFormat,
				kv.me, kv.applyIndex, index)
		}
	}
	return "killed", ""
}



func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op {
		Key		: args.Key,
		Type	: GetType,
	}

	identify := computeIdentify(args.RandNum, args.Key)

	kv.mu.Lock()
	_, keyExist := kv.identifyToResult[identify]

	if keyExist {
		kv.mu.Unlock()

		err, value := kv.waitResult(identify)

		reply.Err = err
		reply.Value = value
		return
	} else {
		err, value := kv.sendToRaft_waitCommit(command, identify)

		reply.Err = err
		reply.Value = value
		return
	}
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

	identify := computeIdentify(args.RandNum, args.Key)

	kv.mu.Lock()
	_, keyExist := kv.identifyToResult[identify]

	if keyExist {
		kv.mu.Unlock()
		err, _ := kv.waitResult(identify)
		
		reply.Err = err
		return
	} else {
		err, _ := kv.sendToRaft_waitCommit(command, identify)

		reply.Err = err
		return
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

func (kv *KVServer) receiveApplyMsg() {
	for apply := range kv.applyCh {
		if apply.CommandValid == false {
			continue
		}
		if _, isLeader := kv.rf.GetState(); isLeader {
			DPrintf(blueFormat+"(applyMsg leader) role: %v, commandIndex: %v"+defaultFormat,
				kv.me, apply.CommandIndex)
		} else {
			DPrintf(blueFormat+"(applyMsg) role: %v, commandIndex: %v"+defaultFormat,
				kv.me, apply.CommandIndex)
		}

		kv.mu.Lock()
		if kv.applyIndex < apply.CommandIndex { 
			command := apply.Command.(Op)
			switch(command.Type) {
			case PutType:
				kv.kvMap[command.Key] = command.Value
			case AppendType:
				kv.kvMap[command.Key] += command.Value
			case GetType:
			case InvalidType:
			}

			kv.applyIndex = apply.CommandIndex
		} else {
			DPrintf(blueFormat+"(receiveApplyMsg role: %v) multiple apply, kv.applyIndex: %v >= apply.CommandIndex: %v"+defaultFormat,
				kv.me, kv.applyIndex, apply.CommandIndex)
		}

		mapTerm, exist := kv.waitIndexMapTerm[apply.CommandIndex]
		for exist {
			if mapTerm != apply.CommandTerm {
				DPrintf(blueFormat+"(receiveApplyMsg) role: %v, index: %v, startTerm: %v, mapTerm: %v"+defaultFormat,
					kv.me, apply.CommandIndex, mapTerm, apply.CommandTerm)
				kv.waitIndexMapTerm[apply.CommandIndex] = -1
			}
			kv.mu.Unlock()

			kv.informOp()
			kv.waitOp()

			kv.mu.Lock()
			mapTerm, exist = kv.waitIndexMapTerm[apply.CommandIndex]
		}
		kv.mu.Unlock()
	}
}


func (kv *KVServer) wakeupRoutine() {
	for !kv.killed() {
		time.Sleep(time.Millisecond * time.Duration(50))
		kv.mu.Lock()

		if _, exist := kv.waitIndexMapTerm[kv.applyIndex]; exist {
			kv.informOp()
		} else {
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

	kv.waitIndexMapTerm = make(map[int] int)
	kv.waitApplyCond.L = new(sync.Mutex)
	kv.informApplyCond.L = new(sync.Mutex)

	kv.identifyToResult = make(map[string] Result)
	
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

func computeIdentify(randNum int64, key string) string {
	lenKey := len(key)

	return strconv.FormatInt(randNum, 10) + strconv.Itoa(lenKey) + key
}