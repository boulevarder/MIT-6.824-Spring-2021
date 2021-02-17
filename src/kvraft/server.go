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

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var (
	redFormat		string = "\033[35m"
	whiteFormat		string = "\033[37m"
	blueFormat		string = "\033[1;34m"
	warnFormat		string = "\033[1;33m"
	defaultFormat	string = "\033[0m"
)

type OpType int
const (
	GetType		OpType = 0
	PutType		OpType = 1
	AppendType 	OpType = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key			string
	Value		string
	Type 		OpType
	RandNum		int64
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
	
	applyIndex			int

	waitApplyCond		sync.Cond
	informApplyCond		sync.Cond

	waitIndexMapTerm	map[int] int
	identifyToResult	map[string] StoreState

	clearIdentifyCh		chan string
}


func (kv *KVServer) solveGetOp(command Op) (Err, string) {
	kv.mu.Lock()
	index, startTerm, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader, ""
	}

	kv.waitIndexMapTerm[index] = startTerm
	kv.mu.Unlock()

	DPrintf(redFormat + "(solveGetOp begin) role: %v, type: %v, index: %v, term: %v, command: %v"+defaultFormat,
			kv.me, command.Type, index, startTerm, command)
	go func() {
		for !kv.killed() {
			time.Sleep(time.Millisecond * time.Duration(100))

			kv.mu.Lock()
			_, exist := kv.waitIndexMapTerm[index]

			if !exist {
				kv.mu.Unlock()
				return
			}

			if kv.applyIndex == index {
				kv.mu.Unlock()
				kv.informOp()
				continue
			}

			if kv.applyIndex < index {
				raftTerm, _ := kv.rf.GetState()

				if raftTerm != startTerm {
					kv.waitIndexMapTerm[index] = -1
					DPrintf(whiteFormat+"(solveGetOp term outdated) role: %v, index: %v"+defaultFormat,
						kv.me, index)
					kv.mu.Unlock()

					kv.informOp()
					continue
				}
			} else {
				log.Fatal(warnFormat+"(solveGetOp timer) role: %v, kv.applyIndex > index, never happend"+defaultFormat,
						kv.me)
			}
			kv.mu.Unlock()
		}
	}()

	for !kv.killed() {
		kv.waitApply()

		kv.mu.Lock()
		commitTerm, exist := kv.waitIndexMapTerm[index]

		if !exist {
			log.Fatalf(warnFormat+"(solveGetOp) role: %v, index: %v not exist waitIndexMapTerm"+defaultFormat,
				kv.me, index)
		}

		if commitTerm == -1 {
			delete(kv.waitIndexMapTerm, index)
			DPrintf(redFormat+"(solveGetOp log truncated) role: %v, index: %v, startTerm: %v, commitTerm: -1"+defaultFormat,
					kv.me, index, startTerm)
			kv.mu.Unlock()

			return ErrWrongLeader, ""
		}

		if kv.applyIndex < index {
			kv.mu.Unlock()

			kv.informApply()
			continue
		}

		if kv.applyIndex == index {
			delete(kv.waitIndexMapTerm, index)
			if commitTerm == startTerm {
				value, keyExist := kv.kvMap[command.Key]
				kv.mu.Unlock()

				kv.informApply()
				if keyExist {
					DPrintf(redFormat+"(solveGetOp, get successed) role: %v, index: %v"+defaultFormat,
						kv.me, index)
					return OK, value
				} else {
					DPrintf(redFormat+"(solveGetOp, get NoKey) role: %v, index: %v"+defaultFormat,
						kv.me, index)
					return ErrNoKey, ""
				}
			} else {
				kv.mu.Unlock()
				kv.informApply()
				DPrintf(redFormat+"(solveGetOp, wrongleader(error)) role: %v, startTerm: %v != commitTerm: %v"+defaultFormat,
					kv.me, startTerm, commitTerm)
				return ErrWrongLeader, ""
			}
		} else if kv.applyIndex < index {
			kv.mu.Unlock()
			kv.informApply()
			continue
		} else {
			log.Fatalf(warnFormat+"(error, solveGetOp) role: %v, kv.applyIndex: %v > index: %v"+defaultFormat,
				kv.me, kv.applyIndex, index)
		}
	}
	return ErrWrongLeader, ""
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op {
		Key		: args.Key,
		Type 	: GetType,
	}

	err, value := kv.solveGetOp(command)
	reply.Err = err
	reply.Value = value
	return 
}

type State int 

const (
	OKState		State = 0
	OtherLeader State = 2
)

type StoreState struct {
	value			string
	state 			State
	alreadyCommit	bool
}

func (kv *KVServer) waitResult(identify string) Err {
	for !kv.killed() {
		kv.mu.Lock()
		storeState, exist := kv.identifyToResult[identify]
		kv.mu.Unlock()

		if exist {
			if storeState.alreadyCommit {
				switch(storeState.state){
				case OKState:
					return OK
				case OtherLeader:
					return OK
				}
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

func (kv *KVServer) solvePutAppendOp(command Op, identify string) Err {
	index, startTerm, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	
	kv.identifyToResult[identify] = StoreState {
		alreadyCommit : false,
	}
	kv.waitIndexMapTerm[index] = startTerm
	kv.mu.Unlock()

	DPrintf(redFormat + "(solvePutAppendOp begin) role: %v, type: %v, index: %v, term: %v, command: %v"+defaultFormat,
			kv.me, command.Type, index, startTerm, command)
	go func() {
		for !kv.killed() {
			time.Sleep(time.Millisecond * time.Duration(100))

			kv.mu.Lock()

			_, exist := kv.waitIndexMapTerm[index]
			
			if !exist {
				kv.mu.Unlock()
				kv.clearIdentifyCh <- identify
				return 
			}

			if kv.applyIndex == index {
				kv.mu.Unlock()

				kv.informOp()
				continue
			}

			if kv.applyIndex < index {
				raftTerm, _ := kv.rf.GetState()

				if raftTerm != startTerm {
					kv.waitIndexMapTerm[index] = -1
					kv.mu.Unlock()
					DPrintf(whiteFormat+"(solvePutAppend term outdated) role: %v, index: %v"+defaultFormat,
						kv.me, index)
					kv.informOp()
					continue
				}
			} else {
				log.Fatalf(warnFormat+"(solvePutAppendOp, timer) role: %v, kv.applyIndex: %v > index: %v, never happend"+defaultFormat,
						kv.me, kv.applyIndex, index)
			}
			kv.mu.Unlock()
		}
	}()

	for !kv.killed() {
		kv.waitApply()

		kv.mu.Lock()
		commitTerm, exist := kv.waitIndexMapTerm[index]

		if !exist {
			log.Fatalf(warnFormat+"(solvePutAppendOp) role: %v, index: %v not exist in waitIndexMapTerm"+defaultFormat, 
				kv.me, index)
		}

		if commitTerm == -1 {
			DPrintf(redFormat+"(solvePutAppendOp, log truncated) role: %v, index: %v, startTerm: %v, commitTerm: -1"+defaultFormat,
					kv.me, index, startTerm)
			delete(kv.waitIndexMapTerm, index)
			delete(kv.identifyToResult, identify)
			kv.mu.Unlock()
			
			return ErrWrongLeader
		}

		if kv.applyIndex < index {
			kv.mu.Unlock()

			kv.informApply()
			continue
		}

		if kv.applyIndex == index {
			delete(kv.waitIndexMapTerm, index)
			if commitTerm == startTerm {
				DPrintf(redFormat+"(solvePutAppendOp, putAppend succeed) role: %v, index: %v"+defaultFormat,
					kv.me, index)
				kv.identifyToResult[identify] = StoreState {
					state 			: OKState,
					alreadyCommit	: true,
				}
				kv.mu.Unlock()

				kv.informApply()
				return OK
			} else {
				DPrintf(warnFormat+"(solvePutAppendOp, other leader putAppend) role: %v, commitTerm: %v != startTerm: %v"+defaultFormat,
					kv.me, commitTerm, startTerm)
				kv.identifyToResult[identify] = StoreState {
					state 			: OtherLeader,
					alreadyCommit	: true,
				}
				kv.mu.Unlock()

				kv.informApply()
				return OK
			}
		} else if kv.applyIndex < index {
			kv.mu.Unlock()
			kv.informApply()
			continue
		} else {
			log.Fatal(warnFormat+"(error, solvePutAppendOp) role: %v, kv.applyIndex: %v > index: %v"+defaultFormat,
				kv.me, kv.applyIndex, index)
		}
	}
	return ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op {
		Key 	: args.Key,
		Value	: args.Value,
		RandNum	: args.RandNum,
	}

	switch(args.Op) {
	case "Put":
		command.Type = PutType
	case "Append":
		command.Type = AppendType
	}

	identify := computeIdentify(args.RandNum, args.Key)

	kv.mu.Lock()
	_, exist := kv.identifyToResult[identify]

	if exist {
		kv.mu.Unlock()
		err := kv.waitResult(identify)

		reply.Err = err
		return
	} else {
		err := kv.solvePutAppendOp(command, identify)

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

	kv.applyIndex = 0

	kv.waitApplyCond.L = new(sync.Mutex)
	kv.informApplyCond.L = new(sync.Mutex)

	kv.waitIndexMapTerm = make(map[int] int)
	kv.identifyToResult = make(map[string] StoreState)

	kv.clearIdentifyCh = make(chan string, 1)

	go kv.clearIdentifyRoutine()
	go kv.wakeupRoutine()
	go kv.receiveApplyMsgRoutine()

	return kv
}

func (kv *KVServer) receiveApplyMsgRoutine() {
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
			identify := computeIdentify(command.RandNum, command.Key)

			storeState, exist := kv.identifyToResult[identify]
			if exist == false || storeState.alreadyCommit == false {
				switch(command.Type) {
				case PutType:
					kv.kvMap[command.Key] = command.Value
				case AppendType:
					kv.kvMap[command.Key] += command.Value
				case GetType:
				}

				if exist == false && command.Type != GetType {
					kv.identifyToResult[identify] = StoreState {
						state			: OtherLeader,
						alreadyCommit 	: true,
					}
					kv.clearIdentifyCh <- identify
				} else {
					kv.identifyToResult[identify] = StoreState {
						state 			: OKState,
						alreadyCommit   : true,
					}
				}
			}
			kv.applyIndex = apply.CommandIndex
		} else {
			DPrintf(blueFormat+"(receiveApplyMsg role: %v) multiple apply, kv.applyIndex: %v >= apply.CommandIndex: %v"+defaultFormat,
				kv.me, kv.applyIndex, apply.CommandIndex)
		}

		startTerm, exist := kv.waitIndexMapTerm[apply.CommandIndex]
		for exist {
			if startTerm != apply.CommandTerm {
				DPrintf(blueFormat+"(receiveApplyMsg) role: %v, index: %v, startTerm: %v, applyTerm: %v"+defaultFormat,
					kv.me, apply.CommandIndex, startTerm, apply.CommandTerm)
				kv.waitIndexMapTerm[apply.CommandIndex] = apply.CommandTerm
			}
			kv.mu.Unlock()

			kv.informOp()
			kv.waitOp()

			kv.mu.Lock()
			startTerm, exist = kv.waitIndexMapTerm[apply.CommandIndex]
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


func (kv *KVServer) clearIdentifyRoutine() {

	for identify := range kv.clearIdentifyCh {
		go func(i string) {
			time.AfterFunc(3 * time.Second, func() {
				kv.mu.Lock()
				if _, exist := kv.identifyToResult[i]; exist {
					delete(kv.identifyToResult, i)
				}
				kv.mu.Unlock()
			})
		}(identify)

		if kv.killed() {
			return
		}
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