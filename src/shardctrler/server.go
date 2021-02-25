package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "log"
import "time"
import "sync/atomic"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	clientLastCommand	map[int] int
	indexToConfigState	map[int] OpState

	dead 	int32
}

type OpType int
const (
	QueryType	OpType = 0
	JoinType	OpType = 1
	LeaveType	OpType = 2
	MoveType	OpType = 3
)

type Op struct {
	// Your data here.
	Type	OpType
	Args	interface{}
	Command_id	int
	Client_id	int
}

type ConfigState struct {
	configIndex	int
}

type OpState struct {
	client_id	int
	command_id	int
	configCh	chan ConfigState
}

func (sc *ShardCtrler) sendToRaft(op Op) (bool, int) {
	index, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		return false, 0
	}
	
	sc.mu.Lock()
	client_id := op.Client_id
	command_id := op.Command_id
	configCh := make(chan ConfigState)

	sc.indexToConfigState[index] = OpState {
		client_id	: client_id,
		command_id	: command_id,
		configCh	: configCh,
	}
	sc.mu.Unlock()

	informCh := make(chan bool)

	go func() {
		for !sc.killed() {
			time.Sleep(100 * time.Millisecond)

			sc.mu.Lock()
			_, exist := sc.indexToConfigState[index]
			if !exist {
				sc.mu.Unlock()
				return
			}

			raftTerm, _ := sc.rf.GetState()
			if raftTerm != startTerm {
				configstate := sc.indexToConfigState[index]
				configstate.client_id = -1
				sc.indexToConfigState[index] = configstate
				sc.mu.Unlock()
				close(informCh)
				return
			}

			if command_id <= sc.clientLastCommand[client_id] {
				if op.Type == QueryType {
					configstate := sc.indexToConfigState[index]
					configstate.client_id = -1
					sc.indexToConfigState[index] = configstate
				}
				sc.mu.Unlock()
				close(informCh)
				return
			}
			sc.mu.Unlock()
		}
	}()

	for !sc.killed() {
		var res ConfigState
		select {
		case <-informCh:
		case res =<-configCh:
		}

		sc.mu.Lock()
		if command_id <= sc.clientLastCommand[client_id] {
			if op.Type != QueryType {
				delete(sc.indexToConfigState, index)
				sc.mu.Unlock()
				return true, 0
			} else {
				if sc.indexToConfigState[index].client_id != -1 {
					delete(sc.indexToConfigState, index)
					sc.mu.Unlock()
					return true, res.configIndex
				}
			}
		}

		if sc.indexToConfigState[index].client_id == -1 {
			delete(sc.indexToConfigState, index)
			sc.mu.Unlock()
			return false, 0
		}
		sc.mu.Unlock()
	}
	return false, 0
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op {
		Type	: JoinType,
		Args	: *args,
		Command_id	: args.Command_id,
		Client_id	: args.Client_id,
	}
	succeed, _ := sc.sendToRaft(command)

	if succeed {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op {
		Type	: LeaveType,
		Args	: *args,
		Command_id	: args.Command_id,
		Client_id	: args.Client_id,
	}
	succeed, _ := sc.sendToRaft(command)

	if succeed {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op {
		Type	: MoveType,
		Args	: *args,
		Command_id	: args.Command_id,
		Client_id	: args.Client_id,
	}
	succeed, _ := sc.sendToRaft(command)

	if succeed {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command := Op {
		Type	: QueryType,
		Args	: *args,
		Command_id	: args.Command_id,
		Client_id	: args.Client_id,
	}
	succeed, index := sc.sendToRaft(command)

	if succeed {
		reply.WrongLeader = false
		sc.mu.Lock()
		reply.Config.Num = sc.configs[index].Num
		reply.Config.Groups = make(map[int] []string)
		for gid, servers := range sc.configs[index].Groups {
			reply.Config.Groups[gid] = servers
		}

		for i := 0; i < NShards; i++ {
			reply.Config.Shards[i] = sc.configs[index].Shards[i]
		}
		DPrintf(blueFormat+"query result: %v"+defaultFormat, index)
		sc.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientLastCommand = make(map[int] int)
	sc.indexToConfigState = make(map[int] OpState)

	go sc.receiveApplyMsgRoutine()

	return sc
}


func (sc *ShardCtrler) judgeAlreadyApply(client_id int, command_id int) bool {
	lastCommand_id, exist := sc.clientLastCommand[client_id]
	if !exist || lastCommand_id < command_id {
		sc.clientLastCommand[client_id] = command_id
		return false
	}
	return true
}

func (sc *ShardCtrler) applyCommand(apply *raft.ApplyMsg) {
	sc.mu.Lock()
	op := apply.Command.(Op)
	client_id := op.Client_id
	command_id := op.Command_id
	if sc.judgeAlreadyApply(client_id, command_id) == true && op.Type != QueryType {
		sc.mu.Unlock()
		return
	}

	var configIndex int
	switch(op.Type) {
	case JoinType:
		sc.applyJoin(apply)
	case LeaveType:
		sc.applyLeave(apply)
	case MoveType:
		sc.applyMove(apply)
	case QueryType:
		configIndex = sc.queryConfig(apply)
	}

	opstate, exist := sc.indexToConfigState[apply.CommandIndex]
	if !exist || opstate.client_id != client_id || opstate.command_id != command_id {
		if exist {
			opstate := sc.indexToConfigState[apply.CommandIndex]
			opstate.client_id = -1
			sc.indexToConfigState[apply.CommandIndex] = opstate
			close(opstate.configCh)
		}
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	opstate.configCh <- ConfigState {
		configIndex	: configIndex,
	}
}


func (sc *ShardCtrler) receiveApplyMsgRoutine() {
	for apply := range sc.applyCh {
		if sc.killed() {
			return
		}

		if apply.CommandValid {
			sc.applyCommand(&apply)
		}
	}
}

func copyConfig(src_config *Config) Config {
	dst_config := Config {
		Num		: src_config.Num + 1,
		Groups	: make(map[int] []string),
	}

	for i := 0; i < NShards; i++ {
		dst_config.Shards[i] = src_config.Shards[i]
	}

	for gid, servers := range src_config.Groups {
		dst_config.Groups[gid] = servers
	}
	return dst_config
}

func (sc *ShardCtrler) adjustShards(config *Config) {
	gidNum := len(config.Groups)
	if gidNum == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}

	avgShardPerGid := NShards / gidNum
	isRemainder := false
	if avgShardPerGid * gidNum != NShards {
		isRemainder = true
	}

	gidMapRef := make(map[int] int)
	for gid, _ := range config.Groups {
		gidMapRef[gid] = 0
	}

	for i := 0; i < NShards; i++ {
		gid := config.Shards[i]
		ref, exist := gidMapRef[gid]
		if !exist {
			config.Shards[i] = 0
			continue
		}
		ref++
		
		if isRemainder {
			if ref == avgShardPerGid + 1 {
				isRemainder = false
				gidMapRef[gid] = ref
			} else if ref < avgShardPerGid + 1 {
				gidMapRef[gid] = ref
			} else {
				config.Shards[i] = 0
			}
		} else {
			if ref <= avgShardPerGid {
				gidMapRef[gid] = ref
			} else {
				config.Shards[i] = 0
			}
		}
	}
	DPrintf(redFormat+"(middle)role: %v, ref: %v, Shards: %v"+defaultFormat,
		sc.me, gidMapRef, config.Shards)
	smallRootHeap := make([]*HeapNode, 0)
	for gid, ref := range gidMapRef {
		heapNode := new(HeapNode)
		heapNode.gid = gid
		heapNode.ref = ref
		smallRootHeap = append(smallRootHeap, heapNode)
	}
	buildSmallRootHeap(smallRootHeap)


	for i := 0; i < NShards; i++ {
		if config.Shards[i] != 0 {
			continue
		}
		gid := rootAddOneAndReturnGid(smallRootHeap)
		config.Shards[i] = gid
	}
}

func (sc *ShardCtrler) applyJoin(apply *raft.ApplyMsg) {
	joinArgs := apply.Command.(Op).Args.(JoinArgs)

	cur_config_index := len(sc.configs)
	cur_config := copyConfig(&sc.configs[cur_config_index - 1])

	for gid, servers := range joinArgs.Servers {
		cur_config.Groups[gid] = servers
	}

	sc.configs = append(sc.configs, cur_config)
	sc.adjustShards(&sc.configs[cur_config_index])
	DPrintf(redFormat+"role: %v, Groups: %v, Shards: %v"+defaultFormat,
		sc.me, sc.configs[cur_config_index].Groups, sc.configs[cur_config_index].Shards)
}

func (sc *ShardCtrler) applyLeave(apply *raft.ApplyMsg) {
	leaveArgs := apply.Command.(Op).Args.(LeaveArgs)

	cur_config_index := len(sc.configs)
	cur_config := copyConfig(&sc.configs[cur_config_index - 1])
	
	for _, gid := range leaveArgs.GIDs {
		delete(cur_config.Groups, gid)
	}

	sc.configs = append(sc.configs, cur_config)
	sc.adjustShards(&sc.configs[cur_config_index])
}

func (sc *ShardCtrler) applyMove(apply *raft.ApplyMsg) {
	moveArgs := apply.Command.(Op).Args.(MoveArgs)

	cur_config_index := len(sc.configs)
	cur_config := copyConfig(&sc.configs[cur_config_index - 1])

	cur_config.Shards[moveArgs.Shard] = moveArgs.GID

	sc.configs = append(sc.configs, cur_config)
}

func (sc *ShardCtrler) queryConfig(apply *raft.ApplyMsg) int {
	queryArgs := apply.Command.(Op).Args.(QueryArgs)
	config_index := queryArgs.Num
	if config_index == -1 {
		config_index = len(sc.configs) - 1
	}

	return config_index
}