package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client_id	int
	Command_id	int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client_id	int
	Command_id	int
}

type GetReply struct {
	Err   Err
	Value string
}

var (
	redFormat		string = "\033[35m"
	whiteFormat		string = "\033[37m"
	blueFormat		string = "\033[1;34m"
	warnFormat		string = "\033[1;33m"
	redLightFormat	string = "\033[1;35m"
	defaultFormat	string = "\033[0m"
)