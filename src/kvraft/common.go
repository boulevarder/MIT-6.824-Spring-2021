package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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