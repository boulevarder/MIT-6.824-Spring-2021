package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "sync"

var clerk_lock sync.Mutex
var clerk_index int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu			sync.Mutex
	client_id 	int
	command_id	int
	leader		int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	clerk_lock.Lock()
	client_id := clerk_index
	clerk_index++
	clerk_lock.Unlock()
	ck.client_id = client_id
	ck.command_id = 1

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	command_id := ck.command_id
	ck.command_id++
	server := ck.leader
	ck.mu.Unlock()

	for {
		args := GetArgs{
			Key : key,
			Client_id : ck.client_id,
			Command_id : command_id,
		}
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)

		if ok {
			switch(reply.Err) {
			case OK:
				ck.setLeader(server)
				return reply.Value
			case ErrNoKey:
				ck.setLeader(server)
				return ""
			case ErrWrongLeader:
				server++
			}
		} else {
			server++
		}

		if server == len(ck.servers) {
			server = 0
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	command_id := ck.command_id
	ck.command_id++
	server := ck.leader
	ck.mu.Unlock()

	for {
		args := PutAppendArgs {
			Key		: key,
			Value	: value,
			Op		: op,
			Client_id	: ck.client_id,
			Command_id	: command_id,
		}
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch(reply.Err) {
			case OK:
				ck.setLeader(server)
				return
			case ErrWrongLeader:
				server++
			}
		} else {
			server++
		}

		if server == len(ck.servers) {
			server = 0
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) setLeader(server int) {
	ck.mu.Lock()
	ck.leader = server
	ck.mu.Unlock()
}