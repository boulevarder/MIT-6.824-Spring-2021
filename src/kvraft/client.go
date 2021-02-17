package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader	int
	mu 		sync.Mutex
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
	server := ck.leader
	ck.mu.Unlock()

	for {
		args := GetArgs {
			Key		: key,
		}
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)

		if ok {
			switch(reply.Err) {
			case OK:

				ck.mu.Lock()
				ck.leader = server
				ck.mu.Unlock()

				return reply.Value
			case ErrNoKey:
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
	randNum := nrand()
	ck.mu.Lock()
	server := ck.leader
	ck.mu.Unlock()

	for {
		args := PutAppendArgs {
			Key		: key,
			Value	: value,
			Op 		: op,
			RandNum	: randNum,
		}
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch(reply.Err) {
			case OK:
				ck.mu.Lock()
				ck.leader = server
				ck.mu.Unlock()
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
