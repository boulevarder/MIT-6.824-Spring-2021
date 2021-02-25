package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

var clerk_lock sync.Mutex
var clerk_index int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	mu			sync.Mutex
	client_id	int
	command_id	int	
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
	// Your code here.

	clerk_lock.Lock()
	client_id := clerk_index
	clerk_index++
	clerk_lock.Unlock()
	
	ck.client_id = client_id
	ck.command_id = 1

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	ck.mu.Lock()
	args.Client_id = ck.client_id
	args.Command_id = ck.command_id
	ck.command_id++
	ck.mu.Unlock()
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	ck.mu.Lock()
	args.Client_id = ck.client_id
	args.Command_id = ck.command_id
	ck.command_id++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	ck.mu.Lock()
	args.Client_id = ck.client_id
	args.Command_id = ck.command_id
	ck.command_id++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	ck.mu.Lock()
	args.Client_id = ck.client_id
	args.Command_id = ck.command_id
	ck.command_id++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
