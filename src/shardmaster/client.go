package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

const RetryInterval = time.Duration(50 * time.Millisecond)

type Clerk struct {
	mu         sync.Mutex
	servers    []*labrpc.ClientEnd
	clientId   int64
	requestSeq int
	leaderId   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// retry until RPC call success
func (ck *Clerk) Call(rpcname string, args interface{}, reply interface{}) bool {
	return ck.servers[ck.leaderId].Call(rpcname, args, reply)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.requestSeq = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) getNextRequestSeq() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestSeq ++
	return ck.requestSeq
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num}
	for {
		var reply QueryReply
		if ck.Call("ShardMaster.Query", &args, &reply) && reply.Err == OK {
			return reply.Config
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
	return Config{}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{ClientId: ck.clientId, RequestSeq: ck.getNextRequestSeq(), Servers: servers}
	for {
		var reply JoinReply
		if ck.Call("ShardMaster.Join", &args, &reply) && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{ClientId: ck.clientId, RequestSeq: ck.getNextRequestSeq(), GIDs: gids}
	for {
		var reply LeaveReply
		if ck.Call("ShardMaster.Leave", &args, &reply) && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{ClientId: ck.clientId, RequestSeq: ck.getNextRequestSeq(), Shard: shard, GID: gid}
	for {
		var reply MoveReply
		if ck.Call("ShardMaster.Move", &args, &reply) && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}
