package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers       []*labrpc.ClientEnd
	lastRequestId int64
	leaderId      int
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
	ck.lastRequestId = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num}
loop:
	var reply QueryReply
	if ck.Call("ShardMaster.Query", &args, &reply) {
		switch reply.Err {
		case WrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			goto loop
		case OK:
			break
		}
	} else {
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		goto loop
	}
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	requestId := time.Now().UnixNano()
	args := JoinArgs{RequestId: requestId, ExpireRequestId: ck.lastRequestId, Servers: servers}
	ck.lastRequestId = requestId
loop:
	var reply JoinReply
	if ck.Call("ShardMaster.Join", &args, &reply) {
		switch reply.Err {
		case WrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			goto loop
		case OK:
			return
		}
	} else {
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		goto loop
	}
}

func (ck *Clerk) Leave(gids []int) {
	requestId := time.Now().UnixNano()
	args := LeaveArgs{RequestId: requestId, ExpireRequestId: ck.lastRequestId, GIDs: gids}
	ck.lastRequestId = requestId
loop:
	var reply LeaveReply
	if ck.Call("ShardMaster.Leave", &args, &reply) {
		switch reply.Err {
		case WrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			goto loop
		case OK:
			return
		}
	} else {
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		goto loop
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	requestId := time.Now().UnixNano()
	args := MoveArgs{RequestId: requestId, ExpireRequestId: ck.lastRequestId, Shard: shard, GID: gid}
	ck.lastRequestId = requestId

loop:
	var reply MoveReply
	if ck.Call("ShardMaster.Move", &args, &reply) {
		switch reply.Err {
		case WrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			goto loop
		case OK:
			return
		}
	} else {
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		goto loop
	}
}
