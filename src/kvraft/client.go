package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
	"raft"
)

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

func (ck *Clerk) Get(key string) string {
	requestId := time.Now().UnixNano()
	args := GetArgs{requestId, ck.lastRequestId, key}
	ck.lastRequestId = requestId
	oldLeaderID := ck.leaderId
	for {
		var reply GetReply
		DPrintf("[%d GET %s id %d]", ck.leaderId, args.Key, args.RequestId)
		if ck.Call("KVServer.Get", &args, &reply) {
			DPrintf("[%d GET %s id %d reply %#v]", ck.leaderId, args.Key, args.RequestId, reply)
			if reply.Err == OK {
				return reply.Value
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		if ck.leaderId == oldLeaderID {
			time.Sleep(raft.ElectionTimeout)
		}
	}
	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := time.Now().UnixNano()
	args := PutAppendArgs{requestId, ck.lastRequestId, key, value, op}
	ck.lastRequestId = requestId
	oldLeaderID := ck.leaderId
	for {
		var reply PutAppendReply
		if op == "Put" {
			DPrintf("[%d PUT %s value %s id %d]", ck.leaderId, args.Key, args.Value, args.RequestId)
		} else {
			DPrintf("[%d APPEND %s value %s id %d]", ck.leaderId, args.Key, args.Value, args.RequestId)
		}
		if ck.Call("KVServer.PutAppend", &args, &reply) {
			if op == "Put" {
				DPrintf("[%d PUT %s value %s id %d reply %#v]", ck.leaderId, args.Key, args.Value, args.RequestId, reply)
			} else {
				DPrintf("[%d APPEND %s value %s id %d reply %#v]", ck.leaderId, args.Key, args.Value, args.RequestId, reply)
			}
			if reply.Err == OK {
				return
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		if ck.leaderId == oldLeaderID {
			time.Sleep(raft.ElectionTimeout)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
