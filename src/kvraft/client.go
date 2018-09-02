package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
)

const RetryInterval = time.Duration(10 * time.Millisecond)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	clientId      int64
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
	ck.clientId = nrand()
	ck.lastRequestId = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	requestId := time.Now().UnixNano() - ck.clientId
	args := GetArgs{requestId, ck.lastRequestId, key}
	ck.lastRequestId = requestId
	for {
		var reply GetReply
		// DPrintf("[%d GET %s]", ck.leaderId, args.Key)
		if ck.Call("KVServer.Get", &args, &reply) && reply.Err == OK {
			DPrintf("[%d GET key %s reply %#v]", ck.leaderId, args.Key, reply)
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := time.Now().UnixNano() - ck.clientId
	args := PutAppendArgs{requestId, ck.lastRequestId, key, value, op}
	ck.lastRequestId = requestId
	for {
		var reply PutAppendReply
		if ck.Call("KVServer.PutAppend", &args, &reply) && reply.Err == OK {
			if op == "Put" {
				DPrintf("[%d PUT key %s value %s id %d reply %v]", ck.leaderId, args.Key, args.Value, args.RequestId, reply)
			} else {
				DPrintf("[%d APPEND key %s value %s id %d reply %v]", ck.leaderId, args.Key, args.Value, args.RequestId, reply)
			}
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
