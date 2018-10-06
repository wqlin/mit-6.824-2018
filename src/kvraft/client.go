package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
)

const RetryInterval = time.Duration(125 * time.Millisecond)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	clientId   int64
	RequestSeq int
	leaderId   int
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
	ck.RequestSeq = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{key}
	for {
		var reply GetReply
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
	ck.RequestSeq ++
	args := PutAppendArgs{ck.clientId, ck.RequestSeq, key, value, op}
	for {
		var reply PutAppendReply
		if ck.Call("KVServer.PutAppend", &args, &reply) && reply.Err == OK {
			if op == "Put" {
				DPrintf("[%d seq %d PUT key %s value %s reply %v]", ck.leaderId, args.RequestSeq, args.Key, args.Value, reply)
			} else {
				DPrintf("[%d seq %d APPEND key %s value %s reply %v]", ck.leaderId, args.RequestSeq, args.Key, args.Value, reply)
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
