package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
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
loop:
	var reply GetReply
	if ck.Call("KVServer.Get", &args, &reply) {
		switch reply.Err {
		case WrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			goto loop
		case OK:
			break
		case ErrNoKey:
			break
		}
	} else {
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers) // retry with different server
		goto loop
	}
	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := time.Now().UnixNano()
	args := PutAppendArgs{requestId, ck.lastRequestId, key, value, op}
	ck.lastRequestId = requestId
loop:
	var reply PutAppendReply
	if ck.Call("KVServer.PutAppend", &args, &reply) {
		switch reply.Err {
		case WrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			goto loop
		case OK:
			break
		}
	} else {
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers) // retry with different server
		goto loop
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
