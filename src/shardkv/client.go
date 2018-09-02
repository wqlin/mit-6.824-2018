package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import (
	"time"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm            *shardmaster.Clerk
	config        shardmaster.Config
	make_end      func(string) *labrpc.ClientEnd
	clientId      int64
	lastRequestId int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.lastRequestId = 0
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	requestId := time.Now().UnixNano() - ck.clientId
	args := GetArgs{RequestId: requestId, ExpireRequestId: ck.lastRequestId, ConfigNum: ck.config.Num, Key: key}
	ck.lastRequestId = requestId
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfigNum = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
		loop:
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				// DPrintf("%d [GET %s id %d from %d-%d] shard %d config %d", runtime.NumGoroutine(), args.Key, args.RequestId, gid, si, shard, ck.config.Num)
				if srv.Call("ShardKV.Get", &args, &reply) {
					if reply.Err == OK {
						// DPrintf("%d [GET SUCCESS %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.RequestId, gid, si, shard, ck.config.Num, reply)
						return reply.Value
					} else if reply.Err == ErrWrongGroup {
						break loop
						// DPrintf("%d [GET Wrong Group %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.RequestId, gid, si, shard, ck.config.Num, reply)
					} else {
						// DPrintf("%d [GET Wrong Leader %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.RequestId, gid, si, shard, ck.config.Num, reply)
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(ck.config.Num + 1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := time.Now().UnixNano() - ck.clientId
	args := PutAppendArgs{RequestId: requestId, ExpireRequestId: ck.lastRequestId, ConfigNum: ck.config.Num, Key: key, Value: value, Op: op}
	ck.lastRequestId = requestId
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfigNum = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
		loop:
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				//if op == "Put" {
				//	DPrintf("%d [PUT %s %s id %d from %d-%d] shard %d config %d", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num)
				//} else {
				//	DPrintf("%d [APPEND %s %s id %d from %d-%d] shard %d config %d", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num)
				//}
				if srv.Call("ShardKV.PutAppend", &args, &reply) {
					if reply.Err == OK {
						//if op == "Put" {
						//	DPrintf("%d [PUT SUCCESS %s value %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num, reply)
						//} else {
						//	DPrintf("%d [APPEND SUCCESS %s value %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num, reply)
						//}
						return
					} else if reply.Err == ErrWrongGroup {
						//if op == "Put" {
						//	DPrintf("%d [PUT Wrong Group %s value %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num, reply)
						//} else {
						//	DPrintf("%d [APPEND Wrong Group %s value %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num, reply)
						//}
						break loop
					} else {
						//if op == "Put" {
						//	DPrintf("%d [PUT WRONG Leader %s value %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num, reply)
						//} else {
						//	DPrintf("%d [APPEND WRONG Leader %s value %s id %d from %d-%d] shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num, reply)
						//}
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
