package shardkv

import "log"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type RequestType int

type IntSet map[int]struct{}
type StringSet map[string]struct{}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Put or Append
type PutAppendArgs struct {
	RequestId       int64
	ExpireRequestId int64
	ConfigNum       int
	Key             string
	Value           string
	Op              string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ConfigNum int
	Key       string
}

type GetReply struct {
	Err   Err
	Value string
}

func (arg *GetArgs) copy() GetArgs {
	return GetArgs{ConfigNum: arg.ConfigNum, Key: arg.Key}
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{RequestId: arg.RequestId, ExpireRequestId: arg.ExpireRequestId, ConfigNum: arg.ConfigNum, Key: arg.Key, Value: arg.Value, Op: arg.Op}
}

type ShardMigrationArgs struct {
	Shard     int
	ConfigNum int
}

type MigrationData struct {
	Data  map[string]string
	Cache map[int64]string
}

type ShardMigrationReply struct {
	Err           Err
	Shard         int
	ConfigNum     int
	MigrationData MigrationData
}

type ShardCleanupArgs struct {
	Shard     int
	ConfigNum int
}

func (arg *ShardCleanupArgs) copy() ShardCleanupArgs {
	return ShardCleanupArgs{Shard: arg.Shard, ConfigNum: arg.ConfigNum}
}

type ShardCleanupReply struct {
	Err Err
}
