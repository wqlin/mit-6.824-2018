package shardkv

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
	RequestId       int64
	ExpireRequestId int64
	ConfigNum       int
	Key             string
}

type GetReply struct {
	Err   Err
	Value string
}

func (arg *GetArgs) copy() GetArgs {
	return GetArgs{arg.RequestId, arg.ExpireRequestId, arg.ConfigNum, arg.Key}
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{arg.RequestId, arg.ExpireRequestId, arg.ConfigNum, arg.Key, arg.Value, arg.Op}
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
	return ShardCleanupArgs{arg.Shard, arg.ConfigNum}
}

type ShardCleanupReply struct {
	Err Err
}
