package shardmaster

import "log"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10
const (
	OK          = "OK"
	WrongLeader = "WrongLeader"
)

type Err string

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config Config) Copy() Config {
	newConfig := Config{Num: config.Num, Shards: config.Shards, Groups: make(map[int][]string)}
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	return newConfig
}

type JoinArgs struct {
	ClientId   int64
	RequestSeq int
	Servers    map[int][]string // new GID -> servers mappings
}

func (arg *JoinArgs) copy() JoinArgs {
	result := JoinArgs{arg.ClientId, arg.RequestSeq, make(map[int][]string)}
	for gid, server := range arg.Servers {
		result.Servers[gid] = append([]string{}, server...)
	}
	return result
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	ClientId   int64
	RequestSeq int
	GIDs       []int
}

func (arg *LeaveArgs) copy() LeaveArgs {
	return LeaveArgs{arg.ClientId, arg.RequestSeq, append([]int{}, arg.GIDs...)}
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	ClientId   int64
	RequestSeq int
	Shard      int
	GID        int
}

func (arg *MoveArgs) copy() MoveArgs {
	return MoveArgs{arg.ClientId, arg.RequestSeq, arg.Shard, arg.GID}
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num int
}

func (arg *QueryArgs) copy() QueryArgs {
	return QueryArgs{arg.Num}
}

type QueryReply struct {
	Err    Err
	Config Config
}
