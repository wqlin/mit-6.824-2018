package raftkv

import "log"

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type PutAppendArgs struct {
	ClientId   int64
	RequestSeq int
	Key        string
	Value      string
	Op         string // "Put" or "Append"
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{arg.ClientId, arg.RequestSeq, arg.Key, arg.Value, arg.Op}
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

func (arg *GetArgs) copy() GetArgs {
	return GetArgs{arg.Key}
}

type GetReply struct {
	Err   Err
	Value string
}
