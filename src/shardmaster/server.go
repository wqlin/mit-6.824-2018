package shardmaster

import "raft"
import "labrpc"
import "sync"
import (
	"labgob"
	"log"
	"time"
)

func init() {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type ShardMaster struct {
	mu            sync.Mutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	shutdown      chan struct{}
	configs       []Config                // indexed by config num
	cache         map[int64]int           // cache processed request, detect duplicate request
	notifyChanMap map[int]chan notifyArgs // notify RPC handler
}

type notifyArgs struct {
	Term int
	Args interface{}
}

func (sm *ShardMaster) getConfig(i int) Config {
	var srcConfig Config
	if i < 0 || i >= len(sm.configs) {
		srcConfig = sm.configs[len(sm.configs)-1]
	} else {
		srcConfig = sm.configs[i]
	}
	dstConfig := Config{Num: srcConfig.Num, Shards: srcConfig.Shards, Groups: make(map[int][]string)}
	for gid, servers := range srcConfig.Groups {
		dstConfig.Groups[gid] = append([]string{}, servers...)
	}
	return dstConfig
}

func (sm *ShardMaster) notifyIfPresent(index int, reply notifyArgs) {
	if ch, ok := sm.notifyChanMap[index]; ok {
		ch <- reply
		delete(sm.notifyChanMap, index)
	}
}

func (sm *ShardMaster) start(arg interface{}) (Err, interface{}) {
	index, term, ok := sm.rf.Start(arg)
	if !ok {
		return WrongLeader, struct{}{}
	}
	sm.mu.Lock()
	notifyCh := make(chan notifyArgs, 1)
	sm.notifyChanMap[index] = notifyCh
	sm.mu.Unlock()
	select {
	case <-time.After(3 * time.Second):
		sm.mu.Lock()
		delete(sm.notifyChanMap, index)
		sm.mu.Unlock()
		return WrongLeader, struct{}{}
	case result := <-notifyCh:
		if result.Term != term {
			return WrongLeader, struct{}{}
		} else {
			return OK, result.Args
		}
	}
	return OK, struct{}{}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err, reply.Config = OK, sm.getConfig(args.Num)
		return
	} else { // args.Num <= 0
		sm.mu.Unlock()
		err, config := sm.start(args.copy())
		reply.Err = err
		if err == OK {
			reply.Config = config.(Config)
		}
		sm.mu.Lock()
	}
}

func (sm *ShardMaster) appendNewConfig(newConfig Config) {
	newConfig.Num = len(sm.configs)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) apply(msg raft.ApplyMsg) {
	reply := notifyArgs{Term: msg.CommandTerm, Args: ""}
	if arg, ok := msg.Command.(JoinArgs); ok {
		if sm.cache[arg.ClientId] < arg.RequestSeq {
			newConfig := sm.getConfig(-1) // create new configuration based on last configuration
			newGIDS := make([]int, 0)
			for gid, server := range arg.Servers {
				if s, ok := newConfig.Groups[gid]; ok {
					newConfig.Groups[gid] = append(s, server...)
				} else {
					newConfig.Groups[gid] = append([]string{}, server...)
					newGIDS = append(newGIDS, gid)
				}
			}
			if len(newConfig.Groups) == 0 { // no replica group exists
				newConfig.Shards = [NShards]int{}
			} else if len(newConfig.Groups) <= NShards {
				shardsByGID := make(map[int]int) // gid -> number of shards it owns
				var minShardsPerGID, maxShardsPerGID, maxShardsPerGIDCount int
				minShardsPerGID = NShards / len(newConfig.Groups)
				if maxShardsPerGIDCount = NShards % len(newConfig.Groups); maxShardsPerGIDCount != 0 {
					maxShardsPerGID = minShardsPerGID + 1
				} else {
					maxShardsPerGID = minShardsPerGID
				}
				// divide shards as evenly as possible among replica groups and move as few shards as possible
				for i, j := 0, 0; i < NShards; i++ {
					gid := newConfig.Shards[i]
					if gid == 0 ||
						(minShardsPerGID == maxShardsPerGID && shardsByGID[gid] == minShardsPerGID) ||
						(minShardsPerGID < maxShardsPerGID && shardsByGID[gid] == minShardsPerGID && maxShardsPerGIDCount <= 0) {
						newGID := newGIDS[j]
						newConfig.Shards[i] = newGID
						shardsByGID[newGID] += 1
						j = (j + 1) % len(newGIDS)
					} else {
						shardsByGID[gid] += 1
						if shardsByGID[gid] == minShardsPerGID {
							maxShardsPerGIDCount -= 1
						}
					}
				}
			}
			DPrintf("[Join] ShardMaster %d append new Join config, newGIDS: %v, config: %v", sm.me, newGIDS, newConfig)
			sm.cache[arg.ClientId] = arg.RequestSeq
			sm.appendNewConfig(newConfig)
		}
	} else if arg, ok := msg.Command.(LeaveArgs); ok {
		if sm.cache[arg.ClientId] < arg.RequestSeq {
			newConfig := sm.getConfig(-1) // create new configuration
			leaveGIDs := make(map[int]struct{})
			for _, gid := range arg.GIDs {
				delete(newConfig.Groups, gid)
				leaveGIDs[gid] = struct{}{}
			}
			if len(newConfig.Groups) == 0 { // remove all gid
				newConfig.Shards = [NShards]int{}
			} else {
				remainingGIDs := make([]int, 0)
				for gid := range newConfig.Groups {
					remainingGIDs = append(remainingGIDs, gid)
				}
				shardsPerGID := NShards / len(newConfig.Groups) // NShards / total number of gid
				if shardsPerGID < 1 {
					shardsPerGID = 1
				}
				shardsByGID := make(map[int]int)
			loop:
				for i, j := 0, 0; i < NShards; i++ {
					gid := newConfig.Shards[i]
					if _, ok := leaveGIDs[gid]; ok || shardsByGID[gid] == shardsPerGID {
						for _, id := range remainingGIDs {
							count := shardsByGID[id]
							if count < shardsPerGID {
								newConfig.Shards[i] = id
								shardsByGID[id] += 1
								continue loop
							}
						}
						id := remainingGIDs[j]
						j = (j + 1) % len(remainingGIDs)
						newConfig.Shards[i] = id
						shardsByGID[id] += 1
					} else {
						shardsByGID[gid] += 1
					}
				}
				DPrintf("[Leave] ShardMaster %d append new Leave config, shardsPerGID: %d, remainingGIDS: %v, config: %v", sm.me, shardsPerGID, remainingGIDs, newConfig)
			}
			sm.cache[arg.ClientId] = arg.RequestSeq
			sm.appendNewConfig(newConfig)
		}
	} else if arg, ok := msg.Command.(MoveArgs); ok {
		if sm.cache[arg.ClientId] < arg.RequestSeq {
			newConfig := sm.getConfig(-1)
			newConfig.Shards[arg.Shard] = arg.GID
			DPrintf("[Move] ShardMaster %d append new Move config: %#v", sm.me, newConfig)
			sm.cache[arg.ClientId] = arg.RequestSeq
			sm.appendNewConfig(newConfig)
		}
	} else if arg, ok := msg.Command.(QueryArgs); ok {
		reply.Args = sm.getConfig(arg.Num)
	}
	sm.notifyIfPresent(msg.CommandIndex, reply)
}

func (sm *ShardMaster) run() {
	go sm.rf.Replay(1)
	for {
		select {
		case msg := <-sm.applyCh:
			sm.mu.Lock()
			if msg.CommandValid {
				sm.apply(msg)
			} else if cmd, ok := msg.Command.(string); ok && cmd == "NewLeader" {
				sm.rf.Start("")
			}
			sm.mu.Unlock()
		case <-sm.shutdown:
			return
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	// First configuration is numbered zero, it contains no groups,
	// and shards should be assigned to GID zero
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan raft.ApplyMsg, 1000)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.cache = make(map[int64]int)
	sm.notifyChanMap = make(map[int]chan notifyArgs)
	sm.shutdown = make(chan struct{})
	go sm.run()
	return sm
}
