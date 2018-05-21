package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"labgob"
	"log"
	"time"
	"bytes"
	"runtime"
	_ "net/http/pprof"
)

const Debug = 0
const PollingInterval = time.Millisecond * time.Duration(80)

func init() {
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(ShardMigrationArgs{})
	labgob.Register(ShardMigrationReply{})
	labgob.Register(ShardCleanupArgs{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(MigrationData{})
	labgob.Register(Op{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type RequestType
	Args interface{}
}

type notifyArgs struct {
	Term  int
	Value string
	Err   Err
}

// make a copy of config
func copyConfig(config shardmaster.Config) shardmaster.Config {
	newConfig := shardmaster.Config{Num: config.Num, Shards: config.Shards, Groups: make(map[int][]string)}
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	return newConfig
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	rf           *raft.Raft

	make_end func(string) *labrpc.ClientEnd
	gid      int
	masters  []*labrpc.ClientEnd
	mck      *shardmaster.Clerk
	config   shardmaster.Config // store the latest configuration
	shutdown chan struct{}

	ownShards       IntSet                        // shards that currently owned by server at current configuration
	migratingShards map[int]map[int]MigrationData // config number -> shard and migration data
	waitingShards   map[int]int                   // shards -> config num, waiting to migrate from other group
	cleaningShards  map[int]IntSet                // config number -> shards
	historyConfigs  []shardmaster.Config          // store history configs, so that we don't need to query shard master

	data          map[string]string
	cache         map[int64]string // key -> id of request, value -> key of data
	notifyChanMap map[int]chan notifyArgs
}

func (kv *ShardKV) notifyIfPresent(index int, reply notifyArgs) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		ch <- reply
		delete(kv.notifyChanMap, index)
	}
}

func (kv *ShardKV) snapshot(lastCommandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.ownShards)
	e.Encode(kv.migratingShards)
	e.Encode(kv.waitingShards)
	e.Encode(kv.cleaningShards)
	e.Encode(kv.historyConfigs)
	e.Encode(kv.config)
	e.Encode(kv.cache)
	e.Encode(kv.data)

	snapshot := w.Bytes()
	kv.rf.PersistAndSaveSnapshot(lastCommandIndex, snapshot)
}

func (kv *ShardKV) snapshotIfNeeded(lastCommandIndex int) {
	var threshold = int(1.5 * float64(kv.maxraftstate))
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= threshold {
		kv.snapshot(lastCommandIndex)
	}
}

func (kv *ShardKV) readSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var config shardmaster.Config
	ownShards, migratingShards, waitingShards, cleaningShards := make(IntSet), make(map[int]map[int]MigrationData), make(map[int]int), make(map[int]IntSet)
	historyConfigs, cache, data := make([]shardmaster.Config, 0), make(map[int64]string), make(map[string]string)

	if d.Decode(&ownShards) != nil ||
		d.Decode(&migratingShards) != nil ||
		d.Decode(&waitingShards) != nil ||
		d.Decode(&cleaningShards) != nil ||
		d.Decode(&historyConfigs) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&cache) != nil ||
		d.Decode(&data) != nil {
		log.Fatal("Error in reading snapshot")
	}
	kv.config = config
	kv.ownShards, kv.migratingShards, kv.waitingShards, kv.cleaningShards = ownShards, migratingShards, waitingShards, cleaningShards
	kv.historyConfigs, kv.cache, kv.data = historyConfigs, cache, data
}

func (kv *ShardKV) getCurrentConfigNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num
}

// string is used in get
func (kv *ShardKV) startOp(op Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index, term, ok := kv.rf.Start(op)
	if !ok {
		return ErrWrongLeader, ""
	}
	notifyCh := make(chan notifyArgs, 1)
	kv.notifyChanMap[index] = notifyCh
	kv.mu.Unlock()
	select {
	case <-time.After(3 * time.Second):
		kv.mu.Lock()
		delete(kv.notifyChanMap, index)
		return ErrWrongLeader, ""
	case result := <-notifyCh:
		kv.mu.Lock()
		if result.Term != term {
			return ErrWrongLeader, ""
		} else {
			return result.Err, result.Value
		}
	}
	return OK, ""
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{Type: Get,
		Args: GetArgs{RequestId: args.RequestId, ExpireRequestId: args.ExpireRequestId,
			ConfigNum: args.ConfigNum, Key: args.Key}}
	err, value := kv.startOp(op)
	reply.Err, reply.Value = err, value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Type: PutAppend,
		Args: PutAppendArgs{RequestId: args.RequestId, ExpireRequestId: args.ExpireRequestId,
			ConfigNum: args.ConfigNum, Key: args.Key, Value: args.Value, Op: args.Op}}
	err, _ := kv.startOp(op)
	reply.Err = err
}

// shard migration RPC handler
func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err, reply.Shard, reply.ConfigNum = OK, args.Shard, args.ConfigNum
	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	reply.MigrationData = MigrationData{Data: make(map[string]string), Cache: make(map[int64]string)}
	if v, ok := kv.migratingShards[args.ConfigNum]; ok {
		if migrationData, ok := v[args.Shard]; ok {
			for k, v := range migrationData.Data {
				reply.MigrationData.Data[k] = v
			}
			for k, v := range migrationData.Cache {
				reply.MigrationData.Cache[k] = v
			}
		}
	}
}

// pull shard from other group
func (kv *ShardKV) makeShardMigrationCall(shard int, oldConfigNum int) {
	kv.mu.Lock()
	oldConfig := kv.historyConfigs[oldConfigNum]
	kv.mu.Unlock()
	configNum := oldConfig.Num
	gid := oldConfig.Shards[shard]
	servers := oldConfig.Groups[gid]
	args := ShardMigrationArgs{Shard: shard, ConfigNum: configNum}

	for si, server := range servers {
		srv := kv.make_end(server)
		var reply ShardMigrationReply

		if srv.Call("ShardKV.ShardMigration", &args, &reply) {
			if reply.Err == OK {
				DPrintf("%d %d-%d pull shard %d at %d from %d-%d SUCCESS", runtime.NumGoroutine(), kv.gid, kv.me, shard, configNum, gid, si)
				go kv.startOp(Op{Type: AddWaitingShard, Args: reply})
				return
			}
		}
	}
}

func (kv *ShardKV) ShardCleanup(args *ShardCleanupArgs, reply *ShardCleanupReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.migratingShards[args.ConfigNum]; ok {
		if _, ok := kv.migratingShards[args.ConfigNum][args.Shard]; ok {
			kv.mu.Unlock()
			result, _ := kv.startOp(Op{Type: MigratingShardCleanup, Args: ShardCleanupArgs{Shard: args.Shard, ConfigNum: args.ConfigNum}})
			reply.Err = result
			kv.mu.Lock()
		}
	}
}

func (kv *ShardKV) deleteCleaningShard(shard int, configNum int) {
	kv.mu.Lock()
	if _, ok := kv.cleaningShards[configNum]; ok {
		delete(kv.cleaningShards[configNum], shard)
		if len(kv.cleaningShards[configNum]) == 0 {
			delete(kv.cleaningShards, configNum)
			DPrintf("%d %d-%d clean shard %d at %d success, cleaningShards %v",
				runtime.NumGoroutine(), kv.gid, kv.me, shard, configNum, kv.cleaningShards)
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) makeShardCleanupCall(shard int, configNum int) {
	args := ShardCleanupArgs{Shard: shard, ConfigNum: configNum}
	kv.mu.Lock()
	config := kv.historyConfigs[configNum]
	kv.mu.Unlock()
	gid := config.Shards[shard]
	servers := config.Groups[gid]
	for {
		select {
		case <-kv.shutdown:
			return
		default:
			for _, server := range servers {
				srv := kv.make_end(server)
				var reply ShardCleanupReply
				if srv.Call("ShardKV.ShardCleanup", &args, &reply) {
					if reply.Err == OK {
						kv.deleteCleaningShard(shard, configNum)
						return
					}
				}
			}
		}
	}
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.shutdown)
}

func (kv *ShardKV) handleValidCommand(msg raft.ApplyMsg, isReplay bool) {
	result := notifyArgs{Term: msg.CommandTerm, Value: "", Err: OK}
	if cmd, ok := msg.Command.(Op); ok {
		switch cmd.Type {
		case Get:
			args := cmd.Args.(GetArgs)
			shard := key2shard(args.Key)
			if args.ConfigNum != kv.config.Num {
				result.Err = ErrWrongGroup
			} else if _, ok := kv.ownShards[shard]; !ok {
				result.Err = ErrWrongGroup
			} else {
				result.Value = kv.data[args.Key]
			}
		case PutAppend:
			args := cmd.Args.(PutAppendArgs)
			shard := key2shard(args.Key)
			if args.ConfigNum != kv.config.Num {
				result.Err = ErrWrongGroup
			} else if _, ok := kv.ownShards[shard]; !ok {
				result.Err = ErrWrongGroup
			} else if _, ok := kv.cache[args.RequestId]; !ok {
				if args.Op == "Put" {
					kv.data[args.Key] = args.Value
				} else {
					kv.data[args.Key] += args.Value
				}
				delete(kv.cache, args.ExpireRequestId)
				kv.cache[args.RequestId] = args.Key
			}
		case Reconfiguration:
			kv.handleReconfiguration(msg, isReplay)
		case AddWaitingShard:
			args := cmd.Args.(ShardMigrationReply)
			if args.ConfigNum == kv.config.Num-1 {
				delete(kv.waitingShards, args.Shard)
				if _, ok := kv.ownShards[args.Shard]; !ok {
					if _, ok := kv.cleaningShards[args.ConfigNum]; !ok {
						kv.cleaningShards[args.ConfigNum] = make(IntSet)
					}
					kv.cleaningShards[args.ConfigNum][args.Shard] = struct{}{}
					kv.ownShards[args.Shard] = struct{}{}
					for k, v := range args.MigrationData.Data {
						kv.data[k] = v
					}
					for k, v := range args.MigrationData.Cache {
						kv.cache[k] = v
					}
					go kv.makeShardCleanupCall(args.Shard, args.ConfigNum)
				}
			}
		case MigratingShardCleanup:
			args := cmd.Args.(ShardCleanupArgs)
			if _, ok := kv.migratingShards[args.ConfigNum]; ok {
				delete(kv.migratingShards[args.ConfigNum], args.Shard)
				if len(kv.migratingShards[args.ConfigNum]) == 0 {
					delete(kv.migratingShards, args.ConfigNum)
				}
			}
		}
		kv.snapshotIfNeeded(msg.CommandIndex)
		kv.notifyIfPresent(msg.CommandIndex, result)
	}
}

func (kv *ShardKV) handleReconfiguration(msg raft.ApplyMsg, isReplay bool) {
	newConfig := msg.Command.(Op).Args.(shardmaster.Config)
	if newConfig.Num > kv.config.Num {
		oldConfig, oldShards := kv.config, kv.ownShards
		kv.historyConfigs = append(kv.historyConfigs, copyConfig(oldConfig))
		kv.ownShards, kv.config = make(IntSet), copyConfig(newConfig)
		for shard, newGID := range newConfig.Shards {
			if newGID == kv.gid {
				if _, ok := oldShards[shard]; ok || oldConfig.Num == 0 {
					kv.ownShards[shard] = struct{}{}
					delete(oldShards, shard)
				} else {
					kv.waitingShards[shard] = oldConfig.Num
				}
			}
		}
		if len(oldShards) != 0 {
			v := make(map[int]MigrationData)
			for shard := range oldShards {
				data := MigrationData{Data: make(map[string]string), Cache: make(map[int64]string)}
				for k, v := range kv.data {
					if key2shard(k) == shard {
						data.Data[k] = v
						delete(kv.data, k)
					}
				}
				for k, v := range kv.cache {
					if key2shard(v) == shard {
						data.Cache[k] = v
						delete(kv.cache, k)
					}
				}
				v[shard] = data
			}
			kv.migratingShards[oldConfig.Num] = v
		}
		if !isReplay {
			for shard, config := range kv.waitingShards {
				go kv.makeShardMigrationCall(shard, config)
			}
		}
	}
}

func (kv *ShardKV) shardManagement() {
	waitingTimer := time.NewTimer(1 * time.Second)
	for {
		select {
		case <-kv.shutdown:
			return
		case <-waitingTimer.C:
			waitingTimer.Reset(1 * time.Second)
			kv.mu.Lock()
			if _, isLeader := kv.rf.GetState(); isLeader {
				for shard, configNum := range kv.waitingShards {
					go kv.makeShardMigrationCall(shard, configNum)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) pollShardMaster() {
	pollingTimer := time.NewTimer(PollingInterval)
	for {
		select {
		case <-kv.shutdown:
			return
		case <-pollingTimer.C:
			pollingTimer.Reset(PollingInterval)
			kv.mu.Lock()
			if len(kv.waitingShards) == 0 {
				nextConfigNum := kv.config.Num + 1
				kv.mu.Unlock()
				newConfig := kv.mck.Query(nextConfigNum)
				kv.mu.Lock()
				if newConfig.Num > kv.config.Num {
					op := Op{Type: Reconfiguration, Args: newConfig}
					kv.rf.Start(op)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) run() {
	go kv.rf.Replay(1)
	isReplay := true
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if msg.CommandValid {
				kv.handleValidCommand(msg, isReplay)
			} else if cmd, ok := msg.Command.(string); ok {
				if cmd == "InstallSnapshot" {
					kv.readSnapshot()
				} else if cmd == "NewLeader" {
					kv.rf.Start("")
				} else if cmd == "ReplayDone" {
					isReplay = false
					go kv.pollShardMaster()
					go kv.shardManagement()
					for configNum, shards := range kv.cleaningShards {
						for shard := range shards {
							go kv.makeShardCleanupCall(shard, configNum)
						}
					}
				}
			}
			kv.mu.Unlock()
		case <-kv.shutdown:
			return
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int,
	masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = shardmaster.Config{}
	kv.shutdown = make(chan struct{})

	kv.ownShards = make(IntSet)
	kv.migratingShards = make(map[int]map[int]MigrationData)
	kv.waitingShards = make(map[int]int)
	kv.cleaningShards = make(map[int]IntSet)
	kv.historyConfigs = make([]shardmaster.Config, 0)

	kv.data = make(map[string]string)
	kv.cache = make(map[int64]string)
	kv.notifyChanMap = make(map[int]chan notifyArgs)
	go kv.run()
	return kv
}
