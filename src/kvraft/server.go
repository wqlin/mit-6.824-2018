package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const StartTimeoutInterval = time.Duration(3 * time.Second)
const SnapshotThreshold = 1.5

func init() {
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// used to notify RPC handler
type notifyArgs struct {
	Term  int
	Value string
	Err   Err
}

type KVServer struct {
	sync.Mutex
	me           int
	maxraftstate int // snapshot if log grows this big

	rf        *raft.Raft
	persister *raft.Persister // Object to hold this peer's persisted state

	applyCh  chan raft.ApplyMsg
	shutdown chan struct{}

	data          map[string]string       // storing Key-Value pair
	cache         map[int64]int           // cache put/append requests that server have processed, use request id as key
	notifyChanMap map[int]chan notifyArgs // use index returned from raft as key
}

func (kv *KVServer) notifyIfPresent(index int, reply notifyArgs) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		delete(kv.notifyChanMap, index)
		ch <- reply
	}
}

func (kv *KVServer) start(args interface{}) (Err, string) {
	index, term, ok := kv.rf.Start(args)
	if !ok {
		return ErrWrongLeader, ""
	}
	kv.Lock()
	notifyCh := make(chan notifyArgs, 1)
	kv.notifyChanMap[index] = notifyCh
	kv.Unlock()
	select {
	case <-time.After(StartTimeoutInterval):
		kv.Lock()
		delete(kv.notifyChanMap, index)
		kv.Unlock()
		return ErrWrongLeader, ""
	case result := <-notifyCh:
		if result.Term != term {
			return ErrWrongLeader, ""
		} else {
			return result.Err, result.Value
		}
	}
	return OK, ""
}
func (kv *KVServer) snapshot(lastCommandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.cache)
	e.Encode(kv.data)
	snapshot := w.Bytes()
	kv.rf.PersistAndSaveSnapshot(lastCommandIndex, snapshot)
}

func (kv *KVServer) snapshotIfNeeded(lastCommandIndex int) {
	var threshold = int(SnapshotThreshold * float64(kv.maxraftstate))
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= threshold {
		kv.snapshot(lastCommandIndex)
	}
}

func (kv *KVServer) readSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.cache) != nil ||
		d.Decode(&kv.data) != nil {
		log.Fatal("Error in reading snapshot")
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Err, reply.Value = kv.start(args.copy())
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err, _ = kv.start(args.copy())
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	close(kv.shutdown)
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	result := notifyArgs{Term: msg.CommandTerm, Value: "", Err: OK}
	if arg, ok := msg.Command.(GetArgs); ok {
		result.Value = kv.data[arg.Key]
	} else if arg, ok := msg.Command.(PutAppendArgs); ok {
		if kv.cache[arg.ClientId] < arg.RequestSeq {
			if arg.Op == "Put" {
				kv.data[arg.Key] = arg.Value
			} else {
				kv.data[arg.Key] += arg.Value
			}
			kv.cache[arg.ClientId] = arg.RequestSeq
		}
	} else {
		result.Err = ErrWrongLeader
	}
	kv.notifyIfPresent(msg.CommandIndex, result)
	kv.snapshotIfNeeded(msg.CommandIndex)
}

func (kv *KVServer) run() {
	go kv.rf.Replay(1)
	for {
		select {
		case msg := <-kv.applyCh:
			kv.Lock()
			if msg.CommandValid {
				kv.apply(msg)
			} else if cmd, ok := msg.Command.(string); ok {
				if cmd == "InstallSnapshot" {
					kv.readSnapshot()
				} else if cmd == "NewLeader" {
					kv.rf.Start("")
				}
			}
			kv.Unlock()
		case <-kv.shutdown:
			return
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant Key/Value service.
// me is the Index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shutdown = make(chan struct{})

	kv.data = make(map[string]string)
	kv.cache = make(map[int64]int)
	kv.notifyChanMap = make(map[int]chan notifyArgs)
	go kv.run()
	return kv
}
