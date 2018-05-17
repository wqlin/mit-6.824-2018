package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"bytes"
	"time"
)

const Debug = 0

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// represent an operation
type Op struct {
	RequestId,
	ExpireRequestId int64 // uniquely identifying one request
	Key   string
	Value string
	Op    string
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
	cache         map[int64]struct{}      // cache put/append requests that server have processed, use request id as key
	notifyChanMap map[int]chan notifyArgs // use index returned from raft as key
}

func (kv *KVServer) notifyIfPresent(index int, reply notifyArgs) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		delete(kv.notifyChanMap, index)
		ch <- reply
	}
}

func (kv *KVServer) startOp(op Op) (Err, string) {
	index, term, ok := kv.rf.Start(op)
	if !ok {
		return WrongLeader, ""
	}
	kv.Lock()
	notifyCh := make(chan notifyArgs)
	kv.notifyChanMap[index] = notifyCh
	kv.Unlock()
	DPrintf("%d start %d at %d", kv.me, op.RequestId, index)
	timeoutTimer := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-timeoutTimer.C:
			kv.Lock()
			delete(kv.notifyChanMap, index)
			kv.Unlock()
			DPrintf("%d delete %d channel at %d", kv.me, op.RequestId, index)
			return WrongLeader, ""
		case result := <-notifyCh:
			DPrintf("%d get %d reply %v at %d", kv.me, op.RequestId, result, index)
			if result.Term != term {
				return WrongLeader, ""
			} else {
				return result.Err, result.Value
			}
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
	var threshold = int(1.5 * float64(kv.maxraftstate))
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
	op := Op{RequestId: args.RequestId, ExpireRequestId: args.ExpireRequestId, Key: args.Key, Value: "", Op: "Get"}
	err, value := kv.startOp(op)
	reply.Err, reply.Value = err, value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{RequestId: args.RequestId, Key: args.Key, Value: args.Value, Op: args.Op}
	err, _ := kv.startOp(op)
	reply.Err = err
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	close(kv.shutdown)
}

func (kv *KVServer) handleValidCommand(msg raft.ApplyMsg) {
	if cmd, ok := msg.Command.(Op); ok {
		delete(kv.cache, cmd.ExpireRequestId) // delete older request
		result := notifyArgs{Term: msg.CommandTerm, Value: "", Err: OK}
		if cmd.Op == "Get" {
			result.Value = kv.data[cmd.Key]
		} else if _, ok := kv.cache[cmd.RequestId]; !ok { // prevent duplication
			if cmd.Op == "Put" {
				kv.data[cmd.Key] = cmd.Value
			} else {
				kv.data[cmd.Key] += cmd.Value
			}
			kv.cache[cmd.RequestId] = struct{}{}
		}
		kv.snapshotIfNeeded(msg.CommandIndex)
		kv.notifyIfPresent(msg.CommandIndex, result)
	}
}

func (kv *KVServer) run() {
	go kv.rf.Replay(1)
	for {
		select {
		case msg := <-kv.applyCh:
			kv.Lock()
			if msg.CommandValid {
				kv.handleValidCommand(msg)
			} else if cmd, ok := msg.Command.(string); ok && cmd == "InstallSnapshot" {
				kv.readSnapshot()
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shutdown = make(chan struct{})

	kv.data = make(map[string]string)
	kv.cache = make(map[int64]struct{})
	kv.notifyChanMap = make(map[int]chan notifyArgs)
	go kv.run()
	return kv
}
