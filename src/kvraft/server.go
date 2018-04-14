package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"bytes"
)

const Debug = 1

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
	me               int
	lastCommandIndex int // last command index kv server receive from raft
	maxraftstate     int // snapshot if log grows this big

	rf        *raft.Raft
	persister *raft.Persister // Object to hold this peer's persisted state

	applyCh chan raft.ApplyMsg

	data          map[string]string       // storing Key-Value pair
	cache         map[int64]struct{}      // cache put/append requests that server have processed, use request id as key
	notifyChanMap map[int]chan notifyArgs // use index returned from raft as key
}

func (kv *KVServer) notifyIfPresent(index int, reply notifyArgs) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		ch <- reply
		delete(kv.notifyChanMap, index)
	}
}

func (kv *KVServer) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastCommandIndex)
	e.Encode(kv.cache)
	e.Encode(kv.data)

	snapshot := w.Bytes()
	kv.rf.PersistAndSaveSnapshot(kv.lastCommandIndex, snapshot)
}

func (kv *KVServer) readSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	lastCommandIndex := 0

	if d.Decode(&lastCommandIndex) != nil ||
		d.Decode(&kv.cache) != nil ||
		d.Decode(&kv.data) != nil {
		log.Fatal("Error in reading snapshot")
	}
	kv.lastCommandIndex = lastCommandIndex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.Lock()
	op := Op{RequestId: args.RequestId, ExpireRequestId: args.ExpireRequestId, Key: args.Key, Value: "", Op: "Get"}
	index, term, ok := kv.rf.Start(op)
	if !ok {
		kv.Unlock()
		reply.Err = WrongLeader
		return
	}
	notifyCh := make(chan notifyArgs)
	kv.notifyChanMap[index] = notifyCh
	kv.Unlock()
	result := <-notifyCh
	if result.Term != term {
		reply.Err = WrongLeader
	} else {
		reply.Value = result.Value
		reply.Err = result.Err
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.Lock()
	if _, ok := kv.cache[args.RequestId]; ok { // find duplicate request
		reply.Err = OK
		kv.Unlock()
		return
	} else {
		op := Op{RequestId: args.RequestId, Key: args.Key, Value: args.Value, Op: args.Op}
		index, term, ok := kv.rf.Start(op)
		if !ok {
			kv.Unlock()
			reply.Err = WrongLeader
			return
		}
		notifyCh := make(chan notifyArgs)
		kv.notifyChanMap[index] = notifyCh
		kv.Unlock()
		result := <-notifyCh
		if result.Term != term {
			reply.Err = WrongLeader
		} else {
			reply.Err = result.Err
		}
	}
}

func (kv *KVServer) Kill() {
	kv.Lock()
	kv.snapshot()
	kv.rf.Kill()
	kv.Unlock()
}

func (kv *KVServer) installSnapshot(lastCommandIndex int) {
	kv.readSnapshot()
	kv.lastCommandIndex = lastCommandIndex
}

func (kv *KVServer) handleValidCommand(msg raft.ApplyMsg) {
	kv.lastCommandIndex = raft.Max(kv.lastCommandIndex, msg.CommandIndex)
	cmd := msg.Command.(Op)
	delete(kv.cache, cmd.ExpireRequestId) // delete older request
	result := notifyArgs{Term: msg.CommandTerm, Value: "", Err: OK}
	if cmd.Op == "Get" {
		if v, ok := kv.data[cmd.Key]; ok {
			result.Value = v
		} else {
			result.Value = ""
			result.Err = ErrNoKey
		}
	} else {
		if _, ok := kv.cache[cmd.RequestId]; !ok { // prevent duplication
			if cmd.Op == "Put" {
				kv.data[cmd.Key] = cmd.Value
			} else {
				if v, ok := kv.data[cmd.Key]; ok {
					kv.data[cmd.Key] = v + cmd.Value
				} else {
					kv.data[cmd.Key] = cmd.Value
				}
			}
			kv.cache[cmd.RequestId] = struct{}{}
		}
	}
	kv.notifyIfPresent(msg.CommandIndex, result)
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.snapshot()
	}
}

func (kv *KVServer) run() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.Lock()
			if msg.CommandValid {
				kv.handleValidCommand(msg)
			} else { // command not valid
				if cmd, ok := msg.Command.(string); ok {
					if cmd == "LogTruncation" || cmd == "" {
						reply := notifyArgs{Term: msg.CommandTerm, Value: "", Err: WrongLeader}
						kv.notifyIfPresent(msg.CommandIndex, reply)
					} else {
						kv.installSnapshot(msg.CommandIndex)

						reply := notifyArgs{Term: msg.CommandTerm, Value: "", Err: WrongLeader}
						for index, ch := range kv.notifyChanMap {
							if index <= msg.CommandIndex {
								ch <- reply
								delete(kv.notifyChanMap, index)
							}
						}
					}
				}
			}
			kv.Unlock()
		}
	}
}

func (kv *KVServer) restoreState() {
	go kv.rf.Replay(1)
	kv.Lock()
loop:
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.handleValidCommand(msg)
			} else { // command not valid
				if cmd, ok := msg.Command.(string); ok {
					if cmd == "InstallSnapshot" {
						kv.installSnapshot(msg.CommandIndex)
					} else if cmd == "ReplayDone" {
						break loop
					}
				}
			}
		}
	}
	kv.Unlock()
	go kv.run()
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
	kv.lastCommandIndex = 0
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.cache = make(map[int64]struct{})
	kv.notifyChanMap = make(map[int]chan notifyArgs)
	go kv.restoreState()
	return kv
}
