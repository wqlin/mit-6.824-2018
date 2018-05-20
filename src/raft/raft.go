package raft

import (
	"sync"
	"labrpc"
	"time"
	crand "crypto/rand"
	"math/rand"
	"labgob"
	"bytes"
	"log"
	"math/big"
)

const AppendEntriesInterval = time.Millisecond * time.Duration(100) // sleep time between successive AppendEntries call
const ElectionTimeout = time.Millisecond * time.Duration(1000)
const HeartBeatTimeout = time.Millisecond * time.Duration(1000)

// seed random number generator
func init() {
	labgob.Register(LogEntry{})
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	seed := bigx.Int64()
	rand.Seed(seed)
}

// generate random time duration that is between minDuration and 2x minDuration
func newRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return time.Duration(minDuration + extra)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	sync.Mutex                            // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	leaderId          int                 // leader's id
	currentTerm       int                 // latest term server has seen, initialized to 0
	votedFor          int                 // candidate that received vote in current term
	commitIndex       int                 // index of highest log entry known to be committed, initialized to 0
	lastApplied       int                 // index of highest log entry applied to state machine, initialized to 0
	lastIncludedIndex int                 // index of the last entry in the log that snapshot replaces, initialized to 0
	logIndex          int                 // index of next log entry to be stored, initialized to 1
	state             serverState         // state of server
	shutdown          chan struct{}       // shutdown gracefully
	log               []LogEntry          // log entries
	nextIndex         []int               // for each server, index of the next log entry to send to that server
	matchIndex        []int               // for each server, index of highest log entry, used to track committed index
	applyCh           chan ApplyMsg       // apply to client
	notifyApplyCh     chan struct{}       // notify to apply
	timer             *time.Timer         // heartbeat timer, kick off new leader election when time out
}

func (rf *Raft) setOrResetTimer(duration time.Duration) {
	if rf.timer == nil {
		rf.timer = time.NewTimer(duration)
	} else {
		// Always stop a timer before reusing it. See https://golang.org/pkg/time/#Timer.Reset
		// We ignore the value return from Stop() because if Stop() return false, the value inside the channel has been drained out
		rf.timer.Stop()
		rf.timer.Reset(duration)
	}
}

// After a leader comes to power, it calls this function to initialize nextIndex and matchIndex
func (rf *Raft) initIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock() // use synchronization to ensure visibility
	defer rf.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage, where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.logIndex)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)

	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersistState() {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0, 0

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&rf.log) != nil {
		log.Fatal("Error")
	}
	rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.logIndex, rf.commitIndex, rf.lastApplied = currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied
}

func (rf *Raft) PersistAndSaveSnapshot(lastIncludedIndex int, snapshot [] byte) {
	rf.Lock()
	defer rf.Unlock()
	if lastIncludedIndex > rf.lastIncludedIndex {
		truncationStartIndex := rf.getOffsetIndex(lastIncludedIndex)
		rf.log = append([]LogEntry{}, rf.log[truncationStartIndex:]...) // log entry previous at lastIncludedIndex at 0 now
		rf.lastIncludedIndex = lastIncludedIndex
		data := rf.getPersistState()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
}

// because snapshot will replace committed log entries in log
// thus the length of rf.log is different from(less than or equal) rf.logIndex
func (rf *Raft) getOffsetIndex(i int) int {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) getEntry(i int) LogEntry {
	offsetIndex := rf.getOffsetIndex(i)
	return rf.log[offsetIndex]
}

func (rf *Raft) notifyNewLeader() {
	rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: -1, CommandTerm: -1, Command: "NewLeader"}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return
	}
	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) { // the server has voted in this term
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
		if rf.state != Follower { // once server becomes follower, it has to reset timer
			rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))
			rf.state = Follower
		}
	}
	rf.leaderId = -1 // other server trying to elect a new leader
	reply.Term = args.Term
	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.getEntry(lastLogIndex).LogTerm
	if lastLogTerm > args.LastLogTerm || // the server has log with higher term
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) { // under same term, this server has longer index
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.setOrResetTimer(newRandDuration(HeartBeatTimeout)) // granting vote to candidate, reset timer
	rf.persist()
}

// send RequestVote RPC call to server and handle reply
func (rf *Raft) makeRequestVoteCall(server int, currentTerm, lastLogIndex, lastLogTerm int, timeoutDuration time.Duration, voteCh chan<- bool) {
	timeoutTimer := time.NewTimer(timeoutDuration)
	args := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	for {
		select {
		case <-rf.shutdown:
			return
		case <-timeoutTimer.C:
			return
		default:
			var reply RequestVoteReply
			if ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply); ok {
				if reply.VoteGranted {
					voteCh <- true
					return
				} else { // since other server don't grant the vote, check if this server is obsolete
					rf.Lock()
					if rf.currentTerm < reply.Term {
						rf.state, rf.currentTerm, rf.votedFor, rf.leaderId = Follower, reply.Term, -1, -1
						rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))
						rf.persist()
						go func(ch chan<- bool) { ch <- false }(voteCh)
					}
					rf.Unlock()
					return
				}
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.Lock()
	rf.leaderId = -1     // server believes there is no leader
	rf.state = Candidate // transition to candidate state
	rf.currentTerm += 1  // increment current term
	rf.votedFor = rf.me  // vote for self
	currentTerm, lastLogIndex, me, serverCount := rf.currentTerm, rf.logIndex-1, rf.me, len(rf.peers)
	lastLogTerm := rf.getEntry(lastLogIndex).LogTerm
	DPrintf("%d at %d start election, last index %d last term %d last entry %v",
		rf.me, rf.currentTerm, lastLogIndex, lastLogTerm, rf.getEntry(lastLogIndex))
	rf.persist()
	rf.Unlock()

	electionDuration := newRandDuration(ElectionTimeout)
	electionTimer := time.NewTimer(electionDuration) // in case there's no quorum, this election should timeout

	voteCh := make(chan bool, serverCount)
	for i := 0; i < serverCount; i++ {
		if i != me {
			go rf.makeRequestVoteCall(i, currentTerm, lastLogIndex, lastLogTerm, electionDuration, voteCh)
		}
	}

	voteCount, threshold := 0, serverCount/2 // counting vote

	for {
		select {
		case status := <-voteCh:
			if !status {
				return
			}
			voteCount += 1
			if voteCount >= threshold { // receive enough vote
				rf.Lock()
				if rf.state == Candidate { // check if server is in candidate state before becoming a leader
					DPrintf("CANDIDATE: %d receive enough vote and becoming a new leader", rf.me)
					rf.state = Leader
					rf.initIndex() // after election, reinitialized nextIndex and matchIndex
					go rf.replicateLog()
					go rf.notifyNewLeader()
				} // if server is not in candidate state, then another server may establishes itself as leader
				rf.Unlock()
				return
			}
		case <-electionTimer.C: // election timeout
			rf.Lock()
			if rf.state == Candidate {
				go rf.startElection()
			}
			rf.Unlock()
			return
		case <-rf.shutdown:
			return
		}
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()
	if rf.currentTerm > args.Term { // RPC call comes from an illegitimate leader
		reply.Term, reply.Success = rf.currentTerm, false
		return
	} // else args.Term >= rf.currentTerm

	reply.Term = args.Term
	rf.leaderId = args.LeaderId
	rf.setOrResetTimer(newRandDuration(HeartBeatTimeout)) // reset timer
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.state = Follower
	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex
	if prevLogIndex < rf.lastIncludedIndex {
		reply.Success, reply.ConflictIndex = false, rf.lastIncludedIndex+1
		return
	}
	if logIndex <= prevLogIndex || rf.getEntry(prevLogIndex).LogTerm != args.PrevLogTerm { // follower don't agree with leader on last log entry
		conflictIndex := Min(rf.logIndex-1, prevLogIndex)
		conflictTerm := rf.getEntry(conflictIndex).LogTerm
		floor := Max(rf.lastIncludedIndex, rf.commitIndex)
		for ; conflictIndex > floor && rf.getEntry(conflictIndex - 1).LogTerm == conflictTerm; conflictIndex-- {
		}
		reply.Success, reply.ConflictIndex = false, conflictIndex
		return
	}
	reply.Success, reply.ConflictIndex = true, -1

	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i >= rf.logIndex {
			break
		}
		if rf.getEntry(prevLogIndex + 1 + i).LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = prevLogIndex + 1 + i
			truncationEndIndex := rf.getOffsetIndex(rf.logIndex)
			rf.log = append(rf.log[:truncationEndIndex]) // delete any conflicting log entries
			break
		}
	}
	for ; i < args.Len; i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, args.PrevLogIndex+args.Len))
	rf.persist()
	rf.setOrResetTimer(newRandDuration(HeartBeatTimeout)) // reset timer
	if rf.commitIndex > oldCommitIndex {
		rf.notifyApplyCh <- struct{}{}
	}
}

// make append entries call to follower and handle reply
func (rf *Raft) makeAppendEntriesCall(follower int, done chan struct{}) {
	select {
	case <-rf.shutdown:
		return
	default:
		rf.Lock()
		if rf.state != Leader {
			rf.Unlock()
			done <- struct{}{}
			return
		}
		prevLogIndex := rf.nextIndex[follower] - 1
		if prevLogIndex < rf.lastIncludedIndex {
			rf.Unlock()
			go rf.makeInstallSnapshotCall(follower)
			return
		}
		var args AppendEntriesArgs
		prevLogTerm := rf.getEntry(prevLogIndex).LogTerm
		if rf.nextIndex[follower] == rf.logIndex {
			args = AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, CommitIndex: rf.commitIndex, Len: 0, Entries: nil}
		} else {
			logs := append([]LogEntry{}, rf.log[rf.getOffsetIndex(prevLogIndex+1):]...)
			args = AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, CommitIndex: rf.commitIndex, Len: len(logs), Entries: logs}
		}
		rf.Unlock()
		var reply AppendEntriesReply
		if ok := rf.peers[follower].Call("Raft.AppendEntries", &args, &reply); ok { // RPC call success
			rf.Lock()
			if !reply.Success {
				if reply.Term > rf.currentTerm { // the leader is obsolete
					rf.currentTerm, rf.state, rf.votedFor, rf.leaderId = reply.Term, Follower, -1, -1
					rf.persist()
					rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))
				} else { // follower is inconsistent with leader
					rf.nextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.logIndex))
				}
			} else { // reply.Success is true
				prevLogIndex, logEntriesLen := args.PrevLogIndex, args.Len
				if prevLogIndex+logEntriesLen >= rf.nextIndex[follower] { // in case apply arrive in out of order
					rf.nextIndex[follower] = prevLogIndex + logEntriesLen + 1
					rf.matchIndex[follower] = prevLogIndex + logEntriesLen
				}
				// if log entry contains term equals to current term, then try if we can commit log by counting replicas
				if prevLogIndex+logEntriesLen < rf.logIndex && rf.commitIndex < prevLogIndex+logEntriesLen && rf.getEntry(prevLogIndex + logEntriesLen).LogTerm == rf.currentTerm {
					l := len(rf.peers)
					threshold, count, agreedFollower := l/2, 0, make([]int, 0, l)
					for j := 0; j < l; j++ {
						if j != rf.me && rf.matchIndex[j] >= prevLogIndex+logEntriesLen {
							count += 1
							agreedFollower = append(agreedFollower, j)
						}
					}
					if count >= threshold {
						rf.commitIndex = prevLogIndex + logEntriesLen // can commit log
						rf.persist()
						rf.notifyApplyCh <- struct{}{}
						// DPrintf("Leader %d have following servers: %v replicating log and can update commit index to :%d", rf.me, agreedFollower, rf.commitIndex)
					}
				}
			}
			rf.Unlock()
		}
	}
}

// replicate (empty)log to follower
func (rf *Raft) replicateLog() {
	heartBeatTimer := time.NewTimer(AppendEntriesInterval)
	done := make(chan struct{}, 100)
	for {
		select {
		case <-rf.shutdown:
			return
		case <-done:
			return
		case <-heartBeatTimer.C:
			heartBeatTimer.Reset(AppendEntriesInterval)
			for follower := 0; follower < len(rf.peers); follower++ {
				if follower != rf.me {
					go rf.makeAppendEntriesCall(follower, done)
				}
			}
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))
	rf.leaderId = args.LeaderId

	if args.LastIncludedIndex > rf.lastIncludedIndex {
		truncationStartIndex := rf.getOffsetIndex(args.LastIncludedIndex)
		rf.lastIncludedIndex = args.LastIncludedIndex
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
		rf.logIndex = Max(rf.logIndex, rf.lastIncludedIndex+1)
		if truncationStartIndex < len(rf.log) { // snapshot contain a prefix of its log
			rf.log = append(rf.log[truncationStartIndex:])
		} else { // snapshot contain new information not already in the follower's log
			rf.log = []LogEntry{{args.LastIncludedIndex, args.LastIncludedTerm, nil}} // discards entire log
		}
		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
		if rf.commitIndex > oldCommitIndex {
			rf.notifyApplyCh <- struct{}{}
		}
	}
	rf.persist()
}

// transfer snapshot in one RPC call
func (rf *Raft) makeInstallSnapshotCall(follower int) {
	rf.Lock()
	if rf.state != Leader {
		rf.Unlock()
		return
	}
	args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.getEntry(rf.lastIncludedIndex).LogTerm, Data: rf.persister.ReadSnapshot()}
	rf.Unlock()
	var reply InstallSnapshotReply
	if ok := rf.peers[follower].Call("Raft.InstallSnapshot", &args, &reply); ok {
		rf.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.persist()
			rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))
		} else {
			rf.nextIndex[follower] = Max(rf.nextIndex[follower], rf.lastIncludedIndex+1)
			rf.matchIndex[follower] = Max(rf.matchIndex[follower], rf.lastIncludedIndex)
		}
		rf.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Lock()
	defer rf.Unlock()

	if rf.state == Leader { // append log only if server is leader
		index := rf.logIndex
		entry := LogEntry{LogIndex: index, LogTerm: rf.currentTerm, Command: command}

		if offsetIndex := rf.getOffsetIndex(rf.logIndex); offsetIndex < len(rf.log) {
			rf.log[offsetIndex] = entry
		} else {
			rf.log = append(rf.log, entry)
		}
		rf.matchIndex[rf.me] = rf.logIndex
		rf.logIndex += 1
		rf.persist()
		return index, rf.currentTerm, true
	} else {
		return -1, -1, false
	}
}

func (rf *Raft) Kill() {
	rf.Lock()
	rf.persist()
	close(rf.shutdown)
	DPrintf("Kill raft %d at %d state: %s lastLogIndex: %d lastLogEntry %v commit index: %d, last applied index: %d",
		rf.me, rf.currentTerm, rf.state, rf.logIndex, rf.commitIndex, rf.lastApplied)
	rf.Unlock()
}

// handle timer timeout
func (rf *Raft) run() {
	for {
		select {
		case <-rf.timer.C:
			// DPrintf("%d %d timeout, start election", runtime.NumGoroutine(), rf.me)
			go rf.startElection() // follower timeout, start a new election
		case <-rf.shutdown:
			return
		}
	}
}

func (rf *Raft) apply() {
	for {
		select {
		case <-rf.notifyApplyCh:
			rf.Lock()
			var commandValid bool
			var entries []LogEntry
			if rf.lastApplied < rf.lastIncludedIndex {
				commandValid = false
				rf.lastApplied = rf.lastIncludedIndex
				entries = append(entries, LogEntry{LogIndex: rf.lastIncludedIndex, LogTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"})
			} else if rf.lastApplied < rf.logIndex && rf.lastApplied < rf.commitIndex {
				commandValid = true
				startIndex, endIndex := rf.getOffsetIndex(rf.lastApplied+1), rf.getOffsetIndex(rf.commitIndex)
				entries = append([]LogEntry{}, rf.log[startIndex:endIndex+1]...)
				rf.lastApplied = rf.commitIndex
			}
			rf.persist()
			rf.Unlock()

			for i := 0; i < len(entries); i++ {
				rf.applyCh <- ApplyMsg{CommandValid: commandValid, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
			}
		case <-rf.shutdown:
			return
		}
	}
}

func (rf *Raft) Replay(startIndex int) {
	rf.Lock()
	if startIndex <= rf.lastIncludedIndex {
		rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: rf.log[0].LogIndex, CommandTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"}
		startIndex = rf.lastIncludedIndex + 1
		rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	}
	entries := append([]LogEntry{}, rf.log[rf.getOffsetIndex(startIndex):rf.getOffsetIndex(rf.lastApplied+1)]...)
	rf.Unlock()
	for i := 0; i < len(entries); i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
	}
	rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: -1, CommandTerm: -1, Command: "ReplayDone"}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent serverState, and also initially holds the most
// recent saved serverState, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func
Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.logIndex = 1
	rf.state = Follower // initializing as follower
	rf.shutdown = make(chan struct{})
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 10000)

	rf.readPersistState() // initialize from state persisted before a crash
	rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))

	go rf.run()
	go rf.apply()

	return rf
}
