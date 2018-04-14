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

const AppendEntriesInterval = time.Millisecond * time.Duration(80) // sleep time between successive AppendEntries call
const ElectionTimeout = time.Millisecond * time.Duration(800)
const HeartBeatTimeout = time.Millisecond * time.Duration(800)

// seed random number generator
func init() {
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
	status            serverStatus        // live or dead
	log               []LogEntry          // log entries
	nextIndex         []int               // for each server, index of the next log entry to send to that server
	matchIndex        []int               // for each server, index of highest log entry, used to track committed index
	applyCh           chan ApplyMsg       // apply to client
	notifyCh          chan struct{}       // notify to apply
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
		rf.log = append(rf.log[truncationStartIndex:]) // log entry previous at lastIncludedIndex at 0 now
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
func (rf *Raft) makeRequestVoteCall(server int, args *RequestVoteArgs, voteCh chan<- bool, retryCh chan<- int) {
	var reply RequestVoteReply
	if ok := rf.peers[server].Call("Raft.RequestVote", args, &reply); ok {
		if reply.VoteGranted {
			voteCh <- true
		} else { // since other server don't grant the vote, check if this server is obsolete
			rf.Lock()
			if rf.currentTerm < reply.Term {
				rf.state, rf.currentTerm, rf.votedFor, rf.leaderId = Follower, reply.Term, -1, -1
				rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))
				rf.persist()
				go func() { voteCh <- false }() // stop election
			}
			rf.Unlock()
		} // else other server is more up-to-date this server
	} else {
		retryCh <- server
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
	rf.persist()
	rf.Unlock()

	args := RequestVoteArgs{Term: currentTerm, CandidateId: me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}

	voteCh := make(chan bool, serverCount-1)
	retryCh := make(chan int, serverCount-1)
	for i := 0; i < serverCount; i++ {
		if i != me {
			go rf.makeRequestVoteCall(i, &args, voteCh, retryCh)
		}
	}

	electionDuration := newRandDuration(ElectionTimeout)
	electionTimer := time.NewTimer(electionDuration) // in case there's no quorum, this election should timeout

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
					DPrintf("CANDIDATE: Server %d receive enough vote and becoming a new leader", rf.me)
					rf.state = Leader
					rf.initIndex() // after election, reinitialized nextIndex and matchIndex
					go rf.replicateLog()
				} // if server is not in candidate state, then another server may establishes itself as leader
				rf.Unlock()
				return
			}
		case follower := <-retryCh:
			rf.Lock()
			if rf.status == Live && rf.state == Candidate {
				go rf.makeRequestVoteCall(follower, &args, voteCh, retryCh)
				rf.Unlock()
			} else {
				rf.Unlock()
				return
			}
		case <-electionTimer.C: // election timeout
			rf.Lock()
			if rf.status == Live && rf.state == Candidate {
				go rf.startElection()
			}
			rf.Unlock()
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
	// DPrintf("Raft server %d receive append entries, args.PrevLogIndex: %d, rf.logIndex: %d, rf.lastIncludedIndex: %d, entries: %#v", rf.me, args.PrevLogIndex, rf.logIndex, rf.lastIncludedIndex,args.Entries)
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
			go rf.notifyLogTruncation(prevLogIndex+1+i, rf.logIndex)
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
	rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, args.PrevLogIndex+args.Len))
	rf.persist()
	rf.setOrResetTimer(newRandDuration(HeartBeatTimeout)) // reset timer
	go rf.notifyApply()
}

// make append entries call to follower and handle reply
func (rf *Raft) makeAppendEntriesCall(follower int, retryCh chan<- int, empty bool) {
	rf.Lock()
	if rf.state != Leader {
		rf.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[follower] - 1
	if prevLogIndex < rf.lastIncludedIndex {
		rf.Unlock()
		go rf.makeInstallSnapshotCall(follower, retryCh)
		return
	}
	var args AppendEntriesArgs
	// DPrintf("Raft server %d send append entries to follower: %d, nextIndex: %d, actual offset index: %d, rf length of log: %d", rf.me, follower, rf.nextIndex[follower], rf.getOffsetIndex(prevLogIndex), len(rf.log))
	prevLogTerm := rf.getEntry(prevLogIndex).LogTerm
	if empty || rf.nextIndex[follower] == rf.logIndex {
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
					go rf.notifyApply()
					DPrintf("Leader %d have following servers: %v replicating log and can update commit index to :%d", rf.me, agreedFollower, rf.commitIndex)
				}
			}
		}
		rf.Unlock()
	} else { // retry
		retryCh <- follower
	}
}

// handle AppendEntries call fail, only sends heartbeat messages(empty log entry)
func (rf *Raft) retryAppendEntries(retryCh chan int, done <-chan struct{}) {
	for {
		select {
		case follower := <-retryCh:
			go rf.makeAppendEntriesCall(follower, retryCh, true)
		case <-done:
			return
		}
	}
}

// replicate (empty)log to follower
func (rf *Raft) replicateLog() {
	retryCh := make(chan int)
	done := make(chan struct{})
	rf.Start("") // start empty log entry to help commit
	go rf.retryAppendEntries(retryCh, done)
	for {
		rf.Lock()
		if rf.status != Live || rf.state != Leader {
			rf.Unlock()
			done <- struct{}{}
			return
		}
		for follower := 0; follower < len(rf.peers); follower++ {
			if follower != rf.me {
				go rf.makeAppendEntriesCall(follower, retryCh, false)
			}
		}
		rf.Unlock()
		time.Sleep(AppendEntriesInterval)
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
		DPrintf("Follower %d receive install snapshot request, rf.lastIncludedIndex: %d, args.LastIncludedIndex: %d",
			rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)
		truncationStartIndex := rf.getOffsetIndex(args.LastIncludedIndex)
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
		rf.logIndex = Max(rf.logIndex, rf.lastIncludedIndex+1)
		if truncationStartIndex < len(rf.log) { // snapshot contain a prefix of its log
			rf.log = append(rf.log[truncationStartIndex:])
		} else { // snapshot contain new information not already in the follower's log
			rf.log = []LogEntry{{args.LastIncludedIndex, args.LastIncludedTerm, nil}} // discards entire log
		}
		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
		go rf.notifyApply()
		DPrintf("Follower %d install snapshot successfully, notify kv server", rf.me)
	}
}

// transfer snapshot in one RPC call
func (rf *Raft) makeInstallSnapshotCall(follower int, retryCh chan<- int) {
	rf.Lock()
	if rf.state != Leader {
		rf.Unlock()
		return
	}
	// DPrintf("Raft server %d send install snapshot to follower %d", rf.me, follower)
	args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.getEntry(rf.lastIncludedIndex).LogTerm, Data: rf.persister.ReadSnapshot()}
	rf.Unlock()
	var reply InstallSnapshotReply
	if ok := rf.peers[follower].Call("Raft.InstallSnapshot", &args, &reply); ok {
		rf.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))
		} else {
			previousNextIndex, previousMatchIndex := rf.nextIndex[follower], rf.matchIndex[follower]
			rf.nextIndex[follower] = Max(rf.nextIndex[follower], rf.lastIncludedIndex+1)
			rf.matchIndex[follower] = Max(rf.matchIndex[follower], rf.lastIncludedIndex)
			DPrintf("Raft server %d install snapshot for follower %d successfully, update next index, previous: %d, now: %d, update match index, previous: %d, now: %d",
				rf.me, follower, previousNextIndex, rf.nextIndex[follower], previousMatchIndex, rf.matchIndex[follower])
		}
		rf.Unlock()
	} else {
		retryCh <- follower
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

	if rf.status == Live && rf.state == Leader { // append log only if server is leader
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
	rf.status = Dead
	DPrintf("Kill raft server: %d, state: %s, lastLogIndex: %d, term: %d, commit index: %d, last applied index: %d", rf.me, rf.state, rf.logIndex, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	rf.Unlock()
}

// handle timer timeout
func (rf *Raft) run() {
loop:
	<-rf.timer.C
	go rf.startElection() // follower timeout, start a new election
	goto loop
}

// notify client when raft server truncate log
func (rf *Raft) notifyLogTruncation(start int, end int) {
	for i := start; i < end; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: i, CommandTerm: -1, Command: "LogTruncation"}
	}
}

// notify to apply
func (rf *Raft) notifyApply() {
	rf.notifyCh <- struct{}{}
}

func (rf *Raft) apply() {
loop:
	<-rf.notifyCh
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
		entries = append([]LogEntry{},rf.log[startIndex:endIndex+1]...)
		rf.lastApplied = rf.commitIndex
		rf.persist()
	}
	rf.Unlock()

	for i := 0; i < len(entries); i++ {
		if cmd, ok := entries[i].Command.(string); ok && cmd == "" {
			rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
		} else {
			rf.applyCh <- ApplyMsg{CommandValid: commandValid, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
		}
	}
	goto loop
}

func (rf *Raft) Replay(startIndex int) {
	rf.Lock()

	if startIndex <= rf.lastIncludedIndex {
		rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: rf.log[0].LogIndex, CommandTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"}
		startIndex = rf.lastIncludedIndex + 1
		rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	}
	entries := append([]LogEntry{},rf.log[rf.getOffsetIndex(startIndex):rf.getOffsetIndex(rf.lastApplied+1)]...)
	rf.Unlock()
	for i := 0; i < len(entries); i++ {
		if cmd, ok := entries[i].Command.(string); ok && cmd == "" {
			rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
		} else {
			rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
		}
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
	rf.status = Live
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.applyCh = applyCh
	rf.notifyCh = make(chan struct{})

	rf.readPersistState() // initialize from state persisted before a crash
	DPrintf("Raft server: %d call readPersistState to restore state, currentTerm: %d, lastLogIndex: %d, commit index: %d, last applied index: %d, lastIncludedIndex: %d",
		rf.me, rf.currentTerm, rf.logIndex, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex)
	rf.setOrResetTimer(newRandDuration(HeartBeatTimeout))

	go rf.run()
	go rf.apply()

	return rf
}
