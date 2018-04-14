package raft

type serverState int32
const (
	Leader    serverState = iota
	Follower
	Candidate
)

func (state serverState) String() string{
	switch state {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	default:
		return "Candidate"
	}
}

type serverStatus int32
const (
	Live serverStatus = iota
	Dead
)

type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	CommandTerm  int
	Command      interface{}
}

// struct definition for log entry
type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

type RequestVoteArgs struct {
	Term,         // candidate's current term
	CandidateId,  // candidate requesting vote
	LastLogIndex, // index of candidate's last log entry
	LastLogTerm int // term of candidate's last log entry
}

type RequestVoteReply struct {
	VoteGranted bool // true means candidate received vote
	Term        int  // current term from other servers
}

type AppendEntriesArgs struct {
	Term,
	LeaderId,
	PrevLogIndex,
	PrevLogTerm,
	CommitIndex int
	Len     int        // number of logs sends to follower
	Entries []LogEntry // logs that send to follower
}

type AppendEntriesReply struct {
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	Term    int
	ConflictIndex int // in case of conflicting, follower include the first index it store for conflict term
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}
