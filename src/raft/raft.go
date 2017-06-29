package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	logs        []Log

	commitIndex int
	lastApplied int

	state int

	nextIndex  []int
	matchIndex []int

	chanCommand chan interface{}

	chanHeartBeat chan bool

	chanBecomeLeader chan bool
	voteCount        int
}

type Log struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == STATE_LEADER
	return term, isleader
}

func (rf *Raft) GetLastTerm() int {
	lastIndex := rf.GetLastIndex()
	if lastIndex == -1 {
		return -1
	}
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) GetLastIndex() int {
	return len(rf.logs) - 1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type RequestVoteArgs struct {
	Term           int
	CandidateIndex int
	LastLogIndex   int
	LastLogTerm    int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderIndex  int
	LastLogIndex int
	LastLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.GetLastIndex() >= args.LastLogIndex {
		if rf.currentTerm > args.Term || rf.state == STATE_LEADER {
			return
		}
	}
	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateIndex {
		return
	}
	if rf.currentTerm < args.Term {
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
	}
	if rf.GetLastTerm() < args.LastLogTerm || (rf.GetLastTerm() == args.LastLogTerm && rf.GetLastIndex() <= args.LastLogIndex) {
		rf.votedFor = args.CandidateIndex
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.GetLastIndex()
		return
	}
	rf.chanHeartBeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term
	if args.LastLogIndex > rf.GetLastIndex() {
		reply.NextIndex = len(rf.logs)
		return
	}
	reply.Success = true
	reply.NextIndex = len(rf.logs)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//DPrintf("%v's request vote has been sent\n", server)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != STATE_CANDIDATE || args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.votedFor = -1
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == STATE_CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				rf.chanBecomeLeader <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//DPrintf("%v's append entries has been sent\n", server)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		rf.nextIndex[server] = reply.NextIndex
		if reply.Success == false && reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
		} else {
			rf.nextIndex[server] = 0
		}
	}
	return ok
}

func (rf *Raft) BroadcastRequestVote() {
	//DPrintf("%v is broadcasting requestvote\n", rf.me)
	if rf.state != STATE_CANDIDATE {
		return
	}
	args := &RequestVoteArgs{CandidateIndex: rf.me, Term: rf.currentTerm, LastLogTerm: rf.GetLastTerm(), LastLogIndex: rf.GetLastIndex()}
	chanApply := make(chan int, len(rf.peers)-1)
	for k, _ := range rf.peers {
		if k != rf.me {
			chanApply <- k
			go func() {
				reply := &RequestVoteReply{Term: rf.currentTerm}
				target := <-chanApply
				rf.sendRequestVote(target, args, reply)
			}()
		}
	}
	//DPrintf("%v's broadcasting end\n", rf.me)
}

func (rf *Raft) BroadcastAppendEntries() {
	if rf.state != STATE_LEADER {
		return
	}
	for k, _ := range rf.peers {
		if k != rf.me {
			logIndex := rf.nextIndex[k]
			var logTerm int
			if logIndex < rf.GetLastIndex() {
				logTerm = rf.logs[logIndex].Term
			} else {
				logTerm = -1
			}
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderIndex: rf.me, LeaderCommit: rf.commitIndex, LastLogIndex: logIndex, LastLogTerm: logTerm}
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(k, args, reply)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) Loop() {
	//DPrintf("%v's loop started", rf.me)
	Timeout := 0
	for {
		Timeout = rand.Intn(300) + 800
		//DPrintf("set %v's timeout to %v", rf.me, Timeout)
		//DPrintf("STATE:%v", rf.state)
		switch rf.state {
		case STATE_FOLLOWER:
			select {
			case <-rf.chanHeartBeat:
			case <-time.After(time.Duration(Timeout) * time.Millisecond):
				if rf.state != STATE_LEADER {
					rf.mu.Lock()
					//DPrintf("set %v's state to candidate", rf.me)
					rf.state = STATE_CANDIDATE
					rf.mu.Unlock()
				}
			}
		case STATE_LEADER:
			rf.LeaderState()
		case STATE_CANDIDATE:
			rf.CandidateState()
		}
	}
}

func (rf *Raft) DoingStateMachine(applyCh chan ApplyMsg) {
	for {
		time.Sleep(30 * time.Millisecond)
		if rf.lastApplied < rf.commitIndex {
			go func() {
				rf.mu.Lock()
				lastApplied := rf.lastApplied
				commitIndex := rf.commitIndex
				rf.lastApplied = commitIndex
				rf.mu.Unlock()
				if rf.GetLastIndex() < commitIndex {
					return
				}
				time.Sleep(10 * time.Millisecond)
				for i := lastApplied + 1; i <= commitIndex; i++ {
					msg := new(ApplyMsg)
					msg.Index = i
					msg.Command = rf.logs[i].Command
					applyCh <- *msg
				}
			}()
		}
	}
}

func (rf *Raft) CandidateState() {
	//DPrintf("%v's sleep started", rf.me)
	time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
	//DPrintf("%v's sleep end", rf.me)
	select {
	case <-rf.chanBecomeLeader:
	case <-rf.chanHeartBeat:
		rf.state = STATE_FOLLOWER
		return
	default:
	}
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.mu.Unlock()
	rf.BroadcastRequestVote()
	select {
	case newLeader := <-rf.chanBecomeLeader:
		if newLeader {
			rf.state = STATE_LEADER
			rf.mu.Lock()
			rf.nextIndex = []int{}
			for range rf.peers {
				rf.nextIndex = append(rf.nextIndex, rf.GetLastIndex())
			}
			rf.mu.Unlock()
			go rf.BroadcastAppendEntries()
			return
		}
	case <-time.After(time.Duration(rand.Intn(300)) * time.Millisecond):
		if rf.state != STATE_LEADER {
			rf.state = STATE_FOLLOWER
		}
		return
	}
}

func (rf *Raft) LeaderState() {
	time.Sleep(10 * time.Millisecond)
	if rf.lastApplied == rf.commitIndex {
		time.Sleep(25 * time.Millisecond)
	}
	go rf.BroadcastAppendEntries()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = STATE_FOLLOWER
	//rf.logs = append(rf.logs, Log{Term: 0})
	rf.chanBecomeLeader = make(chan bool)
	rf.chanHeartBeat = make(chan bool)
	rf.chanCommand = make(chan interface{}, 10)
	//DPrintf("%v inited\n", me)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Loop()
	go rf.DoingStateMachine(applyCh)
	return rf
}
