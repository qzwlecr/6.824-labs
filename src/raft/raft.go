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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	votedFor    int
	logs        []Log

	commitIndex int
	lastApplied int

	state int

	nextIndex  []int
	matchIndex []int

	chanHeartBeat chan bool

	chanCommit chan bool

	chanGrantVote chan bool

	chanBecomeLeader chan bool
	voteCount        int

	chanApply chan ApplyMsg
}

type Log struct {
	Command interface{}
	Term    int
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == STATE_LEADER
	return term, isleader
}

func (rf *Raft) GetLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) GetLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		//rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		DPrintf("set %v's state to follower3", rf.me)
		rf.votedFor = -1
		//rf.mu.Unlock()
	}
	reply.Term = rf.currentTerm
	if rf.GetLastTerm() < args.LastLogTerm || (rf.GetLastTerm() == args.LastLogTerm && rf.GetLastIndex() <= args.LastLogIndex) {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateIndex {
			//rf.mu.Lock()
			rf.chanGrantVote <- true
			rf.state = STATE_FOLLOWER
			DPrintf("set %v's state to follower4", rf.me)
			reply.VoteGranted = true
			//DPrintf("%v vote %v", rf.me, args.CandidateIndex)
			rf.votedFor = args.CandidateIndex
			//rf.mu.Unlock()
		}
	}
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	defer rf.persist()
	if ok {
		if rf.state != STATE_CANDIDATE || args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			//rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			DPrintf("set %v's state to follower5", rf.me)
			rf.votedFor = -1
			//rf.mu.Unlock()
		}
		if reply.VoteGranted {
			rf.mu.Lock()
			rf.voteCount++
			rf.mu.Unlock()
			//DPrintf("%v's voteCount = %v", rf.me, rf.voteCount)
			if rf.state == STATE_CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				rf.state = STATE_FOLLOWER
				DPrintf("set %v's state to follower6", rf.me)
				rf.chanBecomeLeader <- true
			}
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
	for k := range rf.peers {
		if k != rf.me && rf.state == STATE_CANDIDATE {
			go func(k int) {
				reply := &RequestVoteReply{}
				rf.sendRequestVote(k, args, reply)
			}(k)
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderIndex  int
	LastLogIndex int
	LastLogTerm  int
	LeaderCommit int
	Entries      []Log
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.persist()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.GetLastIndex() + 1
		return
	}
	rf.chanHeartBeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		DPrintf("set %v's state to follower7", rf.me)
	}
	reply.Term = args.Term
	if args.LastLogIndex > rf.GetLastIndex() {
		reply.NextIndex = rf.GetLastIndex() + 1
		return
	}
	term := rf.logs[args.LastLogIndex].Term
	if args.LastLogTerm != term {
		for i := args.LastLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != term {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}
	rf.logs = rf.logs[:args.LastLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)
	reply.Success = true
	reply.NextIndex = rf.GetLastIndex() + 1
	if args.LeaderCommit > rf.commitIndex {
		last := rf.GetLastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	defer rf.persist()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			DPrintf("set %v's state to follower1", rf.me)
			rf.votedFor = -1
			return ok
		}
		if rf.state != STATE_LEADER || args.Term != rf.currentTerm {
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}

	}
	return ok
}

func (rf *Raft) BroadcastAppendEntries() {
	if rf.state != STATE_LEADER {
		return
	}
	//DPrintf("%v is broadcasting appendentries\n", rf.me)
	commit := rf.commitIndex
	last := rf.GetLastIndex()
	for i := last; i >= rf.commitIndex; i-- {
		serverCount := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.logs[i].Term == rf.currentTerm {
				serverCount++
			}
		}
		if serverCount*2 > len(rf.peers) {
			commit = i
			break
		}
	}
	if commit != rf.commitIndex {
		rf.chanCommit <- true
		rf.commitIndex = commit
	}
	for k := range rf.peers {
		if k != rf.me && rf.state == STATE_LEADER {
			logIndex := rf.nextIndex[k] - 1
			if logIndex > rf.GetLastIndex() {
				continue
			}
			//DPrintf("%v %v Log index: %v with larges logindex = %v", rf.me, k, logIndex, rf.GetLastIndex())
			logTerm := rf.logs[logIndex].Term
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderIndex: rf.me, LeaderCommit: rf.commitIndex, LastLogIndex: logIndex, LastLogTerm: logTerm}
			args.Entries = make([]Log, len(rf.logs[args.LastLogIndex+1:]))
			copy(args.Entries, rf.logs[args.LastLogIndex+1:])
			reply := &AppendEntriesReply{}
			go func(args *AppendEntriesArgs, k int) {
				rf.sendAppendEntries(k, args, reply)
			}(args, k)
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER
	if isLeader {
		index = rf.GetLastIndex() + 1
		rf.logs = append(rf.logs, Log{Term: term, Command: command})
		rf.persist()
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

func (rf *Raft) Loop() {
	for {
		switch rf.state {
		case STATE_FOLLOWER:
			rf.FollowerState()
		case STATE_LEADER:
			rf.LeaderState()
		case STATE_CANDIDATE:
			rf.CandidateState()
		}
	}
}

func (rf *Raft) FollowerState() {
	select {
	case <-rf.chanHeartBeat:
	case <-rf.chanGrantVote:
	case <-time.After(time.Duration(rand.Intn(500)+800) * time.Millisecond):
		DPrintf("set %v's state to candidate", rf.me)
		rf.state = STATE_CANDIDATE
	}
}

func (rf *Raft) CandidateState() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()
	go rf.BroadcastRequestVote()
	select {
	case <-rf.chanBecomeLeader:
		rf.state = STATE_LEADER
		DPrintf("set %v's state to leader", rf.me)
		rf.nextIndex = []int{}
		rf.matchIndex = []int{}
		for range rf.peers {
			rf.nextIndex = append(rf.nextIndex, rf.GetLastIndex()+1)
			rf.matchIndex = append(rf.matchIndex, 0)
		}
		rf.BroadcastAppendEntries()
	case <-time.After(time.Duration(rand.Intn(500)+800) * time.Millisecond):
	case <-rf.chanHeartBeat:
		rf.state = STATE_FOLLOWER
	}
}

func (rf *Raft) LeaderState() {
	go rf.BroadcastAppendEntries()
	time.Sleep(50 * time.Millisecond)
}

func (rf *Raft) DoingCommit(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.chanCommit:
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{Index: i, Command: rf.logs[i].Command}
				DPrintf("%v applied", msg)
				applyCh <- msg
				rf.lastApplied = i
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = STATE_FOLLOWER
	DPrintf("set %v's state to follower2", rf.me)
	rf.logs = append(rf.logs, Log{Term: 0})
	rf.chanBecomeLeader = make(chan bool)
	rf.chanHeartBeat = make(chan bool)
	rf.chanCommit = make(chan bool)
	rf.chanGrantVote = make(chan bool)
	rf.chanApply = make(chan ApplyMsg)
	//DPrintf("%v inited\n", me)
	rf.readPersist(persister.ReadRaftState())
	go rf.Loop()
	go rf.DoingCommit(applyCh)
	return rf
}
