package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Kind string
	Args interface{}
}

type OpWithAR struct {
	Kind  string
	Args  interface{}
	Reply interface{}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	db      map[string]string
	ack     map[int64]int
	message map[int]chan OpWithAR
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	entry := Op{
		Kind: "Get",
		Args: *args,
	}
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	if _, ok := kv.message[index]; !ok {
		kv.message[index] = make(chan OpWithAR, 1)
	}
	chanMsg := kv.message[index]
	kv.mu.Unlock()
	select {
	case msg := <-chanMsg:
		if msgArgs, ok := msg.Args.(GetArgs); !ok {
			reply.Err = "Get reply Error"
		} else {
			if args.ID != msgArgs.ID || args.RequestID != msgArgs.RequestID {
				reply.Err = "Conflict Server"
			} else {
				*reply = msg.Reply.(GetReply)
			}
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = "Timeout"
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{
		Kind: "PutAppend",
		Args: *args,
	}
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	if _, ok := kv.message[index]; !ok {
		kv.message[index] = make(chan OpWithAR, 1)
	}
	chanMsg := kv.message[index]
	kv.mu.Unlock()
	select {
	case msg := <-chanMsg:
		if msgArgs, ok := msg.Args.(PutAppendArgs); !ok {
			reply.Err = "Get reply Error"
		} else {
			if args.ID != msgArgs.ID || args.RequestID != msgArgs.RequestID {
				reply.Err = "Conflict Server"
			} else {
				reply.Err = msg.Reply.(PutAppendReply).Err
			}
		}
	case <-time.After(1000 * time.Millisecond):
		reply.Err = "Timeout"
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	DPrintf("%v has been killed", kv.me)
	kv.rf.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func (kv *RaftKV) ReadyToApply() {
	for msg := range kv.applyCh{
		op := msg.Command.(Op)
		var ret OpWithAR
		if op.Kind == "Get" {
			args := op.Args.(GetArgs)
			ret.Args = args
		}else{
			args := op.Args.(PutAppendArgs)
			ret.Args = args
		}
		ret.Kind = op.Kind
		ret.Reply = kv.Apply(op)
		kv.mu.Lock()
		if _, ok := kv.message[msg.Index]; !ok {
			kv.message[msg.Index] = make(chan OpWithAR, 1)
		} else {
			select {
			case <-kv.message[msg.Index]:
			default:
			}
		}
		kv.message[msg.Index] <- ret
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Apply(op Op) interface{} {
	switch op.Args.(type) {
	case GetArgs:
		reply := GetReply{}
		args := op.Args.(GetArgs)
		if value, ok := kv.db[args.Key]; !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = value
		}
		return reply
	case PutAppendArgs:
		reply := PutAppendReply{}
		args := op.Args.(PutAppendArgs)
		if value, ok := kv.ack[args.ID]; !ok || value < args.RequestID {
			kv.ack[args.ID] = args.RequestID
			if args.Op == "Put" {
				kv.db[args.Key] = args.Value
			} else {
				kv.db[args.Key] += args.Value
			}
		}
		reply.Err = OK
		return reply
	}
	return nil
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetReply{})

	DPrintf("Start (ID:%v) KVServer", me)
	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.message = make(map[int]chan OpWithAR)
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	go kv.ReadyToApply()
	return kv
}
