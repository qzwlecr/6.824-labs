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
	Kind   string
	Key    string
	Value  string
	ID     int64
	SeenID int64
}

type RaftKV struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	db      map[string]string
	opMap   map[int64]Op
	opMu	sync.RWMutex
	chanMap map[int64]chan Op
	chanMu	sync.Mutex
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	entry := Op{
		Kind:   "Get",
		Key:    args.Key,
		ID:     args.ID,
		SeenID: args.SeenID,
	}
	kv.opMu.RLock()
	if lastOp, exist := kv.opMap[args.ID]; exist {
		kv.opMu.RUnlock()
		reply.WrongLeader = false
		reply.Err = OK
		reply.Value = lastOp.Value
		return
	}
	kv.opMu.RUnlock()
	resultCh := make(chan Op, 1)
	kv.chanMu.Lock()
	if _, exist := kv.chanMap[args.ID]; !exist {
		kv.chanMap[args.ID] = resultCh
	} else {
		resultCh = kv.chanMap[args.ID]
		DPrintf("chan Op for ID %v exists", args.ID)
	}
	kv.chanMu.Unlock()
	_, _, isLeader := kv.rf.Start(entry)
	reply.WrongLeader = !isLeader
	if reply.WrongLeader {
		reply.Err = "not a leader"
		kv.chanMu.Lock()
		delete(kv.chanMap, args.ID)
		kv.chanMu.Unlock()
		return
	}
	select {
	case resultOp := <-resultCh:
		reply.Err = OK
		reply.Value = resultOp.Value
		return
	case <-time.After(time.Duration(1000) * time.Millisecond):
		reply.Err = "timeout"
		kv.chanMu.Lock()
		delete(kv.chanMap, args.ID)
		kv.chanMu.Unlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{
		Kind:   args.Op,
		Key:    args.Key,
		Value:  args.Value,
		ID:     args.ID,
		SeenID: args.SeenID,
	}
	kv.opMu.RLock()
	if _, exist := kv.opMap[args.ID]; exist {
		kv.opMu.RUnlock()
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
	kv.opMu.RUnlock()
	resultCh := make(chan Op, 1)
	kv.chanMu.Lock()
	if _, exist := kv.chanMap[args.ID]; !exist {
		kv.chanMap[args.ID] = resultCh
	} else {
		DPrintf("chan Op for ID %v exists", args.ID)
	}
	kv.chanMu.Unlock()
	index, _, isLeader := kv.rf.Start(entry)
	DPrintf("Response (ID:%v) server RPC ID: %v, Index: %v, isLeader: %v", kv.me, args.ID, index, isLeader)
	reply.WrongLeader = !isLeader
	if reply.WrongLeader {
		reply.Err = "not a leader"
		kv.chanMu.Lock()
		delete(kv.chanMap, args.ID)
		kv.chanMu.Unlock()
		return
	}
	select {
	case <-resultCh:
		reply.Err = OK
		return
	case <-time.After(time.Duration(1000) * time.Millisecond):
		reply.Err = "timeout"
		kv.chanMu.Lock()
		delete(kv.chanMap, args.ID)
		kv.chanMu.Unlock()
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

func (kv *RaftKV) DoingStateMachine() {
	for msg := range kv.applyCh {
		op := msg.Command.(Op)
		kv.opMu.Lock()
		delete(kv.opMap, op.SeenID)
		kv.opMu.Unlock()
		if lastOp, exist := kv.opMap[op.ID]; exist {
			DPrintf("Found duplicated op: %v", lastOp)
			op.Value = lastOp.Value
		} else {
			switch op.Kind {
			case "Get":
				op.Value = kv.db[op.Key]
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				{
					if _, exist := kv.db[op.Key]; exist {
						kv.db[op.Key] += op.Value
					} else {
						kv.db[op.Key] = op.Value
					}
				}
				kv.opMu.Lock()
				kv.opMap[op.ID] = op
				kv.opMu.Unlock()
			}
		}
		kv.chanMu.Lock()
		if ch, exist := kv.chanMap[op.ID]; exist {
			delete(kv.chanMap, op.SeenID)
			kv.chanMu.Unlock()
			go func() {
				ch <- op
			}()
		} else {
			kv.chanMu.Unlock()
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	DPrintf("Start (ID:%v) KVServer", me)
	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.db = make(map[string]string)
	kv.opMap = make(map[int64]Op)
	kv.chanMap = make(map[int64]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.DoingStateMachine()

	return kv
}
