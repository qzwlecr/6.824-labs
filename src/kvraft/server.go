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
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	db      map[string]string
	opMap   map[int64]int64
	chanMap map[int]chan Op
}

func (kv *RaftKV) AppendEntries(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}
	ch, ok := kv.chanMap[index]
	if !ok {
		ch = make(chan Op)
		kv.chanMap[index] = ch
	}
	select {
	case op := <-ch:
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	entry := Op{
		Kind:   "Get",
		Key:    args.Key,
		ID:     args.ID,
		SeenID: args.SeenID,
	}
	ok := kv.AppendEntries(entry)
	kv.mu.Lock()
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false

		reply.Err = OK
		reply.Value = kv.db[args.Key]
		kv.opMap[args.ID] = args.SeenID
	}
	kv.mu.Unlock()

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{
		Kind:   args.Op,
		Key:    args.Key,
		Value:  args.Value,
		ID:     args.ID,
		SeenID: args.SeenID,
	}
	ok := kv.AppendEntries(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
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
		kv.mu.Lock()
		op := msg.Command.(Op)
		lastID, ok := kv.opMap[op.ID]
		if !ok || lastID < op.SeenID {
			switch op.Kind {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			}
			kv.opMap[op.ID] = op.SeenID
		}else{
			DPrintf("Found duplicated op: %v",msg)
		}
		if ch, ok := kv.chanMap[msg.Index];ok{
			select {
			case <-kv.chanMap[msg.Index]:
			default:
			}
			go func(){
			ch <- op
			}()
		} else {
			kv.chanMap[msg.Index] = make(chan Op)
		}
		kv.mu.Unlock()
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.db = make(map[string]string)
	kv.opMap = make(map[int64]int64)
	kv.chanMap = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.DoingStateMachine()

	return kv
}
