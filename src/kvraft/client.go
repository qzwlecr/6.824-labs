package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import mrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	SeenID int64
	mu     sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:    key,
		ID:     nrand(),
		SeenID: ck.SeenID,
	}
	leader := -1
	for {
		if leader < 0 {
			leader = ck.leader
		} else {
			next := leader
			for leader == next {
				leader = mrand.Intn(len(ck.servers))
			}
		}
		var reply GetReply
		ok := ck.servers[leader].Call("RaftKV.Get", &args, &reply)
		if ok {
			if reply.WrongLeader {
				DPrintf("Get (ID:%v) error %v", args.ID, reply.Err)
				continue
			}
			if reply.Err != OK {
				DPrintf("Get (ID:%v) error %v", args.ID, reply.Err)
				continue
			}
			DPrintf("Get (ID:%v) success", args.ID)
			ck.SeenID = args.ID
			ck.leader = leader
			return reply.Value
		} else {
			DPrintf("Get (ID:%v) RPC fail", args.ID)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		ID:     nrand(),
		Op:     op,
		SeenID: ck.SeenID,
	}
	leader := -1
	for {
		if leader < 0 {
			leader = ck.leader
		} else {
			record := leader
			for leader == record {
				leader = mrand.Intn(len(ck.servers))
			}
		}
		var reply PutAppendReply
		ok := ck.servers[leader].Call("RaftKV.PutAppend", &args, &reply)
		if ok {
			if reply.WrongLeader {
				DPrintf("PutAppend (ID:%v) error %v", args.ID, reply.Err)
				continue
			}
			if reply.Err != OK {
				DPrintf("PutAppend (ID:%v) error %v", args.ID, reply.Err)
				continue
			}
			DPrintf("PutAppend (ID:%v) success", args.ID)
			ck.SeenID = args.ID
			ck.leader = leader
			return
		} else {
			DPrintf("PutAppend (ID:%v) RPC fail", args.ID)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
