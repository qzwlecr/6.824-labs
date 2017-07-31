package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	leaderID  int
	ID        int64
	requestID int
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
	ck.leaderID = 0
	ck.requestID = 0
	ck.ID = nrand()
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:       key,
		ID:        ck.ID,
		RequestID: ck.requestID,
	}
	ck.requestID++
	ret := ""
	for i := ck.leaderID; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				ck.leaderID = i
				if reply.Err == OK {
					ret = reply.Value
					DPrintf("Get (ID:%v) success", args.ID)
					break
				} else {
					DPrintf("Get (ID:%v) error: %v", args.ID, reply.Err)
				}
			} else {
				DPrintf("Get (ID:%v) error: %v is Not Leader", args.ID, i)
			}
		} else {
			DPrintf("Get (ID:%v) error: RPC Error", args.ID)
		}
	}
	DPrintf("return : %v", ret)
	return ret
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
	DPrintf("				receive command %v %v %v", key, value, op)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ID:        ck.ID,
		RequestID: ck.requestID,
	}
	ck.requestID++
	for i := ck.leaderID; ; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				ck.leaderID = i
				if reply.Err == OK {
					DPrintf("PutAppend (ID:%v) success", args.ID)
					break
				} else {
					DPrintf("PutAppend (ID:%v) error: %v", args.ID, reply.Err)
				}
			} else {
				DPrintf("PutAppend (ID:%v) error: %v is Not Leader", args.ID, i)
			}
		} else {
			DPrintf("PutAppend (ID:%v) error: RPC Error", args.ID)
		}
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
