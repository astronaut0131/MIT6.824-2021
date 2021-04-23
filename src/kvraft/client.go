package kvraft

import (
	"6.824/labrpc"
	"sync"
)
var objCnt int = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
	lastOKCommandID int64
	counter int
	me 		int
	mu      sync.Mutex
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderID = 0
	ck.lastOKCommandID = -1
	ck.counter = 0
	ck.me = objCnt
	objCnt++
	return ck
}

func (ck *Clerk) GetNextCounter() int64{
	var ret int64
	ck.mu.Lock()
	ret += int64(ck.counter)
	ret <<= 32
	ck.counter++
	ck.mu.Unlock()
	ret += int64(ck.me)
	return ret
}

//
// fetch the current Value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Get(key string) string {
	commandID := ck.GetNextCounter()
	args := &GetArgs{
		Key: key,
		CommandID: commandID,
		LastOKCommandID: ck.lastOKCommandID,
	}
	for true {
		for ID := ck.leaderID; ID != ck.leaderID + len(ck.servers); ID++ {
			serverID := ID % len(ck.servers)
			DPrintf("Send Get %s to S%d",key,serverID)
			reply := &GetReply{}
			ok := ck.servers[serverID].Call("KVServer.Get", args, reply)
			if ok{
				switch reply.Err {
				case OK:
					{
						ck.leaderID = serverID
						ck.lastOKCommandID = commandID
						return reply.Value
					}
				case ErrNoKey:
					return ""
				default:
					DPrintf("Get Error %s",reply.Err)
					continue
				}
			}
		}
	}
	panic("code should not reach here")
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	commandID := ck.GetNextCounter()
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		CommandID: commandID,
		LastOKCommandID: ck.lastOKCommandID,
	}
	for true {
		for ID := ck.leaderID; ID != ck.leaderID + len(ck.servers); ID++ {
			serverID := ID % len(ck.servers)
			DPrintf("Send PutAppend key:%s Value%s to S%d",key,value,serverID)
			reply := &PutAppendReply{}
			ok := ck.servers[serverID].Call("KVServer.PutAppend", args, reply)
			if ok {
				DPrintf("Receive %s from S%d",reply.Err,serverID)
				if reply.Err == OK {
					ck.leaderID = serverID
					ck.lastOKCommandID = commandID
					return
				} else {
					continue
				}
			}
		}
	}
	panic("code should not reach here")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
