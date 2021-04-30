package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"time"

	//"fmt"
	"sync"
)
var objCnt int = 1
var mu      sync.Mutex

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
	lastOKCommandID int64
	counter int
	getCounter int
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
	ck.getCounter = 2147483647
	mu.Lock()
	ck.me = objCnt
	objCnt++
	mu.Unlock()
	return ck
}

func (ck *Clerk) GetNextCounter() int{
	ck.mu.Lock()
	ret := ck.counter
	ck.counter++
	ck.mu.Unlock()
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
	commandID := ck.GetNextGetCounter()
	args := &GetArgs{
		Key: key,
		CommandID: commandID,
		ClientID: ck.me,
	}
	for true {
		for ID := ck.leaderID; ID != ck.leaderID + len(ck.servers); ID++ {
			serverID := ID % len(ck.servers)
			fmt.Printf("Send Get %s to S%d\n",key,serverID)
			DPrintf("Send Get %s to S%d",key,serverID)
			reply := &GetReply{}
			ok := ck.servers[serverID].Call("KVServer.Get", args, reply)
			if ok{
				switch reply.Err {
				case OK:
					{
						fmt.Printf("%d Receive %s from S%d\n",ck.me,reply.Value,serverID)
						ck.leaderID = serverID
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
		time.Sleep(time.Duration(600) * time.Millisecond)
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

	//fmt.Printf("client %d commandID %d\n",ck.me,commandID)
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		CommandID: commandID,
		ClientID: ck.me,
	}
	for true {
		for ID := ck.leaderID; ID != ck.leaderID + len(ck.servers); ID++ {
			serverID := ID % len(ck.servers)
			//fmt.Printf("Send PutAppend key:%s Value%s to S%d\n",key,value,serverID)
			DPrintf("Send PutAppend key:%s Value%s to S%d",key,value,serverID)
			reply := &PutAppendReply{}
			ok := ck.servers[serverID].Call("KVServer.PutAppend", args, reply)
			if ok {
				if reply.Err == OK {
					//fmt.Printf("%d Receive %s from S%d commandID%d\n",ck.me,reply.Err,serverID,commandID)
					ck.leaderID = serverID
					return
				} else {
					if reply.Err == ErrWrongLeader {
						fmt.Printf("wrong leader S%d\n",serverID)
					}
					continue
				}
			}
		}
		time.Sleep(time.Duration(600) * time.Millisecond)
	}
	panic("code should not reach here")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) GetNextGetCounter() int {
	ck.mu.Lock()
	ret := ck.getCounter
	ck.getCounter--
	ck.mu.Unlock()
	return ret
}
