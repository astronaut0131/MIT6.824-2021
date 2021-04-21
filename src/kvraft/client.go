package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
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
	// You'll have to add code here.
	ck.leaderID = 0
	return ck
}

const Timeout int = 2000
//
// fetch the current value for a key.
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
	commandID := nrand()
	args := &GetArgs{
		Key: key,
		CommandID: commandID,
	}
	for true {
		for ID := ck.leaderID; ID != ck.leaderID + len(ck.servers); ID++ {
			serverID := ID % len(ck.servers)
			DPrintf("Send Get %s to S%d",key,serverID)
			reply := &GetReply{}
			ch := make(chan bool)
			go func() {
				ch <- ck.servers[serverID].Call("KVServer.Get", args, reply)
			}()
			select {
				case <-ch :{
					if reply.Err == OK{
						ck.leaderID = serverID
					}
					switch reply.Err {
					case OK:
						{
							return reply.Value
						}
					case ErrNoKey:
						return ""
					default:
						DPrintf("Get error %s",reply.Err)
						continue
					}
				}
				case <- time.After(time.Duration(Timeout) * time.Millisecond): {
					DPrintf("Get Request to S%d timeout, try another server",serverID)
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
	commandID := nrand()
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		CommandID: commandID,
	}
	for true {
		for ID := ck.leaderID; ID != ck.leaderID + len(ck.servers); ID++ {
			serverID := ID % len(ck.servers)
			DPrintf("Send PutAppend key:%s value%s to S%d",key,value,serverID)
			reply := &PutAppendReply{}
			ch := make(chan bool)
			go func() {
				ch <- ck.servers[serverID].Call("KVServer.PutAppend", args, reply)
			}()
			select {
				case <- ch: {
					DPrintf("Receive %s from S%d",reply.Err,serverID)
					if reply.Err == OK{
						ck.leaderID = serverID
					}
					if reply.Err == OK {
						return
					} else {
						continue
					}
				}
				case <- time.After(time.Duration(Timeout) * time.Millisecond): {
					DPrintf("PutAppend Request to S%d timeout, try another server",serverID)
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
