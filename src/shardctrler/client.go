package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

var globalClientID int = 0
var globalLock sync.Mutex
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientID int
	commandID int
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
	// Your code here.
	globalLock.Lock()
	ck.clientID = globalClientID
	globalClientID++
	globalLock.Unlock()
	ck.commandID = 0
	return ck
}
func (ck *Clerk)getNextCommandID() int{
	nextCommandID := ck.commandID
	ck.commandID++
	return nextCommandID
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientID = ck.clientID
	args.CommandID = ck.getNextCommandID()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			Debug(dInfo,"client%d send query",ck.clientID)
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				Debug(dInfo,"client%d query ok",ck.clientID)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID = ck.clientID
	args.CommandID = ck.getNextCommandID()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			Debug(dInfo,"client%d send join",ck.clientID)
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				Debug(dInfo,"client%d join ok",ck.clientID)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.clientID
	args.CommandID = ck.getNextCommandID()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			Debug(dInfo,"client%d send leave",ck.clientID)
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				Debug(dInfo,"client%d leave ok",ck.clientID)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.clientID
	args.CommandID = ck.getNextCommandID()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			Debug(dInfo,"client%d send move",ck.clientID)
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				Debug(dInfo,"client%d move ok",ck.clientID)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
