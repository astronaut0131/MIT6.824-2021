package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"time"

	//"time"

	//"fmt"
	//"time"

	//"fmt"
	//"time"

	//"fmt"
	"log"
	"sync"
	"sync/atomic"
	//"time"
)

const Debug = false
const Timeout = 700

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	GET    OpType = 0
	PUT    OpType = 1
	APPEND OpType = 2
)

type OpResult struct {
	error Err
	value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OpType
	Key       string
	Value     string
	CommandID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap       map[string]string
	chanMap     map[int64]chan OpResult
	seenReplies map[int64]OpResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType:    GET,
		Key:       args.Key,
		Value:     "",
		CommandID: args.CommandID,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	kv.mu.Lock()
	ch, ok := kv.chanMap[args.CommandID]
	if !ok {
		ch = make(chan OpResult)
		kv.chanMap[args.CommandID] = ch
	}
	kv.mu.Unlock()
	select {
	case opResult := <-ch:
		{
			if opResult.error == ErrWrongLeader {
				panic("should not get ErrWrongLeader from OpResult")
			}
			reply.Err = opResult.error
			reply.Value = opResult.value
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		{
			reply.Err = ErrFailed
			reply.Value = ""
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var opType OpType
	if args.Op == "Put" {
		opType = PUT
	} else {
		opType = APPEND
	}
	op := Op{
		OpType:    opType,
		Key:       args.Key,
		Value:     args.Value,
		CommandID: args.CommandID,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch, ok := kv.chanMap[args.CommandID]
	if !ok {
		ch = make(chan OpResult)
		kv.chanMap[args.CommandID] = ch
	}
	kv.mu.Unlock()
	select {
	case opResult := <-ch:
		{
			if opResult.error == ErrWrongLeader {
				panic("should not get ErrWrongLeader from OpResult")
			}
			reply.Err = opResult.error
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		{
			reply.Err = ErrFailed
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Apply() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op, assertOk := msg.Command.(Op)
			if !assertOk {
				panic("invalid command")
			}
			DPrintf("state machine get index %d", msg.CommandIndex)
			kv.mu.Lock()
			opResult, seen := kv.seenReplies[op.CommandID]
			ch, hasCh := kv.chanMap[op.CommandID]
			if !seen {
				opResult.error = ErrFailed
				opResult.value = ""
				if op.OpType == GET {
					value, ok := kv.kvMap[op.Key]
					if ok {
						opResult.error = OK
						opResult.value = value
					} else {
						opResult.error = ErrNoKey
						opResult.value = ""
					}
				} else {
					value, ok := kv.kvMap[op.Key]
					if !ok {
						kv.kvMap[op.Key] = op.Value
					} else {
						if op.OpType == APPEND {
							kv.kvMap[op.Key] = value + op.Value
						} else {
							kv.kvMap[op.Key] = op.Value
						}
					}
					opResult.error = OK
				}
				kv.seenReplies[op.CommandID] = opResult
			}
			kv.mu.Unlock()
			if hasCh {
				select {
				case ch <- opResult:
					{

					}
				case <-time.After(time.Duration(100) * time.Millisecond):
					{

					}
				}
			}
		} else if msg.SnapshotValid {

		} else {
			panic("applyCh receives an invalid msg")
		}
	}
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.chanMap = make(map[int64]chan OpResult)
	kv.kvMap = make(map[string]string)
	kv.seenReplies = make(map[int64]OpResult)
	go kv.Apply()
	return kv
}
