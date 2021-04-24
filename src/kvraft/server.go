package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	Error Err
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OpType
	Key       string
	Value     string
	CommandID int
	ClientID  int
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
	maxClientCommandID	map[int]int
	seenReplies map[int]OpResult
	persister 	*raft.Persister
}

func (kv *KVServer) CombineID(clientID int,commandID int) int64{
	var ret int64
	ret += int64(clientID)
	ret <<= 32
	ret += int64(commandID)
	return ret
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	maxCommandID,ok := kv.maxClientCommandID[args.ClientID]
	if ok && maxCommandID > args.CommandID {
		reply.Err = kv.seenReplies[args.ClientID].Error
		reply.Value = kv.seenReplies[args.ClientID].Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// Your code here.
	op := Op{
		OpType:    GET,
		Key:       args.Key,
		Value:     "",
		CommandID: args.CommandID,
		ClientID: args.ClientID,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	kv.mu.Lock()
	combineID := kv.CombineID(args.ClientID,args.CommandID)
	ch, ok := kv.chanMap[combineID]
	if !ok {
		ch = make(chan OpResult)
		kv.chanMap[combineID] = ch
	} else {
		// there is already such a command in this server
		reply.Err = ErrFailed
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	select {
	case opResult := <-ch:
		{
			if opResult.Error == ErrWrongLeader {
				panic("should not get ErrWrongLeader from OpResult")
			}
			reply.Err = opResult.Error
			reply.Value = opResult.Value
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		{
			reply.Err = ErrFailed
			reply.Value = ""
		}
	}
	kv.deleteChan(combineID)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	maxCommandID,ok := kv.maxClientCommandID[args.ClientID]
	if ok && maxCommandID > args.CommandID {
		reply.Err = kv.seenReplies[args.ClientID].Error
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
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
		ClientID: args.ClientID,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	combineID := kv.CombineID(args.ClientID,args.CommandID)
	kv.mu.Lock()
	ch, ok := kv.chanMap[combineID]
	if !ok {
		ch = make(chan OpResult)
		kv.chanMap[combineID] = ch
	} else {
		reply.Err = ErrFailed
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	select {
	case opResult := <-ch:
		{
			if opResult.Error == ErrWrongLeader {
				panic("should not get ErrWrongLeader from OpResult")
			}
			reply.Err = opResult.Error
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		{
			reply.Err = ErrFailed
		}
	}
	kv.deleteChan(combineID)
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
		if kv.killed() {
			return
		}
		// check whether to generate a snapshot
		if kv.rf != nil && kv.maxraftstate != -1 && kv.persister.RaftStateSize() + 500 > kv.maxraftstate {
			kv.mu.Lock()
			lastIndex := msg.CommandIndex - 1
			data := kv.genSnapshot()
			DPrintf("S%d making snapshot, with index %d",kv.me,lastIndex)
			kv.rf.Snapshot(lastIndex,data)
			kv.mu.Unlock()
		}
		if msg.CommandValid {
			op, assertOk := msg.Command.(Op)
			if !assertOk {
				panic("invalid command")
			}
			DPrintf("S%d state machine get index %d", kv.me,msg.CommandIndex)
			kv.mu.Lock()
			//if op.OpType == APPEND {
			//	println(kv.me,"applying append ",op.Value, "\n")
			//}

			maxCommandID,ok := kv.maxClientCommandID[op.ClientID]
			var opResult OpResult
			ch, hasCh := kv.chanMap[kv.CombineID(op.ClientID,op.CommandID)]
			if ok && maxCommandID > op.CommandID {
				opResult = kv.seenReplies[op.ClientID]
			} else {
				opResult.Error = ErrFailed
				opResult.Value = ""
				if op.OpType == GET {
					value, ok := kv.kvMap[op.Key]
					if ok {
						opResult.Error = OK
						opResult.Value = value
					} else {
						opResult.Error = ErrNoKey
						opResult.Value = ""
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
					opResult.Error = OK
				}
				kv.seenReplies[op.ClientID] = opResult
				kv.maxClientCommandID[op.ClientID]++
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
			//println(kv.me,"receive snapshot apply")
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot) {
				buffer := bytes.NewBuffer(msg.Snapshot)
				decoder := labgob.NewDecoder(buffer)
				kv.mu.Lock()
				if decoder.Decode(&kv.maxClientCommandID) != nil || decoder.Decode(&kv.seenReplies) != nil || decoder.Decode(&kv.kvMap) != nil {
					panic("decode failed in server")
				}
				kv.mu.Unlock()
			}
		} else {
			panic("applyCh receives an invalid msg")
		}
	}
}

func (kv *KVServer) genSnapshot() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.maxClientCommandID)
	encoder.Encode(kv.seenReplies)
	encoder.Encode(kv.kvMap)
	data := buffer.Bytes()
	return data
}

func (kv *KVServer) deleteChan(id int64) {
	kv.mu.Lock()
	delete(kv.chanMap,id)
	kv.mu.Unlock()
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/Value service.
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
	// read the snapshot and recover

	// You may need initialization code here.
	kv.chanMap = make(map[int64]chan OpResult)
	kv.kvMap = make(map[string]string)
	kv.seenReplies = make(map[int]OpResult)
	kv.maxClientCommandID = make(map[int]int)
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Apply()
	//fmt.Printf("server %d start with map %v\n", kv.me, kv.kvMap)
	return kv
}
