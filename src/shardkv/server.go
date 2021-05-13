package shardkv

import (
	"6.824/labrpc"
	"bytes"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

import "6.824/shardctrler"
import "6.824/raft"
import "sync"
import "6.824/labgob"

const watchTimeout = 100
const chanWaitTimeout = 800
const internalMigrationID = 2147483647
const internalConfigurationChangeID = 2147483646

type OpType int

const (
	GET    OpType = 0
	PUT    OpType = 1
	APPEND OpType = 2
	ConfigurationChange OpType = 3
	Migration OpType = 4
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OpType
	Key       string
	Value     string
	CommandID int
	ClientID  int
	NewConfig shardctrler.Config
	MigrationShardMap	map[string]string
	Shard	  int
	MaxClientCommandID	map[int]int
}

type OpResult struct {
	Err   Err
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	mck    *shardctrler.Clerk
	shards []int // shards id in this group
	dead   int32

	chanMap map[int64]chan OpResult

	maxClientCommandID map[int]int
	persister          *raft.Persister

	kvMap            map[string]string
	config           shardctrler.Config
	configCond       *sync.Cond
	newConfig        *sync.Cond
	lastMsgIndex	 int
}

func (kv *ShardKV) CombineID(clientID int, commandID int) int64 {
	var ret int64
	ret += int64(clientID)
	ret <<= 32
	ret += int64(commandID)
	return ret
}

func (kv *ShardKV) ConfigurationChange(newConfig shardctrler.Config){
	for {
		kv.mu.Lock()
		if kv.config.Num >= newConfig.Num {
			kv.mu.Unlock()
			return
		}
		combineID := kv.CombineID(internalConfigurationChangeID, newConfig.Num)
		ch := make(chan OpResult)
		kv.chanMap[combineID] = ch
		op := Op{
			Key:       "",
			Value:     "",
			CommandID: newConfig.Num,
			ClientID:  internalConfigurationChangeID,
			NewConfig: newConfig,
		}
		op.OpType = ConfigurationChange
		_, _, isLeader := kv.rf.Start(op)
		kv.mu.Unlock()
		if !isLeader {
			time.Sleep(time.Duration(chanWaitTimeout) * time.Millisecond)
		} else {
			select {
			case <-ch:
				{
					return
				}
			case <-time.After(time.Duration(chanWaitTimeout) * time.Millisecond):
				{
					Debug(dInfo, "gid %d server %d broadcast timeout %d", kv.gid, kv.me, newConfig.Num)
				}
			}
		}
	}
}

func (kv *ShardKV) Migration(args *MigrationArgs,reply *MigrationReply) {
	Debug(dInfo,"gid %d server %d receive migration %d shard %d",kv.gid,kv.me,args.ConfigNum,args.Shard)
	kv.mu.Lock()
	Debug(dInfo,"gid %d server %d receive migration %d shard %d get lock ok",kv.gid,kv.me,args.ConfigNum,args.Shard)
	if args.ConfigNum <= kv.config.Num {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	if args.ConfigNum != kv.config.Num + 1 {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	alreadyHasShard := false
	for _,shard := range kv.shards {
		if shard == args.Shard {
			alreadyHasShard = true
			break
		}
	}
	if alreadyHasShard {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	combineID := kv.CombineID(internalMigrationID,args.ConfigNum)
	ch, ok := kv.chanMap[combineID]
	op := Op{
		OpType:            Migration,
		Key:               "",
		Value:             "",
		CommandID:         args.ConfigNum,
		ClientID:          0,
		NewConfig:         shardctrler.Config{},
		MigrationShardMap: args.ShardMap,
		Shard:             args.Shard,
		MaxClientCommandID: args.MaxClientCommandID,
	}

	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		if !ok {
			ch = make(chan OpResult)
			kv.chanMap[combineID] = ch
		}
	}
	kv.mu.Unlock()
	select {
	case opResult := <-ch:
		{
			curTerm, _ := kv.rf.GetState()
			if curTerm == term {
				reply.Err = opResult.Err
			} else {
				reply.Err = ErrWrongLeader
			}
		}
	case <-time.After(time.Duration(chanWaitTimeout) * time.Millisecond):
		{
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *ShardKV) migrateShards(oldShards []int, newShards []int, newConfig shardctrler.Config){
	// migrate out
	Debug(dInfo,"gid %d server %d handling migrate out, Num %d",kv.gid,kv.me,newConfig.Num)
	// switch to new shards
	shardDestinationMap := kv.getShardsDestination(oldShards,newShards,newConfig)
	kv.removeMovedShards(newShards)
	maxClientCommand := make(map[int]int)
	for client,id := range kv.maxClientCommandID {
		maxClientCommand[client] = id
	}
	//keysToDel := make([]string,0)
	for shard,destination := range shardDestinationMap {
		shardMap := make(map[string]string)
		for k,v := range kv.kvMap {
			if key2shard(k) == shard {
				shardMap[k] = v
				//keysToDel = append(keysToDel, k)
			}
		}
		if len(destination) != 0 {
			go kv.sendShardTo(shard, shardMap, newConfig.Num, destination, maxClientCommand)
		}
	}
	//for _,key := range keysToDel {
	//	delete(kv.kvMap,key)
	//}
	//if len(keysToDel) != 0{
	//	kv.doSnapshotWithLock()
	//}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType:    GET,
		Key:       args.Key,
		Value:     "",
		CommandID: args.CommandID,
		ClientID:  args.ClientID,
	}

	combineID := kv.CombineID(args.ClientID, args.CommandID)
	Debug(dInfo,"gid %d server %d receive get %s",kv.gid,kv.me,args.Key)
	kv.mu.Lock()
	inShards := false
	for _, shard := range kv.shards {
		if shard == args.ShardID {
			inShards = true
			break
		}
	}
	if !inShards {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		reply.Value = ""
		return
	}

	_, term, isLeader := kv.rf.Start(op)
	ch, ok := kv.chanMap[combineID]
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		kv.mu.Unlock()
		return
	} else {
		if !ok {
			ch = make(chan OpResult)
			kv.chanMap[combineID] = ch
		}
	}
	kv.mu.Unlock()

	select {
	case opResult := <-ch:
		{
			if opResult.Err == ErrWrongLeader {
				panic("should not get ErrWrongLeader from OpResult")
			}
			curTerm, _ := kv.rf.GetState()
			if curTerm == term {
				reply.Err = opResult.Err
				reply.Value = opResult.Value
				Debug(dInfo,"gid %d server %d replied client %d 's get %s with %s",kv.gid,kv.me,args.ClientID,args.Key,reply.Value)
			} else {
				reply.Err = ErrWrongLeader
				reply.Value = ""
			}
		}
	case <-time.After(time.Duration(chanWaitTimeout) * time.Millisecond):
		{
			reply.Err = ErrWrongLeader
			reply.Value = ""
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Debug(dInfo,"gid %d server %d receive putAppend %s %s\n",kv.gid,kv.me,args.Key,args.Value)
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
		ClientID:  args.ClientID,
	}

	combineID := kv.CombineID(args.ClientID, args.CommandID)
	kv.mu.Lock()

	Debug(dInfo,"gid %d server %d receive putAppend %s %s get lock ok\n",kv.gid,kv.me,args.Key,args.Value)
	inShards := false
	for _, shard := range kv.shards {
		if shard == args.ShardID {
			inShards = true
			break
		}
	}
	if !inShards {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	maxCommandID, ok := kv.maxClientCommandID[args.ClientID]
	if !ok {
		kv.maxClientCommandID[args.ClientID] = 0
		maxCommandID = 0
	}
	if args.CommandID < maxCommandID {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	_, term, isLeader := kv.rf.Start(op)
	ch, ok := kv.chanMap[combineID]
	if !isLeader {
		Debug(dInfo,"gid %d server %d reply putAppend %s %s because wrong leader\n",kv.gid,kv.me,args.Key,args.Value)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		if !ok {
			ch = make(chan OpResult)
			kv.chanMap[combineID] = ch
		}
	}
	kv.mu.Unlock()
	select {
	case opResult := <-ch:
		{
			if opResult.Err == ErrWrongLeader {
				panic("should not get ErrWrongLeader from OpResult")
			}
			curTerm, _ := kv.rf.GetState()
			if curTerm == term {
				reply.Err = opResult.Err
			} else {

				Debug(dInfo,"gid %d server %d reply putAppend %s %s wrong term\n",kv.gid,kv.me,args.Key,args.Value)
				reply.Err = ErrWrongLeader
			}
		}
	case <-time.After(time.Duration(chanWaitTimeout) * time.Millisecond):
		{

			Debug(dInfo,"gid %d server %d reply putAppend %s %s timeout\n",kv.gid,kv.me,args.Key,args.Value)
			reply.Err = ErrWrongLeader
		}
	}
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) isSameShardingArray(oldShards []int, newShards []int) bool {
	if len(oldShards) != len(newShards) {
		return false
	}
	sort.Ints(oldShards)
	sort.Ints(newShards)
	for i := 0; i < len(oldShards); i++ {
		if oldShards[i] != newShards[i] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) configurationWatcher() {
	for !kv.killed() {
		kv.mu.Lock()
		nextConfigNum := kv.config.Num+ 1
		kv.mu.Unlock()
		newConfig := kv.mck.Query(nextConfigNum)
		newShards := kv.getShardsFromConfig(newConfig)
		shouldSnapshot := false
		if newConfig.Num == nextConfigNum {
			kv.newConfig.Broadcast()
			Debug(dInfo,"gid %d server %d found new config %v",kv.gid,kv.me,newConfig)
			go kv.ConfigurationChange(newConfig)
			kv.mu.Lock()
			for kv.config.Num == newConfig.Num - 1 && !kv.isSameShardingArray(kv.shards,newShards) {
				kv.configCond.Wait()
				Debug(dInfo,"gid %d server %d shards %v target shards %v target Num %d",kv.gid,kv.me,kv.shards,newShards,newConfig.Num)
			}
			// in case of snapshot
			if kv.config.Num == newConfig.Num - 1 {
				kv.config = newConfig
			}
			keysToDel := make([]string,0)
			for k,_ := range kv.kvMap {
				shouldDelete := true
				for _,shard := range kv.shards {
					if key2shard(k) == shard {
						shouldDelete = false
						break
					}
				}
				if shouldDelete {
					keysToDel = append(keysToDel,k)
				}
			}
			for _,k := range keysToDel {
				delete(kv.kvMap,k)
			}
			if len(keysToDel) != 0 {
				shouldSnapshot = true
			}
			shards := make([]int,0)
			for k,_ := range kv.kvMap {
				shards = append(shards,key2shard(k))
			}
			Debug(dInfo,"gid %d server %d config index update to %d new shards %v actual kv shards %v",kv.gid,kv.me,kv.config.Num,kv.shards,shards)
			kv.mu.Unlock()
		}
		if shouldSnapshot {
			kv.doSnapshot()
		}
		time.Sleep(time.Duration(watchTimeout) * time.Millisecond)
	}
}

func (kv *ShardKV) genSnapshot() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.maxClientCommandID)
	encoder.Encode(kv.kvMap)
	encoder.Encode(kv.config.Num)
	encoder.Encode(kv.config.Shards)
	encoder.Encode(kv.config.Groups)
	encoder.Encode(kv.shards)
	data := buffer.Bytes()

	return data
}

func (kv *ShardKV) handleGetPutAppend(op Op,commandIndex int) {
	kv.mu.Lock()
	maxCommandID := kv.maxClientCommandID[op.ClientID]
	combineID := kv.CombineID(op.ClientID, op.CommandID)
	ch, hasCh := kv.chanMap[combineID]
	var opResult OpResult
	inShards := false
	for _, shard := range kv.shards {
		if shard == key2shard(op.Key) {
			inShards = true
			break
		}
	}
	if !inShards {
		opResult.Err = ErrWrongGroup
	} else {
		if op.OpType == GET {
			value, ok := kv.kvMap[op.Key]
			if ok {
				opResult.Err = OK
				opResult.Value = value
			} else {
				opResult.Err = ErrNoKey
				opResult.Value = ""
			}
		} else {
			opResult.Err = OK
			if op.CommandID >= maxCommandID {
				value, ok := kv.kvMap[op.Key]
				if !ok {
					kv.kvMap[op.Key] = op.Value
				} else {
					if op.OpType == APPEND {
						Debug(dInfo,"gid %d server %d append %s to %s",kv.gid,kv.me,op.Value,op.Key)
						kv.kvMap[op.Key] = value + op.Value
					} else {
						kv.kvMap[op.Key] = op.Value
					}
				}
				kv.maxClientCommandID[op.ClientID] = op.CommandID + 1
			}
		}
	}
	kv.lastMsgIndex = commandIndex
	kv.mu.Unlock()
	if hasCh {
		select {
		case ch <- opResult:
			{

			}
		default:
			{

			}
		}
	}
}

func (kv *ShardKV) doSnapshot() {
	kv.mu.Lock()
	if kv.rf != nil && kv.maxraftstate != -1 {
		lastIndex := kv.lastMsgIndex
		data := kv.genSnapshot()
		Debug(dInfo, "gid %d server %d snapshot with Num %d size %d index %d", kv.gid, kv.me, kv.config.Num, len(data), lastIndex)
		kv.rf.Snapshot(lastIndex, data)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) Applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}
		if msg.CommandValid {
			// ignore no-op
			if msg.Command == nil {
				continue
			}
			op, assertOk := msg.Command.(Op)
			if !assertOk {
				panic("invalid command")
			}

			if op.OpType == ConfigurationChange {
				kv.handleConfigurationChange(op,msg.CommandIndex)
			} else if op.OpType == Migration {
				kv.handleMigration(op,msg.CommandIndex)
			} else {
				kv.handleGetPutAppend(op,msg.CommandIndex)
			}
		} else if msg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.switchToSnapshot(msg.Snapshot)
			}
		}
		if  kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.doSnapshot()
		}
	}
}

func (kv *ShardKV) getShardsDestination(oldShards[]int, newShards []int, c shardctrler.Config) map[int][]*labrpc.ClientEnd {
	shardsDestination := make(map[int][]*labrpc.ClientEnd)
	for _, shard := range oldShards {
		moved := true
		for _, newShard := range newShards {
			if newShard == shard {
				moved = false
				break
			}
		}
		if moved {
			if c.Shards[shard] != 0 {
				ends := make([]*labrpc.ClientEnd, 0)
				for _, destination := range c.Groups[c.Shards[shard]] {
					ends = append(ends,kv.make_end(destination))
				}
				shardsDestination[shard] = ends
			} else {
				ends := make([]*labrpc.ClientEnd,0)
				shardsDestination[shard] = ends
			}
		}
	}
	return shardsDestination
}


func (kv *ShardKV) getShardsFromConfig(config shardctrler.Config) []int{
	newShards := make([]int, 0)
	for index, gid := range config.Shards {
		if gid == kv.gid {
			newShards = append(newShards, index)
		}
	}
	return newShards
}

func (kv *ShardKV) appendZeroOriginShards(newShards []int) {
	for shard, gid := range kv.config.Shards {
		if gid == 0 {
			inNewShards := false
			for _, newShard := range newShards {
				if newShard == shard {
					inNewShards = true
					break
				}
			}
			alreadyInShards := false
			for _, curShard := range kv.shards {
				if curShard == shard {
					alreadyInShards = true
					break
				}
			}
			if inNewShards && !alreadyInShards{
				kv.shards = append(kv.shards, shard)
			}
		}
	}
}

func (kv *ShardKV) handleConfigurationChange(op Op,commandIndex int) {
	kv.mu.Lock()
	Debug(dInfo,"gid %d server %d handle configuration change, new config %d cur config %d",kv.gid,kv.me, op.NewConfig.Num,kv.config.Num)
	for op.CommandID > kv.config.Num + 1 {
		kv.newConfig.Wait()
	}
	opResult := OpResult{}
	combineID := kv.CombineID(op.ClientID, op.CommandID)
	ch, hasCh := kv.chanMap[combineID]
	if op.CommandID == kv.config.Num + 1 {
		newShards := kv.getShardsFromConfig(op.NewConfig)
		kv.appendZeroOriginShards(newShards)
		kv.migrateShards(kv.shards,newShards,op.NewConfig)
		kv.configCond.Broadcast()
	}
	kv.lastMsgIndex = commandIndex
	kv.mu.Unlock()
	if hasCh {
		select {
		case ch <- opResult:
			{

			}
		default:
			{

			}
		}
	}
}

func (kv *ShardKV) switchToSnapshot(snapshot []byte) {
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	kv.mu.Lock()
	var Num int
	var shards [shardctrler.NShards]int
	var groups map[int][]string
	if decoder.Decode(&kv.maxClientCommandID) != nil || decoder.Decode(&kv.kvMap) != nil ||
		decoder.Decode(&Num) != nil || decoder.Decode(&shards) != nil ||
		decoder.Decode(&groups) != nil || decoder.Decode(&kv.shards) != nil {
		panic("decode failed in server")
	}
	kv.config.Num = Num
	kv.config.Shards = shards
	kv.config.Groups = groups
	Debug(dInfo,"gid %d server %d switch to snapshot with Num %d shards %d",kv.gid,kv.me,Num,kv.shards)
	kv.configCond.Broadcast()
	kv.mu.Unlock()
}

func (kv *ShardKV) assertShardsIsSubset(shards1 []int, shards2 []int) {
	for _,shard1 := range shards1 {
		contains := false
		for _,shard2 := range shards2 {
			if shard1 == shard2 {
				contains = true
				break
			}
		}
		if !contains {
			errorString := fmt.Sprintf("gid %d server %d shard1 %v not subset of shard2 %v", kv.gid,kv.me,
				shards1,shards2)
			panic(errorString)
		}
	}
}

func (kv *ShardKV) sendShardTo(shard int, shardMap map[string]string,configNum int,destination []*labrpc.ClientEnd,maxClientCommandID map[int]int) {
	args := MigrationArgs{
		ShardMap:  shardMap,
		ConfigNum: configNum,
		Shard:	   shard,
		MaxClientCommandID: maxClientCommandID,
	}
	for {
		success := false
		for _,dest := range destination {
			var reply MigrationReply
			Debug(dInfo,"gid %d server %d send shard %d",kv.gid,kv.me,shard)
			ok := dest.Call("ShardKV.Migration",&args,&reply)
			if ok && reply.Err == OK {
				success = true
				break
			}
		}
		if success {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	Debug(dInfo,"gid %d server %d send shard %d oookkk",kv.gid,kv.me,shard)
}

func (kv *ShardKV) handleMigration(op Op,commandIndex int) {
	kv.mu.Lock()
	Debug(dInfo,"gid %d server %d handle migration, new config %d, shard %d",kv.gid,kv.me,op.CommandID, op.Shard)
	combineID := kv.CombineID(internalMigrationID, op.CommandID)
	ch, hasCh := kv.chanMap[combineID]
	opResult := OpResult{
		Err:   OK,
		Value: "",
	}
	for op.CommandID > kv.config.Num + 1 {
		kv.newConfig.Wait()
	}
	if op.CommandID == kv.config.Num + 1 {
		alreadyHasShard := false
		for _, shard := range kv.shards {
			if shard == op.Shard {
				alreadyHasShard = true
				break
			}
		}
		if !alreadyHasShard {
			for k, v := range op.MigrationShardMap {
				kv.kvMap[k] = v
			}
			kv.shards = append(kv.shards, op.Shard)
		}
		for client,id := range op.MaxClientCommandID {
			if client != internalMigrationID && client != internalConfigurationChangeID {
				if id > kv.maxClientCommandID[client] {
					kv.maxClientCommandID[client] = id
				}
			}
		}
		kv.configCond.Broadcast()
	}
	kv.lastMsgIndex = commandIndex
	kv.mu.Unlock()
	if hasCh {
		select {
		case ch <- opResult:
			{

			}
		default:
			{

			}
		}
	}
}

func (kv *ShardKV) removeMovedShards(newShards []int) {
	shardsToSwitch := make([]int,0)
	for _, shard := range kv.shards {
		for _,newShard := range newShards {
			if shard == newShard {
				shardsToSwitch = append(shardsToSwitch,shard)
			}
		}
	}
	kv.shards = shardsToSwitch
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.dead = 0

	kv.maxClientCommandID = make(map[int]int)
	kv.chanMap = make(map[int64]chan OpResult)
	kv.persister = persister
	kv.kvMap = make(map[string]string)
	kv.config = shardctrler.Config{}
	kv.configCond = sync.NewCond(&kv.mu)
	kv.newConfig = sync.NewCond(&kv.mu)
	kv.maxClientCommandID[internalMigrationID] = 1
	kv.maxClientCommandID[internalConfigurationChangeID] = 1
	kv.lastMsgIndex = 0
	go kv.Applier()
	go kv.configurationWatcher()
	return kv
}
