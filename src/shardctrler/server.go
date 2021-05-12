package shardctrler

import (
	"6.824/raft"
	"bytes"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"


const Timeout = 500
const SnapshotThreshold = 500

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs            []Config // indexed by config num
	chanMap            map[int64]chan OpResult
	maxClientCommandID map[int]int
	dead               int32 // set by Kill()
	persister          *raft.Persister

	maxraftstate int // snapshot if log grows this big
}

type OpResult struct {
	Error  Err
	Config Config
}

func (sc *ShardCtrler) CombineID(clientID int, commandID int) int64 {
	var ret int64
	ret += int64(clientID)
	ret <<= 32
	ret += int64(commandID)
	return ret
}

type OpType int

const (
	JOIN  OpType = 0
	LEAVE OpType = 1
	MOVE  OpType = 2
	QUERY OpType = 3
)

type Op struct {
	// Your data here.
	OpType    OpType
	ClientID  int
	CommandID int
	ServerID  int
	// for Join
	JoinServers map[int][]string
	// for Leave
	GIDs []int
	// for Move
	Shard int
	GID   int
	// for Query
	Num int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpType:      JOIN,
		ClientID:    args.ClientID,
		CommandID:   args.CommandID,
		JoinServers: args.Servers,
		GIDs:        nil,
		Shard:       0,
		GID:         0,
		Num:         0,
	}
	combineID := sc.CombineID(args.ClientID, args.CommandID)
	sc.mu.Lock()
	if args.CommandID < sc.maxClientCommandID[args.ClientID] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	ch, ok := sc.chanMap[combineID]
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		if !ok {
			ch = make(chan OpResult)
			sc.chanMap[combineID] = ch
		}
	}
	sc.mu.Unlock()

	select {
	case opResult := <-ch:
		{
			curTerm, _ := sc.rf.GetState()
			if curTerm == term {
				reply.Err = opResult.Error
			} else {
				reply.WrongLeader = true
			}
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		{
			reply.WrongLeader = true
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType:      LEAVE,
		ClientID:    args.ClientID,
		CommandID:   args.CommandID,
		JoinServers: nil,
		GIDs:        args.GIDs,
		Shard:       0,
		GID:         0,
		Num:         0,
	}
	combineID := sc.CombineID(args.ClientID, args.CommandID)
	sc.mu.Lock()
	if args.CommandID < sc.maxClientCommandID[args.ClientID] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	ch, ok := sc.chanMap[combineID]
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		if !ok {
			ch = make(chan OpResult)
			sc.chanMap[combineID] = ch
		}
	}
	sc.mu.Unlock()

	select {
	case opResult := <-ch:
		{
			curTerm, _ := sc.rf.GetState()
			if curTerm == term {
				reply.Err = opResult.Error
			} else {
				reply.WrongLeader = true
			}
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		{
			reply.WrongLeader = true
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpType:      MOVE,
		ClientID:    args.ClientID,
		CommandID:   args.CommandID,
		JoinServers: nil,
		GIDs:        nil,
		Shard:       args.Shard,
		GID:         args.GID,
		Num:         0,
	}
	combineID := sc.CombineID(args.ClientID, args.CommandID)
	sc.mu.Lock()
	if args.CommandID < sc.maxClientCommandID[args.ClientID] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	ch, ok := sc.chanMap[combineID]
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		if !ok {
			ch = make(chan OpResult)
			sc.chanMap[combineID] = ch
		}
	}
	sc.mu.Unlock()

	select {
	case opResult := <-ch:
		{
			curTerm, _ := sc.rf.GetState()
			if curTerm == term {
				reply.Err = opResult.Error
			} else {
				reply.WrongLeader = true
			}
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		{
			reply.WrongLeader = true
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpType:      QUERY,
		ClientID:    args.ClientID,
		CommandID:   args.CommandID,
		JoinServers: nil,
		GIDs:        nil,
		Shard:       0,
		GID:         0,
		Num:         args.Num,
	}
	combineID := sc.CombineID(args.ClientID, args.CommandID)
	sc.mu.Lock()
	ch, ok := sc.chanMap[combineID]
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		if !ok {
			ch = make(chan OpResult)
			sc.chanMap[combineID] = ch
		}
	}
	sc.mu.Unlock()

	select {
	case opResult := <-ch:
		{
			curTerm, _ := sc.rf.GetState()
			if curTerm == term {
				reply.Err = opResult.Error
				reply.Config = opResult.Config
			} else {
				reply.WrongLeader = true
			}
		}
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		{
			reply.WrongLeader = true
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func min(a int,b int) int{
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int,b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (sc *ShardCtrler) Applier() {
	for msg := range sc.applyCh {
		if sc.killed() {
			return
		}
		// check whether to generate a snapshot
		if sc.rf != nil && sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate {
			sc.mu.Lock()
			lastIndex := msg.CommandIndex - 1
			data := sc.genSnapshot()
			sc.rf.Snapshot(lastIndex, data)
			sc.mu.Unlock()
		}
		if msg.CommandValid {
			if msg.Command == nil {
				continue
			}
			op, assertOk := msg.Command.(Op)
			if !assertOk {
				panic("invalid command")
			}
			sc.mu.Lock()
			opResult := OpResult{
				Error:  OK,
				Config: Config{},
			}
			ch,hasCh := sc.chanMap[sc.CombineID(op.ClientID,op.CommandID)]
			switch op.OpType {
			case JOIN:
				{
					if op.CommandID >= sc.maxClientCommandID[op.ClientID]{
						newConfig := sc.copyFromLastConfig()
						lastGIDs := make([]int,0)
						for k,_ := range newConfig.Groups {
							lastGIDs = append(lastGIDs,k)
						}
						// add new groups
						for k, v := range op.JoinServers {
							newConfig.Groups[k] = v
						}
						if len(lastGIDs) < NShards {
							sc.joinRedistribute(&newConfig)
						}
						sc.configs = append(sc.configs, newConfig)
						sc.maxClientCommandID[op.ClientID] = op.CommandID + 1
						Debug(dInfo,"server %d shards after join is %v",
							sc.me,newConfig.Shards)
					}
				}
			case LEAVE:
				{
					if op.CommandID >= sc.maxClientCommandID[op.ClientID] {
						newConfig := sc.copyFromLastConfig()
						// delete all the leave groups
						shouldRedistribute := false
						for _,leaveGid := range op.GIDs {
							delete(newConfig.Groups, leaveGid)
							for _,gid := range newConfig.Shards {
								if gid == leaveGid {
									shouldRedistribute = true
									break
								}
							}
						}
						if shouldRedistribute {
							sc.leaveRedistribute(&newConfig)
						}
						sc.configs = append(sc.configs, newConfig)
						sc.maxClientCommandID[op.ClientID] = op.CommandID + 1
						Debug(dInfo,"server %d shards after leave is %v",
							sc.me,newConfig.Shards)
					}
				}
			case MOVE:
				{
					if op.CommandID >= sc.maxClientCommandID[op.ClientID] {
						newConfig := sc.copyFromLastConfig()
						newConfig.Shards[op.Shard] = op.GID
						sc.configs = append(sc.configs, newConfig)
						sc.maxClientCommandID[op.ClientID] = op.CommandID + 1
					}
				}
			case QUERY:
				{
					if op.Num < 0 || op.Num >= len(sc.configs) {
						opResult.Config = sc.configs[len(sc.configs) - 1]
					} else {
						opResult.Config = sc.configs[op.Num]
					}
					if op.CommandID >= sc.maxClientCommandID[op.ClientID] {
						sc.maxClientCommandID[op.ClientID] = op.CommandID + 1
					}
				}
			default:
				panic("invalid op type")
			}
			sc.mu.Unlock()
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
		} else if msg.SnapshotValid {
			if sc.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot) {
				buffer := bytes.NewBuffer(msg.Snapshot)
				decoder := labgob.NewDecoder(buffer)
				sc.mu.Lock()
				if decoder.Decode(&sc.maxClientCommandID) != nil  || decoder.Decode(&sc.configs) != nil {
					panic("decode failed in server")
				}
				sc.mu.Unlock()
			}
		} else {

		}
	}
}

func (sc *ShardCtrler) genSnapshot() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(sc.maxClientCommandID)
	encoder.Encode(sc.configs)
	data := buffer.Bytes()
	return data
}

func (sc *ShardCtrler) copyFromLastConfig() Config {
	lastConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = lastConfig.Shards[i]
	}
	for k,v := range lastConfig.Groups{
		newConfig.Groups[k] = v
	}
	return newConfig
}

func (sc *ShardCtrler) joinRedistribute(c *Config) {
	gids := make([]int,0)
	for gid,_ := range c.Groups {
		gids = append(gids,gid)
	}
	sort.Ints(gids)
	groups := len(gids)
	avg := NShards/groups
	remainder := NShards % groups
	targetDistribution := make(map[int]int)
	for _,gid := range gids {
		targetDistribution[gid] = avg
	}
	index := 0
	for i := 0; i < remainder; i++ {
		targetDistribution[gids[index]]++
		index = (index + 1)%len(gids)
	}
	curDistribution := make(map[int]int)
	for _,gid := range gids {
		curDistribution[gid] = 0
	}
	for _,gid := range c.Shards {
		curDistribution[gid]++
	}
	for idx,gid1 := range c.Shards {
		if curDistribution[gid1] > targetDistribution[gid1] {
			ok := false
			for _,gid2 := range gids {
				if gid1 != gid2 && curDistribution[gid2] < targetDistribution[gid2] {
					c.Shards[idx] = gid2
					curDistribution[gid1]--
					curDistribution[gid2]++
					ok = true
					break
				}
			}
			if !ok {
				panic("joinRedistribution failed")
			}
		}
	}
	sc.assertTargetDistribution(curDistribution,targetDistribution)
}

func (sc *ShardCtrler) leaveRedistribute(c *Config) {
	gids := make([]int,0)
	for gid,_ := range c.Groups {
		gids = append(gids,gid)
	}
	sort.Ints(gids)
	groups := len(gids)
	if groups == 0 {
		// no group
		// all shards set to zero
		for i := 0; i < NShards; i++ {
			c.Shards[i] = 0
		}
		return
	}
	avg := NShards/groups
	remainder := NShards % groups
	curCarry := make(map[int]int)
	for _,gid := range gids {
		curCarry[gid] = 0
	}
	for _,gid := range c.Shards {
		deletedGroup := true
		for _,gid2 := range gids {
			if gid == gid2 {
				deletedGroup = false
				break
			}
		}
		if !deletedGroup {
			curCarry[gid]++
		}
	}
	targetDistribution := make(map[int]int)
	for _,gid := range gids {
		targetDistribution[gid] = avg
		if curCarry[gid] > avg {
			targetDistribution[gid] += min(remainder,curCarry[gid] - avg)
			remainder = max(0,remainder - (curCarry[gid] - avg))
		}
	}
	for i := 0; i < remainder; i++ {
		minDisGID := -1
		minDis := 0x3f3f3f3f
		for _,gid := range gids {
			if targetDistribution[gid] < minDis {
				minDis = targetDistribution[gid]
				minDisGID = gid
			}
		}
		targetDistribution[minDisGID]++
	}
	curDistribution := make(map[int]int)
	for _,gid := range gids {
		curDistribution[gid] = 0
	}
	for _,gid := range c.Shards {
		deletedGroup := true
		for _,gid2 := range gids {
			if gid == gid2 {
				deletedGroup = false
				break
			}
		}
		if !deletedGroup {
			curDistribution[gid]++
		}
	}
	for idx,gid1 := range c.Shards {
		deletedGroup := true
		for _,gid2 := range gids {
			if gid1 == gid2 {
				deletedGroup = false
				break
			}
		}
		if deletedGroup {
			ok := false
			for _,gid2 := range gids {
				if gid1 != gid2 && curDistribution[gid2] < targetDistribution[gid2] {
					c.Shards[idx] = gid2
					curDistribution[gid2]++
					ok = true
					break
				}
			}
			if !ok {
				panic("joinRedistribution failed")
			}
		}
	}
	sc.assertTargetDistribution(curDistribution,targetDistribution)
}

func (sc *ShardCtrler) assertTargetDistribution(distribution1 map[int]int, distribution2 map[int]int) {
	for k,v := range distribution1 {
		if distribution2[k] != v {
			errorString := fmt.Sprintf("redistribution error,cur %v,target %v",distribution1,distribution2)
			panic(errorString)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)

	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.maxClientCommandID = make(map[int]int)
	sc.chanMap = make(map[int64]chan OpResult)
	sc.dead = 0
	sc.persister = persister
	sc.maxraftstate = SnapshotThreshold
	go sc.Applier()

	return sc
}
