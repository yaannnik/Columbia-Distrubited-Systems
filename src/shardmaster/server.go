package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	seq     int
	cfg     int
}

type Op struct {
	// Your data here.
	Type     string
	GID      int64
	Servers  []string
	Shard    int
	Sequence string
}

func (sm *ShardMaster) GetProposalID() string {
	timestamp := time.Now().UnixNano()
	return strconv.FormatInt(timestamp, 10) + "." + strconv.Itoa(sm.me)
}

func (sm *ShardMaster) WaitConsensus(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := sm.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) MakeNewConfig() *Config {
	oldCfg := sm.configs[sm.cfg]
	newCfg := Config{
		Num:    oldCfg.Num + 1,
		Shards: [NShards]int64{},
		Groups: map[int64][]string{},
	}

	for shardId, gid := range oldCfg.Shards {
		newCfg.Shards[shardId] = gid
	}

	for gid, servers := range oldCfg.Groups {
		newCfg.Groups[gid] = servers
	}

	return &newCfg
}

func (sm *ShardMaster) RunJoin(op Op) {
	cfg := sm.MakeNewConfig()
	_, exist := cfg.Groups[op.GID]
	if !exist {
		cfg.Groups[op.GID] = op.Servers
		sm.configs = append(sm.configs, *cfg)
		sm.cfg++
		sm.ReAllocate()
	}
}

func (sm *ShardMaster) RunLeave(op Op) {
	cfg := sm.MakeNewConfig()
	for shard, gid := range cfg.Shards {
		if gid == op.GID {
			cfg.Shards[shard] = 0
		}
	}
	delete(cfg.Groups, op.GID)
	sm.configs = append(sm.configs, *cfg)
	sm.cfg++
	sm.ReAllocate()
}

func (sm *ShardMaster) RunMove(op Op) {
	cfg := sm.MakeNewConfig()
	cfg.Shards[op.Shard] = op.GID
	sm.configs = append(sm.configs, *cfg)
	sm.cfg++
}

func (sm *ShardMaster) RunOp(op Op) {
	var decision Op
	for {
		sm.seq++
		decided, val := sm.px.Status(sm.seq)
		if decided {
			decision = val.(Op)
		} else {
			sm.px.Start(sm.seq, op)
			decision = sm.WaitConsensus(sm.seq)
		}
		switch decision.Type {
		case Join:
			sm.RunJoin(decision)
		case Leave:
			sm.RunLeave(decision)
		case Move:
			sm.RunMove(decision)
		default:
		}

		if decision.Sequence == op.Sequence {
			sm.px.Done(sm.seq)
			break
		}
	}
}

func (sm *ShardMaster) ReAllocate() {
	cfg := &sm.configs[len(sm.configs)-1]
	mp := map[int64]int{}
	average := NShards / len(cfg.Groups)
	module := NShards % len(cfg.Groups)
	var wild []int

	for shard, gid := range cfg.Shards {
		if gid <= 0 {
			wild = append(wild, shard)
		} else {
			_, exists := mp[gid]
			if !exists {
				mp[gid] = 1
			} else {
				mp[gid]++
			}
		}
	}

	for gid := range cfg.Groups {
		_, exists := mp[gid]
		if !exists {
			mp[gid] = 0
		}
	}

	if len(wild) == 0 {
		redundant := map[int64]int{}
		vacant := int64(0)
		for gid, count := range mp {
			if count > average {
				if module > 0 {
					// assign one module to this gid
					module -= 1
					count -= 1
					if count == average {
						continue
					}
				}
				redundant[gid] = count - average
			} else if count == 0 {
				vacant = gid
			}
		}

		for shard, gid := range cfg.Shards {
			count, exist := redundant[gid]
			if exist {
				if count > 0 {
					cfg.Shards[shard] = vacant
					redundant[gid]--
				}
			}
		}
	} else {
		p := 0
		for gid, count := range mp {
			if count <= average {
				if module > 0 {
					// assign one module to this gid
					module -= 1
					count -= 1
					if count == average {
						continue
					}
				}

				for average > count && p < len(wild) {
					cfg.Shards[wild[p]] = gid
					p++
					count++
				}
			}
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type:     Join,
		GID:      args.GID,
		Servers:  args.Servers,
		Shard:    0,
		Sequence: sm.GetProposalID(),
	}
	sm.RunOp(op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type:     Leave,
		GID:      args.GID,
		Servers:  nil,
		Shard:    0,
		Sequence: sm.GetProposalID(),
	}
	sm.RunOp(op)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		Type:     Move,
		GID:      args.GID,
		Servers:  nil,
		Shard:    args.Shard,
		Sequence: sm.GetProposalID(),
	}
	sm.RunOp(op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		Type:     Query,
		GID:      0,
		Servers:  nil,
		Shard:    0,
		Sequence: sm.GetProposalID(),
	}
	sm.RunOp(op)

	if args.Num >= 0 && args.Num <= sm.cfg {
		reply.Config = sm.configs[args.Num]
	} else {
		reply.Config = sm.configs[sm.cfg]
	}

	return nil
}

// Kill please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.cfg = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
