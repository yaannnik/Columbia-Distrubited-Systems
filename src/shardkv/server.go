package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Get   = "Get"
	Put   = "Put"
	Join  = "Join"
	Leave = "Leave"
	Sync  = "Sync"
)

type Op struct {
	// Your definitions here.
	Type     string
	Key      string
	Val      string
	Hash     bool
	Sequence string
	Db       map[string]Value
	Log      map[string]KVPair
	//SeqNum  int
}

type KVPair struct {
	Key string
	Val string
}

type Value struct {
	Val string
	Num int
}

type Buffer struct {
	sequences map[string]string // key -> sequence
	data      map[string]KVPair // sequence -> kv
	queue     []string
	front     int
	back      int
	size      int
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	gid        int64 // my replica group ID
	// Your definitions here.
	cfg   *shardmaster.Config
	cfgMu sync.Mutex
	db    map[string]Value
	seq   int
	buf   Buffer
}

func (kv *ShardKV) WaitConsensus(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) RunOp(op Op) {
	var decision Op

	_, exists := kv.buf.data[op.Sequence]
	if exists {
		return
	}

	for {
		kv.seq++
		decided, val := kv.px.Status(kv.seq)
		if decided {
			decision = val.(Op)
		} else {
			kv.px.Start(kv.seq, op)
			decision = kv.WaitConsensus(kv.seq)
		}

		switch decision.Type {
		//case Get, Sync:
		//	break
		case Put:
			kv.RunPut(decision)
		case Join:
			kv.RunJoin(decision)
		case Leave:
			kv.RunLeave(decision)
		default:
		}

		if decision.Sequence == op.Sequence {
			kv.px.Done(kv.seq)
			break
		}
	}
}

func (kv *ShardKV) RunPut(op Op) {
	oldv, _ := kv.db[op.Key]
	if op.Hash {
		newval := strconv.Itoa(int(hash(oldv.Val + op.Val)))
		kv.db[op.Key] = Value{newval, oldv.Num + 1}

		kv.buf.UpdateQueue(op.Sequence, op.Key, oldv.Val)
	} else {
		kv.db[op.Key] = Value{op.Val, oldv.Num + 1}
	}
}

func (kv *ShardKV) RunJoin(op Op) {
	for key, val := range op.Db {
		if val.Num > kv.db[key].Num {
			kv.db[key] = val
		}
	}

	for sequence, v := range op.Log {
		kv.buf.UpdateQueue(sequence, v.Key, v.Val)
	}
}

func (kv *ShardKV) RunLeave(op Op) {
	db := make(map[string]Value)
	for key, val := range kv.db {
		db[key] = val
	}
	for key := range op.Db {
		delete(db, key)
	}
	kv.db = db
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	sync_ := Op{
		Type:     Sync,
		Sequence: strconv.Itoa(int(time.Now().UnixNano())) + "." + strconv.Itoa(kv.me),
	}
	kv.RunOp(sync_)

	op := Op{
		Type: Leave,
		Db:   make(map[string]Value),
	}
	reply.Db = make(map[string]Value)
	reply.Log = make(map[string]KVPair)
	for k, v := range kv.db {
		shard := key2shard(k)
		if args.Shard == shard {
			reply.Db[k] = v
			op.Db[k] = v
			sequence := kv.buf.sequences[k]
			kvpair := kv.buf.data[sequence]
			reply.Log[sequence] = kvpair
		}
	}
	kv.cfg.Shards[args.Shard] = args.Gid

	return nil
}

func (buf *Buffer) UpdateQueue(sequence string, key string, val string) {
	for len(buf.data) >= buf.size {
		front := buf.queue[buf.front]
		kv := buf.data[front]
		delete(buf.data, front)
		delete(buf.sequences, kv.Key)
		buf.front = (buf.front + 1) % buf.size
	}

	buf.queue[buf.back] = sequence
	buf.data[sequence] = KVPair{
		Key: key,
		Val: val,
	}
	buf.sequences[key] = sequence
	buf.back = (buf.back + 1) % buf.size
}

func (kv *ShardKV) Reconfig(cfg *shardmaster.Config) {
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.cfg.Shards[shard] != kv.gid && cfg.Shards[shard] == kv.gid {
			args := &GetShardArgs{
				Num:   cfg.Num,
				Shard: shard,
				Gid:   kv.gid,
			}
			reply := GetShardReply{}

			gid := kv.cfg.Shards[shard]
			peers := kv.cfg.Groups[gid]

			for _, peer := range peers {
				if call(peer, "ShardKV.GetShard", args, &reply) {
					break
				}
			}

			if len(reply.Db) > 0 {
				kv.mu.Lock()
				op := Op{
					Type:     Join,
					Sequence: strconv.Itoa(int(time.Now().UnixNano())) + "." + strconv.Itoa(kv.me),
					Db:       reply.Db,
					Log:      reply.Log,
				}
				kv.RunOp(op)
				kv.mu.Unlock()
			}
			kv.cfg.Shards[shard] = kv.gid
		}
	}
	kv.cfg = cfg
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(args.Key)
	if kv.cfg.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	reply.Err = OK
	reply.Value = ""
	op := Op{
		Type:     Get,
		Key:      args.Key,
		Sequence: args.Sequence,
	}
	kv.RunOp(op)
	reply.Value = kv.db[args.Key].Val
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(args.Key)
	if kv.cfg.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	reply.Err = OK
	reply.PreviousValue = ""
	op := Op{
		Type:     Put,
		Key:      args.Key,
		Val:      args.Value,
		Sequence: args.Sequence,
		Hash:     args.DoHash,
	}
	kv.RunOp(op)

	val, exists := kv.buf.data[args.Sequence]
	if exists {
		reply.PreviousValue = val.Val
	}

	return nil
}

func (kv *ShardKV) tick() {
	kv.cfgMu.Lock()
	defer kv.cfgMu.Unlock()
	latestCfg := kv.sm.Query(-1)
	//if config.Num > kv.cfg.Num {
	//	kv.Reconfig(&config)
	//}
	for num := kv.cfg.Num + 1; num <= latestCfg.Num; num++ {
		cfg := kv.sm.Query(num)
		if kv.cfg.Num == 0 {
			kv.cfg = &cfg
			continue
		}
		kv.Reconfig(&cfg)
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// StartServer
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.cfg = &shardmaster.Config{Num: 0}
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	// Your initialization code here.
	kv.db = make(map[string]Value)
	kv.buf.data = make(map[string]KVPair)
	kv.buf.sequences = make(map[string]string)
	kv.buf.size = 1024
	kv.buf.queue = make([]string, kv.buf.size)
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()
	return kv
}
