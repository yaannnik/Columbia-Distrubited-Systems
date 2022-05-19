package kvpaxos

import (
	"net"
	"strconv"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

const (
	Get     = "Get"
	Put     = "Put"
	PutHash = "PutHash"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	Xid      int64
	Sequence int64
}

type Response struct {
	sequence int64
	err      Err
	value    string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db        map[string]string
	responses map[int64]*Response
	seq       int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Type:     Get,
		Key:      args.Key,
		Xid:      args.Xid,
		Sequence: args.Sequence,
	}
	err, value := kv.WriteOperation(op)

	reply.Value = value
	reply.Err = err

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Xid:      args.Xid,
		Type:     Put,
		Key:      args.Key,
		Value:    args.Value,
		Sequence: args.Sequence,
	}
	if args.DoHash {
		op.Type = PutHash
	}

	err, value := kv.WriteOperation(op)

	reply.Err = err
	if args.DoHash {
		reply.PreviousValue = value
	}

	return nil
}

func (kv *KVPaxos) WriteOperation(op Op) (Err, string) {
	// Your code here.
	response, exists := kv.responses[op.Xid]
	if exists {
		if op.Sequence <= response.sequence {
			return response.err, response.value
		}
		//if op.Sequence < response.sequence {
		//	return ErrWrongSequence, ""
		//}
	}

	for {
		kv.seq++
		//seq := kv.seq
		kv.px.Start(kv.seq, op)
		decision := kv.WaitConsensus(kv.seq)
		err, value := kv.RunOperation(decision)
		kv.responses[decision.Xid] = &Response{
			sequence: decision.Sequence,
			err:      err,
			value:    value,
		}

		if decision.Xid == op.Xid && decision.Sequence == op.Sequence {
			kv.px.Done(kv.seq)
			return err, value
		}
	}
}

func (kv *KVPaxos) WaitConsensus(seq int) Op {
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

func (kv *KVPaxos) RunOperation(op Op) (Err, string) {
	switch op.Type {
	case Get:
		value, exists := kv.db[op.Key]
		if exists {
			return OK, value
		} else {
			return ErrNoKey, ""
		}
	case Put:
		kv.db[op.Key] = op.Value
		return OK, ""
	case PutHash:
		value, _ := kv.db[op.Key]
		h := hash(value + op.Value)
		kv.db[op.Key] = strconv.Itoa(int(h))
		return OK, value
	default:
		return ErrUnknown, ""
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.responses = make(map[int64]*Response)

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
