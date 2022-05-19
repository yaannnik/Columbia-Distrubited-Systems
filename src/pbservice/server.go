package pbservice

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

// Debug Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.

	mu   sync.Mutex
	view viewservice.View
	db   map[string]string // kv pairs
	st   map[string]string // status
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()

	// wrong server
	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return errors.New(ErrWrongServer)
	}

	//at most once
	if pb.st["id."+args.Me] == args.Xid {
		reply.PreviousValue = pb.st["val."+args.Me]
		reply.Err = OK
		return nil
	}

	value := args.Value
	if args.DoHash {
		reply.PreviousValue = pb.db[args.Key]
		value = strconv.Itoa(int(hash(reply.PreviousValue + value)))
	}

	fargs := &ForwardArgs{
		Db: map[string]string{
			args.Key: value,
		},
		St: map[string]string{
			"id." + args.Me:  args.Xid,
			"val." + args.Me: pb.db[args.Key],
		},
	}

	err := pb.Bak(fargs)
	if err == false {
		reply.Err = ErrWrongServer
		return errors.New(ErrWrongServer)
	}

	pb.WriteDB(fargs.Db, fargs.St)
	reply.Err = OK
	return nil
}

func (pb *PBServer) Bak(args *ForwardArgs) bool {
	if pb.view.Backup == "" {
		return true
	}

	var reply ForwardReply
	ok := call(pb.view.Backup, "PBServer.SendBak", args, &reply)
	if !ok {
		return false
	}
	return true
}

func (pb *PBServer) SendBak(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return errors.New(ErrWrongServer)
	}

	pb.WriteDB(args.Db, args.St)

	reply.Err = OK
	return nil
}

func (pb *PBServer) WriteDB(db map[string]string, st map[string]string) {
	for key, val := range db {
		pb.db[key] = val
	}
	for key, val := range st {
		pb.st[key] = val
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return errors.New(ErrWrongServer)
	}

	val, ok := pb.db[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}

	reply.Value = val
	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		// fmt.Println("Ping error: ", pb.me, " ", view)
	}

	consist := view.Backup != "" && view.Backup != pb.view.Backup && pb.view.Primary == pb.me
	pb.view = view

	if consist {
		fargs := &ForwardArgs{
			Db: pb.db,
			St: pb.st,
		}
		pb.Bak(fargs)
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.db = map[string]string{}
	pb.st = map[string]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
