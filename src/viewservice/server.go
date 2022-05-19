package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.

	view *View

	PrimaryPing uint // ACK signal from Primary
	BackupPing  uint // ACK signal from Backup

	PrimaryTick uint
	BackupTick  uint
	SelfTick    uint
}

// Ping
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.view.Primary == "" && vs.view.Viewnum == 0 {
		// fmt.Println("First Ping, first Primary")
		vs.InitPrimary(args.Me, args.Viewnum)
		vs.PrimaryTick = vs.SelfTick
	} else if args.Me == vs.view.Primary {
		// fmt.Println("Ping from Primary")
		if args.Viewnum == 0 {
			// fmt.Println("Primary restart, treated as dead")
			vs.UseBackUp()
		} else {
			// fmt.Println("Ack Viewnum and update Tick")
			vs.PrimaryPing = args.Viewnum
		}
		vs.PrimaryTick = vs.SelfTick
	} else if vs.view.Backup == "" && vs.ACK() {
		// fmt.Println("Primary ACKed, first Backup")
		vs.InitBackup(args.Me)
		vs.BackupTick = vs.SelfTick
	} else if args.Me == vs.view.Backup {
		// fmt.Println("Ping from Backup")
		if args.Viewnum == 0 && vs.ACK() {
			// Backup restart
			vs.view.Backup = args.Me
		} else if args.Viewnum != 0 {
			// Ping backup
			vs.BackupPing = args.Viewnum
		}
		vs.BackupTick = vs.SelfTick
	}

	reply.View = *vs.view

	return nil
}

func (vs *ViewServer) InitPrimary(server string, viewnum uint) {
	vs.view.Primary = server
	vs.view.Viewnum = viewnum + 1
}

func (vs *ViewServer) InitBackup(server string) {
	vs.view.Backup = server
	vs.view.Viewnum++
}

func (vs *ViewServer) UseBackUp() {
	if vs.view.Backup == "" {
		// fmt.Println("No Backup, wait for next Ping")
		vs.view.Primary = ""
		return
	}
	// fmt.Println("Use backup for Primary")
	vs.view.Primary = vs.view.Backup
	vs.view.Backup = ""

	vs.PrimaryPing = 0
	vs.view.Viewnum++
}

func (vs *ViewServer) UseIdle() {
	vs.view.Backup = ""
	vs.view.Viewnum++
}

func (vs *ViewServer) ACK() bool {
	return vs.view.Viewnum == vs.PrimaryPing
}

// Get
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = *vs.view

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.SelfTick++
	if vs.SelfTick-vs.PrimaryTick >= DeadPings && vs.ACK() {
		// Backup takes over
		vs.UseBackUp()
		vs.PrimaryTick = vs.SelfTick
	}
	if vs.view.Backup != "" && vs.SelfTick-vs.BackupTick >= DeadPings && vs.ACK() {
		// fmt.Println("Backup timeout")
		vs.UseIdle()
		vs.BackupTick = vs.SelfTick
	}

}

// Kill
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	vs.view = &View{Viewnum: 0, Primary: "", Backup: ""}
	vs.PrimaryPing, vs.PrimaryTick = 0, 0
	vs.BackupPing, vs.BackupTick = 0, 0
	//vs.volunteer = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
