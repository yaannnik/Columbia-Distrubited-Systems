package pbservice

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"strconv"
	"time"
	"viewservice"
)

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here

	view viewservice.View
	me   string
}

func MakeXid() int64 {
	max := big.NewInt(int64(1) << 62)
	ri, _ := rand.Int(rand.Reader, max)
	xid := ri.Int64()
	return xid
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here

	ck.view = viewservice.View{}
	ck.me = strconv.FormatInt(MakeXid(), 10)

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Get
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.

	if ck.view.Viewnum == 0 {
		ck.QueryView()
	}

	xid := strconv.FormatInt(MakeXid(), 10)
	args := &GetArgs{
		Key: key,
		Xid: xid,
	}
	var reply GetReply

	for true {
		// keep trying until it succeeds.
		time.Sleep(viewservice.PingInterval)
		ok := call(ck.view.Primary, "PBServer.Get", args, &reply)
		if ok {
			//fmt.Println("Get: ", args.Key, " ", reply.Value)
			return reply.Value
		}
		ck.QueryView()
	}

	return "???"
}

// PutExt
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {

	// Your code here.

	if ck.view.Viewnum == 0 {
		ck.QueryView()
	}

	xid := strconv.FormatInt(MakeXid(), 10)
	args := &PutArgs{
		Key:    key,
		Value:  value,
		DoHash: dohash,
		Xid:    xid,
		Me:     ck.me,
	}
	var reply PutReply

	for true {
		// keep trying until it succeeds.
		time.Sleep(viewservice.PingInterval)
		ok := call(ck.view.Primary, "PBServer.Put", args, &reply)
		if ok {
			//fmt.Println("Put: ", args.Key, " ", args.Value)
			return reply.PreviousValue
		}
		ck.QueryView()
	}

	return "???"
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}

func (ck *Clerk) QueryView() {
	view, err := ck.vs.Ping(ck.view.Viewnum)
	if err != nil {
		return
	}
	ck.view = view
}
