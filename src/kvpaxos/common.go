package kvpaxos

import (
	"crypto/rand"
	"hash/fnv"
	"math/big"
)

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongSequence = "ErrWrongSequence"
	ErrUnknown       = "ErrUnknown"
)

type Err string

type PutArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Xid      int64
	Sequence int64
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Xid      int64
	Sequence int64
}

type GetReply struct {
	Err   Err
	Value string
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func MakeXid() int64 {
	max := big.NewInt(int64(1) << 62)
	ri, _ := rand.Int(rand.Reader, max)
	xid := ri.Int64()
	return xid
}
