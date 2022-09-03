package kvraft

import (
	"log"
	"os"
)

func DPrintf(format string, a ...interface{}) {
	log.SetFlags(log.Lmicroseconds | log.Ltime)
	if os.Getenv("DEBUG") == "1" {
		log.Printf(format, a...)
	}
	return
}

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrNotCommitted = "NotCommitted"
)

type Err string

const (
	PUT    = "Put"
	APPEND = "Append"
	GET    = "Get"
)

type Operation string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    Operation // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
