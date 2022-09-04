package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"
import "github.com/google/uuid"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	i := 0
	id := uuid.New().String()
	for {
		args := GetArgs{key, id}
		reply := GetReply{}

		okCh := make(chan bool)
		go func() {
			okCh <- ck.servers[i].Call("KVServer.Get", &args, &reply)
		}()

		select {
		case ok := <-okCh:
			if ok {
				if reply.Err == OK {
					return reply.Value
				} else if reply.Err == ErrNoKey {
					return ""
				}
			}
		case <-time.After(100 * time.Millisecond):

		}

		i = (i + 1) % len(ck.servers)
		time.Sleep(2 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	i := 0
	id := uuid.New().String()
	for {
		args := PutAppendArgs{key, value, Operation(op), id}
		reply := PutAppendReply{}

		okCh := make(chan bool)
		go func() {
			okCh <- ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		}()

		select {
		case ok := <-okCh:
			if ok {
				if reply.Err == OK {
					return
				}
			}
		case <-time.After(100 * time.Millisecond):
		}

		i = (i + 1) % len(ck.servers)
		time.Sleep(2 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
