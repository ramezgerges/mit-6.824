package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    Operation // "Put" or "Append"
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store     map[string]string
	storeLock sync.Mutex
	chMap     map[int]int
	chMapLock sync.Mutex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	defer DPrintf("kvserver[%+v]: Finished KVServer.Get, args=%+v, reply=%+v", kv.me, args, reply)

	index, term, success := kv.rf.Start(Op{
		Key:   args.Key,
		Value: "",
		Op:    GET,
	})

	DPrintf("kvserver[%+v]: Started KVServer.Get, index=%+v, term=%+v, success=%+v, args=%+v", kv.me, index, term, success, args)

	if success {
		for {
			time.Sleep(10 * time.Millisecond)

			kv.chMapLock.Lock()
			val, ok := kv.chMap[index]
			DPrintf("kvserver[%+v]: kv.chMap[%+v]= %+v,%+v", kv.me, index, val, ok)
			kv.chMapLock.Unlock()
			if ok {
				DPrintf("kvserver[%+v]: in OK", kv.me)
				if val == term {
					kv.storeLock.Lock()
					reply.Value, ok = kv.store[args.Key]
					kv.storeLock.Unlock()

					DPrintf("kvserver[%+v]: after storeLock", kv.me)
					if ok {
						reply.Err = OK
					} else {
						reply.Err = ErrNoKey
					}
				} else {
					reply.Err = ErrNotCommitted
				}

				DPrintf("kvserver[%+v]: broke", kv.me)
				break
			}
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer DPrintf("kvserver[%+v]: Finished KVServer.PutAppend, args=%+v, reply=%+v", kv.me, args, reply)

	index, term, success := kv.rf.Start(Op{
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
	})

	DPrintf("kvserver[%+v]: Started KVServer.PutAppend, index=%+v, term=%+v, success=%+v, args=%+v", kv.me, index, term, success, args)

	if success {
		for {
			time.Sleep(10 * time.Millisecond)

			kv.chMapLock.Lock()
			val, ok := kv.chMap[index]
			kv.chMapLock.Unlock()
			if ok {
				if val == term {
					reply.Err = OK
				} else {
					reply.Err = ErrNotCommitted
				}

				break
			}
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.chMap = make(map[int]int)

	go func() {
		for msg := range kv.applyCh {
			DPrintf("kvserver[%+v]: Applied Message Index:%+v,%+v", kv.me, msg.CommandIndex, msg.CommandTerm)
			if msg.CommandValid {
				kv.chMapLock.Lock()
				kv.chMap[msg.CommandIndex] = msg.CommandTerm
				kv.chMapLock.Unlock()

				op := (msg.Command).(Op)

				if op.Op == PUT || op.Op == APPEND {
					kv.storeLock.Lock()
					if op.Op == PUT {
						kv.store[op.Key] = op.Value
					} else {
						val, ok := kv.store[op.Key]
						if ok {
							kv.store[op.Key] = val + op.Value
						} else {
							kv.store[op.Key] = op.Value
						}
					}
					kv.storeLock.Unlock()
				}
			}
		}
	}()

	return kv
}
