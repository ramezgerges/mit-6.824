package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Id, Term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

const (
	ElectionTimeout      = 500 * time.Millisecond
	ElectionTimeoutRange = 50
	HeartbeatTimeout     = 150 * time.Millisecond
	MaxEntriesPerMessage = 100
)

// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	id                int                 // this peer's id into peers[]
	dead              int32               // set by Kill()
	CurrentTerm       int
	VotedFor          int
	role              string
	time              time.Time
	Log               []LogEntry
	commitIndex       int
	nextIndex         []int
	matchIndex        []int
	expBackoff        []int
	lastApplied       int
	snapshotLastIndex int
	snapshotLastTerm  int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type PrettyRaft struct {
	id          int   // this peer's id into peers[]
	dead        int32 // set by Kill()
	currentTerm int
	votedFor    int
	role        string
	commitIndex int
	nextIndex   []int
	matchIndex  []int
}

func (rf *Raft) pretty() PrettyRaft {
	return PrettyRaft{
		id:          rf.id,
		dead:        rf.dead,
		currentTerm: rf.CurrentTerm,
		votedFor:    rf.VotedFor,
		role:        rf.role,
		commitIndex: rf.commitIndex,
		nextIndex:   rf.nextIndex,
		matchIndex:  rf.matchIndex,
	}
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentTerm, rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	list := make([]interface{}, 0)
	list = append(list, rf.CurrentTerm)
	list = append(list, rf.VotedFor)
	list = append(list, rf.Log)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	for _, item := range list {
		err := e.Encode(item)
		if err != nil {
			panic(nil)
		}
	}

	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	var currentTerm int
	var votedFor int
	var log []LogEntry

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&(currentTerm)) != nil ||
		d.Decode(&(votedFor)) != nil || d.Decode(&(log)) != nil {
		panic(nil)
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
	}
}

func (rf *Raft) updateTerm(term int) {
	if term > rf.CurrentTerm {
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.role = FOLLOWER
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Id           int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	Id           int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		DPrintf("rf[%+v]: Finished RequestVote RPC reply:%+v, my state:%+v, args:%+v", rf.id, reply, rf.pretty(), args)
		rf.persist()
		rf.mu.Unlock()
	}()

	DPrintf("rf[%+v]: Received RequestVote RPC args:%+v, my state:%+v", rf.id, args, rf.pretty())

	rf.updateTerm(args.Term)

	reply.Term = rf.CurrentTerm

	var lastLogTerm, lastLogIndex int
	if len(rf.Log) == 0 {
		lastLogTerm, lastLogIndex = rf.snapshotLastTerm, rf.snapshotLastIndex
	} else {
		lastLogTerm, lastLogIndex = rf.Log[len(rf.Log)-1].Term, rf.Log[len(rf.Log)-1].Index
	}

	voteNotGranted := args.Term < rf.CurrentTerm || // old term
		(rf.VotedFor != -1 && rf.VotedFor != args.Id) || // already voted
		CompareEntries(lastLogTerm, lastLogIndex, args.LastLogTerm, args.LastLogIndex) > 0 // outdated leader
	reply.VoteGranted = !voteNotGranted

	if reply.VoteGranted {
		rf.VotedFor = args.Id
		rf.time = time.Now()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		DPrintf("rf[%+v]: Finished AppendEntries RPC reply:%+v, my state:%+v, args:%+v", rf.id, reply, rf.pretty(), args)
		rf.persist()
		rf.mu.Unlock()
	}()

	DPrintf("rf[%+v]: Received AppendEntries RPC args:%+v, my state:%+v", rf.id, args, rf.pretty())

	rf.updateTerm(args.Term)

	reply.Term = rf.CurrentTerm

	if args.Term >= rf.CurrentTerm {
		rf.time = time.Now()
	}

	var lastLogIndex int
	if len(rf.Log) == 0 {
		lastLogIndex = rf.snapshotLastIndex
	} else {
		lastLogIndex = rf.Log[len(rf.Log)-1].Index
	}

	failure := args.Term < rf.CurrentTerm || // old term
		lastLogIndex < args.PrevLogIndex // gap

	// remove entries that are in the snapshot from args
	// Since indexes in the snapshot are committed, and leaders of higher terms have the committed indexes
	// it's impossible to have a conflict in indexes that are in the snapshot, so we can be confident that
	// the entries at these indexes are matched without looking at these entries.
	// This is necessary because entries in the snapshot are no longer available, so we cannot directly compare them
	// for a match.
	if !failure { // i.e. no gaps

		removedEntries := Max(0, Min(len(args.Entries), rf.snapshotLastIndex-args.PrevLogIndex))
		args.Entries = args.Entries[removedEntries:]
		args.PrevLogIndex += removedEntries

		if args.PrevLogIndex == rf.snapshotLastIndex {
			failure = CompareEntries(rf.snapshotLastTerm, rf.snapshotLastIndex, args.PrevLogTerm, args.PrevLogIndex) != 0 // outdated leader
		} else if args.PrevLogIndex > rf.snapshotLastIndex {
			// again, we can do this without index checks because no gaps
			failure = CompareEntries(rf.Log[args.PrevLogIndex-rf.Log[0].Index].Term, rf.Log[args.PrevLogIndex-rf.Log[0].Index].Index, args.PrevLogTerm, args.PrevLogIndex) != 0 // outdated leader
		}
		// otherwise, all entries in args are in the snapshot and so, failure = false
	}

	reply.Success = !failure

	if reply.Success {
		for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+1+len(args.Entries); i++ {
			entry := args.Entries[i-(args.PrevLogIndex+1)]

			if len(rf.Log) > 0 && rf.Log[len(rf.Log)-1].Index >= i && rf.Log[i-rf.Log[0].Index].Term != entry.Term {
				rf.Log = rf.Log[:i-rf.Log[0].Index]
			}

			if len(rf.Log) == 0 || rf.Log[len(rf.Log)-1].Index == i-1 {
				rf.Log = append(rf.Log, entry)
			}
		}

		rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, args.PrevLogIndex+len(args.Entries)))
	}
}

// example code to send a RequestVote RPC to a server.
// server is the id of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Id that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	DPrintf("rf[%+v]: Started Start my state:%+v, command:%+v", rf.id, rf.pretty(), command)

	if rf.killed() || rf.role != LEADER {
		DPrintf("rf[%+v]: Finished Start, my state:%+v, reply:{%+v},{%+v},{%+v}", rf.id, rf.pretty(), -1, -1, false)
		return -1, -1, false
	} else {
		var lastIndex int
		if len(rf.Log) == 0 {
			lastIndex = rf.snapshotLastIndex
		} else {
			lastIndex = rf.Log[len(rf.Log)-1].Index
		}

		rf.Log = append(rf.Log, LogEntry{
			Command: command,
			Term:    rf.CurrentTerm,
			Index:   lastIndex + 1,
		})

		DPrintf("rf[%+v]: Finished Start, my state:%+v, reply:{%+v},{%+v},{%+v}", rf.id, rf.pretty(), rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term, true)
		return rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term, true
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) CandidateLoop() {
	defer func() {
		DPrintf("rf[%+v]: Finished CandidateLoop, my state:%+v", rf.id, rf.pretty())
		if rf.role == LEADER {
			rf.LeaderLoop(true)
		} else {
			rf.mu.Unlock()
		}
	}()

	rf.role = CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.id
	rf.time = time.Now()

	DPrintf("rf[%+v]: Started CandidateLoop my state:%+v", rf.id, rf.pretty())

	termAtElectionStart := rf.CurrentTerm

	var lastLogTerm, lastLogIndex int

	if len(rf.Log) == 0 {
		lastLogTerm, lastLogIndex = rf.snapshotLastTerm, rf.snapshotLastIndex
	} else {
		lastLogTerm, lastLogIndex = rf.Log[len(rf.Log)-1].Term, rf.Log[len(rf.Log)-1].Index
	}

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		Id:           rf.id,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}

	rf.persist()
	rf.mu.Unlock()

	receivedTerm := make([]int, len(rf.peers))
	var votes int32 = 1
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.id {
			continue
		}

		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(i, args, reply)
			if !ok {
				return
			}

			if reply.VoteGranted {
				atomic.AddInt32(&votes, 1)
				if int(votes) > len(rf.peers)/2 {
					rf.mu.Lock()
					if rf.CurrentTerm == termAtElectionStart && rf.role != LEADER {
						rf.role = LEADER
						for j := range rf.nextIndex {
							rf.nextIndex[j] = lastLogIndex + 1
							rf.matchIndex[j] = 0
							rf.expBackoff[j] = 0
						}
					}
					rf.persist()
					rf.mu.Unlock()
				}
			}

			receivedTerm[i] = reply.Term
		}(i)
	}

	wg.Wait()

	maxReceivedTerm := -1
	for _, term := range receivedTerm {
		if term > maxReceivedTerm {
			maxReceivedTerm = term
		}
	}

	rf.mu.Lock()

	if rf.CurrentTerm != termAtElectionStart {
		return
	}

	if maxReceivedTerm > rf.CurrentTerm {
		rf.CurrentTerm = maxReceivedTerm
		rf.role = FOLLOWER
		rf.persist()
		return
	} else if rf.role != LEADER {
		rf.role = FOLLOWER
	}
}

func (rf *Raft) LeaderLoop(isHeartbeat bool) {
	defer func() {
		DPrintf("rf[%+v]: Finished LeaderLoop, my state:%+v", rf.id, rf.pretty())
		rf.mu.Unlock()
	}()

	DPrintf("rf[%+v]: Started LeaderLoop my state:%+v", rf.id, rf.pretty())

	termAtHeartbeatStart := rf.CurrentTerm

	argsArr := make([]*AppendEntriesArgs, len(rf.peers))

	for i := range argsArr {
		if i == rf.id {
			continue
		}
		if len(rf.Log) > 0 && rf.Log[len(rf.Log)-1].Index >= rf.nextIndex[i] && rf.nextIndex[i] >= rf.Log[0].Index {
			var prevLogTerm, prevLogIndex int

			if idx := rf.nextIndex[i] - 1 - rf.Log[0].Index; idx >= 0 {
				prevLogTerm, prevLogIndex = rf.Log[idx].Term, rf.Log[idx].Index
			} else {
				prevLogTerm, prevLogIndex = rf.snapshotLastTerm, rf.snapshotLastIndex
			}
			argsArr[i] = &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				Id:           rf.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      rf.Log[rf.nextIndex[i]-rf.Log[0].Index : rf.nextIndex[i]-rf.Log[0].Index+Min(MaxEntriesPerMessage, 1+(rf.Log[len(rf.Log)-1].Index-rf.Log[0].Index)-(rf.nextIndex[i]-rf.Log[0].Index))],
				CommitIndex:  rf.commitIndex,
			}
		} else if rf.snapshotLastIndex >= rf.nextIndex[i] {
			// TODO InstallSnapshot RPC
			log.Fatalf("rf[%+v]: TODO: InstallSnapshot RPC, rf.snapshotLastIndex:%+v, rf.nextIndex[%+v]:%+v\n", rf.id, rf.snapshotLastIndex, i, rf.nextIndex[i])
		} else if isHeartbeat {
			// TODO refactor the following lines out based on the fact that last log in rf.Log == rf.nextIndex[i]
			var prevLogTerm, prevLogIndex int
			if len(rf.Log) > 0 {
				prevLogTerm, prevLogIndex = rf.Log[len(rf.Log)-1].Term, rf.Log[len(rf.Log)-1].Index
			} else {
				prevLogTerm, prevLogIndex = rf.snapshotLastTerm, rf.snapshotLastIndex
			}

			argsArr[i] = &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				Id:           rf.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{},
				CommitIndex:  rf.commitIndex,
			}
		}
	}

	rf.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.id {
			continue
		}

		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			reply := &AppendEntriesReply{}
			if argsArr[i] != nil {
				// TODO send InstallSnapshot sometimes
				ok := rf.sendAppendEntries(i, argsArr[i], reply)
				if ok {
					rf.mu.Lock()
					defer func() {
						rf.persist()
						rf.mu.Unlock()
					}()

					if termAtHeartbeatStart == rf.CurrentTerm {
						rf.updateTerm(reply.Term)

						if reply.Success {
							newMatchIndex := argsArr[i].PrevLogIndex + len(argsArr[i].Entries)
							if newMatchIndex > rf.matchIndex[i] {
								rf.matchIndex[i] = newMatchIndex
								rf.nextIndex[i] = rf.matchIndex[i] + 1
							}
							//DPrintf("matchIndex[%v] = %v + %v, reply.Success = %v", rf.matchIndex[i], argsArr[i].PrevLogIndex, len(argsArr[i].Entries), reply.Success)
							rf.expBackoff[i] = 0
						} else {
							rf.nextIndex[i] -= int(math.Pow(2, float64(rf.expBackoff[i])))
							if rf.nextIndex[i] < 1 {
								rf.nextIndex[i] = 1
							}
							rf.expBackoff[i]++
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
	rf.mu.Lock()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		//mu:          sync.Mutex{},
		peers:             peers,
		persister:         persister,
		id:                me,
		dead:              -1,
		CurrentTerm:       0,
		VotedFor:          -1,
		role:              FOLLOWER,
		time:              time.Now(),
		Log:               []LogEntry{},
		commitIndex:       0,
		matchIndex:        make([]int, len(peers)),
		nextIndex:         make([]int, len(peers)),
		expBackoff:        make([]int, len(peers)),
		lastApplied:       0,
		snapshotLastIndex: 0,
		snapshotLastTerm:  0,
	}

	rf.readPersist(persister.ReadRaftState())

	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()

			if !(rf.role != LEADER && time.Now().Sub(rf.time) > ElectionTimeout+time.Duration(rand.Intn(ElectionTimeoutRange))*time.Millisecond) {
				rf.mu.Unlock()
			} else {
				go rf.CandidateLoop()
			}
		}
	}()

	go func() {
		for !rf.killed() {
			time.Sleep(HeartbeatTimeout)
			rf.mu.Lock()

			if rf.role != LEADER {
				rf.mu.Unlock()
			} else {
				go rf.LeaderLoop(true)
			}
		}
	}()

	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()

			if rf.role != LEADER {
				rf.mu.Unlock()
			} else {
				go rf.LeaderLoop(false)
			}
		}
	}()

	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()

			// snapshots contain already committed logs, so if
			// all entries are in the snapshot, then commitIndex cannot increase
			if rf.role == LEADER && len(rf.Log) > 0 {
				matchIndex := make([]int, 0)
				for i, index := range rf.matchIndex {
					if i == rf.id {
						index = rf.Log[len(rf.Log)-1].Index
					}

					if index-rf.Log[0].Index >= 0 && rf.Log[index-rf.Log[0].Index].Term == rf.CurrentTerm {
						matchIndex = append(matchIndex, index)
					} else {
						matchIndex = append(matchIndex, -1000)
					}
				}
				sort.Slice(matchIndex, func(i, j int) bool {
					return matchIndex[i] < matchIndex[j]
				})
				//DPrintf("matchIndex: %+v", matchIndex)
				majorityCommitIndex := matchIndex[len(matchIndex)/2]
				if majorityCommitIndex != -1000 {
					rf.commitIndex = majorityCommitIndex
				}
				//DPrintf("matchIndex: %+v", matchIndex)
			}

			rf.mu.Unlock()
		}
	}()

	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()

			if len(rf.Log) == 0 {
				rf.mu.Unlock()
				continue
			}

			currentLastApplied := rf.lastApplied
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()

			for currentLastApplied < rf.lastApplied {
				currentLastApplied++
				DPrintf("rf[%v]: Applied %+v, rf.lastApplied = %v, rf.commitIndex = %v", rf.id, rf.Log[currentLastApplied-rf.Log[0].Index], rf.lastApplied, rf.commitIndex)
				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.Log[currentLastApplied-rf.Log[0].Index].Command,
					CommandIndex: rf.Log[currentLastApplied-rf.Log[0].Index].Index,
				}
			}
		}
	}()

	return rf
}
