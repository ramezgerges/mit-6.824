package mr

import (
	"io/ioutil"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const ( // iota is reset to 0
	UNSTARTED = iota // c0 == 0
	RUNNING   = iota // c1 == 1
	FINISHED  = iota // c2 == 2
)

type RequestJobError struct {
	errorMessage string
}

type MapJob struct {
	Filename string
	NReduce  int
	MapIndex int
}

type ReduceJob struct {
	ReduceIndex int
	NMap        int
}

func (e RequestJobError) Error() string {
	return e.errorMessage
}

type Master struct {
	// Your definitions here.
	isDone             bool
	nMap               int
	nReduce            int
	mapFiles           []string
	mapFileStatuses    []int
	reduceFileStatuses []int
	mu                 sync.Mutex
	nDone              int
}

//
// start a thread that listens for RPCs from worker.go
//
//goland:noinspection GoUnhandledErrorResult
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.isDone
}

func (m *Master) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	log.SetOutput(ioutil.Discard)
	defer log.Printf("RequestJob %v, replied with %v\n", args, reply)

	mapsFinished := true

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, status := range m.mapFileStatuses {
		if status != FINISHED {
			mapsFinished = false
		}
		if status == UNSTARTED {
			reply.JobType = MAP
			reply.MapJob_ = MapJob{
				Filename: m.mapFiles[i],
				NReduce:  m.nReduce,
				MapIndex: i,
			}
			m.mapFileStatuses[i] = RUNNING
			go func(i int) {
				time.Sleep(time.Second * 10)
				m.mu.Lock()
				defer m.mu.Unlock()
				if m.mapFileStatuses[i] == RUNNING {
					m.mapFileStatuses[i] = UNSTARTED
				}
			}(i)
			return nil
		}
	}

	if !mapsFinished {
		return RequestJobError{errorMessage: "No UNSTARTED map jobs available."}
	}

	for i, status := range m.reduceFileStatuses {
		if status == UNSTARTED {
			reply.JobType = REDUCE
			reply.ReduceJob_ = ReduceJob{
				ReduceIndex: i,
				NMap:        m.nMap,
			}
			m.reduceFileStatuses[i] = RUNNING
			go func(i int) {
				time.Sleep(time.Second * 10)
				m.mu.Lock()
				defer m.mu.Unlock()
				if m.reduceFileStatuses[i] == RUNNING {
					m.reduceFileStatuses[i] = UNSTARTED
				}
			}(i)
			return nil
		}
	}

	return RequestJobError{errorMessage: "No UNSTARTED reduce jobs available."}
}

func (m *Master) MarkDone(args *MarkDoneArgs, reply *MarkDoneReply) error {
	log.Printf("MarkDone %v\n", args)
	index := args.Index
	jobType := args.JobType

	m.mu.Lock()
	defer m.mu.Unlock()

	if jobType == MAP {
		m.mapFileStatuses[index] = FINISHED
	} else {
		m.reduceFileStatuses[index] = FINISHED
		m.nDone += 1
		if m.nDone == m.nReduce {
			m.isDone = true
		}
	}

	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	fileStatuses := make([]int, len(files))
	log.SetOutput(ioutil.Discard)

	reduceFileStatuses := make([]int, nReduce)
	for i := range fileStatuses {
		fileStatuses[i] = UNSTARTED
	}
	for i := range reduceFileStatuses {
		reduceFileStatuses[i] = UNSTARTED
	}

	m := Master{
		isDone:             false,
		mapFiles:           files,
		nReduce:            nReduce,
		nMap:               len(files),
		mapFileStatuses:    fileStatuses,
		nDone:              0,
		reduceFileStatuses: reduceFileStatuses,
	}

	log.Println("Master created.")
	m.server()
	return &m
}
