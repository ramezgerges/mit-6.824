package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	//goland:noinspection ALL
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetOutput(ioutil.Discard)

	for {
		args := RequestJobArgs{}
		reply := RequestJobReply{}
		callSuccessful := call("Master.RequestJob", &args, &reply)
		if !callSuccessful {
			time.Sleep(time.Second * 2)
			continue
		}
		// Your worker implementation here.
		if reply.JobType == MAP {
			doMap(mapf, reply.MapJob_)
		} else {
			doReduce(reducef, reply.ReduceJob_)
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, mj MapJob) {
	log.SetOutput(ioutil.Discard)

	filename := mj.Filename
	nReduce := mj.NReduce
	index := mj.MapIndex
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("Somehow file.Close() called multiple times THIS SHOULDN'T EVER HAPPEN!!!!")
	}

	var tempFiles []*os.File
	var encoders []*json.Encoder
	for i := 0; i < nReduce; i++ {
		pattern := fmt.Sprintf("mr-%v-%v-*", index, i)
		tempFile, err := ioutil.TempFile(".", pattern)
		if err != nil {
			log.Fatalf("cannot create temp file %v", pattern)
		}
		tempFiles = append(tempFiles, tempFile)

		encoders = append(encoders, json.NewEncoder(tempFile))
	}

	kva := mapf(filename, string(content))
	for _, kv := range kva {
		key := kv.Key
		i := ihash(key) % nReduce

		enc := encoders[i]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("could not encode %v", kv)
		}
	}

	for i, tempFile := range tempFiles {
		newName := fmt.Sprintf("mr-%v-%v", index, i)
		err := tempFile.Close()
		if err != nil {
			log.Fatalf("cannot close temp file %v", tempFile.Name())
		}

		err = os.Rename(tempFile.Name(), newName)
		if err != nil {
			log.Fatalf("cannot rename temp file %v", tempFile.Name())
		}
	}

	call("Master.MarkDone", MarkDoneArgs{MAP, index}, &MarkDoneReply{})
}

func doReduce(reducef func(string, []string) string, rj ReduceJob) {
	log.SetOutput(ioutil.Discard)

	index := rj.ReduceIndex
	nMap := rj.NMap

	var kva []KeyValue
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, index)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		err = file.Close()
		if err != nil {
			log.Fatalf("Somehow file.Close() called multiple times THIS SHOULDN'T EVER HAPPEN!!!!")
		}
	}

	sort.Sort(ByKey(kva))

	pattern := fmt.Sprintf("mr-out-%v-*", index)
	tempFile, err := ioutil.TempFile(".", pattern)
	if err != nil {
		log.Fatalf("cannot create temp file %v", pattern)
	}

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write to file %v", tempFile.Name())
		}

		i = j
	}

	newName := fmt.Sprintf("mr-out-%v", index)
	err = tempFile.Close()
	if err != nil {
		log.Fatalf("cannot close temp file %v", tempFile.Name())
	}

	err = os.Rename(tempFile.Name(), newName)
	if err != nil {
		log.Fatalf("cannot rename temp file %v", tempFile.Name())
	}

	call("Master.MarkDone", MarkDoneArgs{REDUCE, index}, &MarkDoneReply{})
}

// example function to show how to make an RPC call to the master.
// the RPC argument and reply types are defined in rpc.go.
//goland:noinspection ALL
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
//goland:noinspection ALL
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
