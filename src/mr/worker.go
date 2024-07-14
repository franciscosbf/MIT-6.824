package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const initalizationTries = 3

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	var (
		wid            int
		tkId           int64
		batchSz        int
		file           string
		files          []string
		intermidiates  []Intermidiate
		remainingTries = initalizationTries
	)

	genWorker := func() bool {
		if remainingTries--; remainingTries == 0 {
			return false
		}

		args := GenWorkerArgs{}
		reply := GenWorkerReply{}
		if !call("Master.GenWorker", &args, &reply) {
			return true
		}

		wid = reply.Wid
		batchSz = reply.BatchSz

		return true
	}

	if !genWorker() {
		return
	}

mapping:
	{
		args := MappingRequestArgs{Wid: wid}
		reply := MappingRequestReply{}
		if !call("Master.MappingRequest", &args, &reply) {
			return
		}
		switch reply.Response {
		case ToDo:
			file = reply.File
			tkId = reply.TkId
			// TODO: do map stuff
		case IntermidiateDone:
			goto reduction
		case InvalidWorkerId:
			if !genWorker() {
				return
			}
		case Finished:
			return
		}
	}
	{
		args := MappingDoneArgs{Wid: wid, Intermidiates: intermidiates, TkId: tkId}
		reply := MappingDoneReply{}
		if !call("Master.MappingDone", &args, &reply) {
			return
		}
		switch reply.Response {
		case Accepted | IntermidiateDone:
			// TODO: write to disk
		case Accepted:
			// TODO: write to disk
			goto mapping
		case InvalidTaskId:
			goto mapping
		case InvalidWorkerId:
			if !genWorker() {
				return
			}
		case Finished:
			return
		}
	}

reduction:
	{
		args := ReductionRequestArgs{Wid: wid}
		reply := ReductionRequestReply{}
		if !call("Master.ReductionRequest", &args, &reply) {
			return
		}
		switch reply.Response {
		case ToDo:
			files = reply.Files
			tkId = reply.TkId
			// TODO: do reduction stuff
		case InvalidWorkerId:
			if !genWorker() {
				return
			}
		case Finished:
			return
		}
	}
	{
		args := ReductionDoneArgs{Wid: wid, TkId: tkId}
		reply := ReductionDoneReply{}
		if !call("Master.ReductionDone", &args, &reply) {
			return
		}
		switch reply.Response {
		case Accepted | Finished:
			// TODO: write to disk
		case Accepted:
			// TODO: write to disk
			goto reduction
		case InvalidTaskId:
			goto reduction
		case InvalidWorkerId:
			if genWorker() {
				goto reduction
			}
		case Finished:
		}
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {
// 	// declare an argument structure.
// 	args := ExampleArgs{}
//
// 	// fill in the argument(s).
// 	args.X = 99
//
// 	// declare a reply structure.
// 	reply := ExampleReply{}
//
// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)
//
// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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

	fmt.Println(err)
	return false
}
