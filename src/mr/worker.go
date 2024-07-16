package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type bucket struct {
	filename string
	f        *os.File
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
		failed         bool
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

			f, err := os.Open(file)
			if failed = err != nil; failed {
				log.Fatalf("cannot open %v", file)
				goto abort_mapping
			}
			content, err := ioutil.ReadAll(f)
			if failed = err != nil; failed {
				log.Fatalf("cannot read %v", file)
				goto abort_mapping
			}
			f.Close()

			kv := mapf(file, string(content))

			buckets := make([]*bucket, batchSz)
			closeBuckets := func() {
				for _, b := range buckets {
					if b.f != nil {
						b.f.Close()
					}
				}
			}
			for _, p := range kv {
				bktid := ihash(p.Key) % batchSz
				b := buckets[bktid]
				if b == nil {
					filename := fmt.Sprintf("mr-%d-%d-%d-*", wid, bktid, tkId)
					tmpFilename := fmt.Sprintf("%v-*", filename)
					f, err = ioutil.TempFile("", tmpFilename)
					if failed = err != nil; failed {
						closeBuckets()
						goto abort_mapping
					}
					b = &bucket{
						filename: filename,
						f:        f,
					}
					buckets[bktid] = b
				}
				pair := fmt.Sprintf("%s %s\n", p.Key, p.Value)
				_, err = f.Write([]byte(pair))
				if failed = err != nil; failed {
					closeBuckets()
					goto abort_mapping
				}
			}
			for _, b := range buckets {
				if b.f != nil {
					err := os.Rename(b.f.Name(), b.filename)
					if failed = err != nil; failed {
						closeBuckets()
						goto abort_mapping
					}
					b.f.Close()
				}
			}
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
abort_mapping:
	{
		args := MappingDoneArgs{
			Wid:           wid,
			Intermidiates: intermidiates,
			TkId:          tkId,
			Failed:        failed,
		}
		reply := MappingDoneReply{}
		if !call("Master.MappingDone", &args, &reply) {
			return
		}
		switch reply.Response {
		case Accepted | IntermidiateDone:
		case Accepted:
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
			// TODO: write to disk
		case InvalidWorkerId:
			if !genWorker() {
				return
			}
		case Finished:
			return
		}
	}
abort_reduction:
	{
		args := ReductionDoneArgs{
			Wid:    wid,
			TkId:   tkId,
			Failed: failed,
		}
		reply := ReductionDoneReply{}
		if !call("Master.ReductionDone", &args, &reply) {
			return
		}
		switch reply.Response {
		case Accepted | Finished:
		case Accepted:
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

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Fatal("calling: ", err)

	return false
}
