package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type bucket struct {
	f        *os.File
	filename string
	kvs      []KeyValue
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func workerDie(wid int, format string, v ...interface{}) {
	format = fmt.Sprintf("worker %v: %v", wid, format)
	log.Fatalf(format, v...)
}

func workerInfo(wid int, format string, v ...interface{}) {
	format = fmt.Sprintf("worker %v: %v", wid, format)
	log.Printf(format, v...)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	var (
		wid           int
		tkId          int64
		batchSz       int
		file          string
		intermidiates []Intermidiate
		files         []string
	)

	{
		args := GenWorkerArgs{}
		reply := GenWorkerReply{}
		call("Master.GenWorker", &args, &reply)

		wid = reply.Wid
		batchSz = reply.BatchSz

		workerInfo(wid, "registered with batch size of %v", batchSz)
	}

mapping:
	{
		args := MappingRequestArgs{Wid: wid}
		reply := MappingRequestReply{}
		call("Master.MappingRequest", &args, &reply)
		switch reply.Response {
		case ToDo:
			intermidiates = nil
			file = reply.File
			tkId = reply.TkId
			mId := reply.Id

			workerInfo(wid, "proceeding to map %v (task id %v)", file, tkId)

			f, err := os.Open(file)
			if err != nil {
				workerDie(wid, "cannot open %v: %v\n", file, err)
			}
			content, err := ioutil.ReadAll(f)
			if err != nil {
				workerDie(wid, "cannot read %v: %v\n", file, err)
			}
			f.Close()

			kvs := mapf(file, string(content))

			buckets := make([]*bucket, batchSz)

			for _, p := range kvs {
				bktid := ihash(p.Key) % batchSz
				b := buckets[bktid]
				if b == nil {
					filename := fmt.Sprintf("mr-%v-%v", mId, bktid)
					tmpFilename := fmt.Sprintf("%v-*", filename)
					f, err = ioutil.TempFile(".", tmpFilename)
					if err != nil {
						workerDie(wid, "cannot open tmp file %v: %v\n", tmpFilename, err)
					}
					b = &bucket{
						filename: filename,
						f:        f,
					}
					buckets[bktid] = b
				}
				b.kvs = append(b.kvs, p)
			}

			for btkid, b := range buckets {
				if b == nil {
					continue
				}

				bts, err := json.Marshal(b.kvs)
				if err != nil {
					workerDie(wid, "cannot marshal key-value pairs")
				}
				b.f.Write(bts)
				b.f.Close()
				if err := os.Rename(b.f.Name(), b.filename); err != nil {
					workerDie(wid, "cannot rename %v to %v: %v\n", b.f.Name(), b.filename, err)
				}

				it := Intermidiate{
					File: b.filename,
					Rid:  btkid,
				}
				intermidiates = append(intermidiates, it)
			}
		case IntermidiateDone:
			workerInfo(wid, "intermidiate step is done")
			goto reduction
		case InvalidWorkerId:
			workerInfo(wid, "got invalid worker id while trying to request a mapping")
			return
		case Finished:
			workerInfo(wid, "got finished while trying to request a mapping")
			return
		}
	}
	{
		args := MappingDoneArgs{
			Wid:           wid,
			Intermidiates: intermidiates,
			TkId:          tkId,
		}
		reply := MappingDoneReply{}
		call("Master.MappingDone", &args, &reply)
		switch reply.Response {
		case Accepted | IntermidiateDone:
			workerInfo(wid, "delivered intermidiate files %v (task id %v) and this phase has finished", intermidiates, tkId)
		case Accepted:
			workerInfo(wid, "delivered intermidiate files %v (task id %v)", intermidiates, tkId)
			goto mapping
		case InvalidTaskId:
			workerInfo(wid, "mapping of %v (old task id %v) was already reassigned", file, tkId)
			goto mapping
		case InvalidWorkerId:
			workerInfo(wid, "got invalid worker id while trying to deliver intermidiate files %v (task id %v)", intermidiates, tkId)
			return
		case Finished:
			workerInfo(wid, "got finished while trying to deliver intermidiate files %v (task id %v)", intermidiates, tkId)
			return
		}
	}

reduction:
	{
		args := ReductionRequestArgs{Wid: wid}
		reply := ReductionRequestReply{}
		call("Master.ReductionRequest", &args, &reply)
		switch reply.Response {
		case ToDo:
			files = reply.Files
			tkId = reply.TkId
			rId := reply.Id

			workerInfo(wid, "proceeding to reduce %v (task id %v)", files, tkId)

			var nWorkers int

			nFiles := len(files)
			nCpus := runtime.NumCPU()
			jobs := make(chan string, nFiles)
			prereduction := make(chan []KeyValue, nFiles)
			finalKvs := []KeyValue{}

			for _, file := range files {
				jobs <- file
			}

			if nFiles < nCpus {
				nWorkers = nFiles
			} else {
				nWorkers = nCpus
			}

			for i := nWorkers; i > 0; i-- {
				go func(
					jobs <-chan string,
					prereduction chan<- []KeyValue,
				) {
					var (
						file string
						ok   bool
					)

					for {
						if file, ok = <-jobs; !ok {
							return
						}

						f, err := os.Open(file)
						if err != nil {
							workerDie(wid, "cannot open %v: %v\n", file, err)
							prereduction <- nil
							return
						}
						content, err := ioutil.ReadAll(f)
						if err != nil {
							workerDie(wid, "cannot read %v: %v\n", file, err)
							prereduction <- nil
							return
						}
						f.Close()

						kvs := []KeyValue{}
						if err := json.Unmarshal(content, &kvs); err != nil {
							workerDie(wid, "cannot unmarshal key-value pairs")
						}
						prereduction <- kvs
					}
				}(jobs, prereduction)
			}

			for i := nFiles; i > 0; i-- {
				finalKvs = append(finalKvs, (<-prereduction)...)
			}
			close(jobs)

			filename := fmt.Sprintf("mr-out-%d", rId)
			tmpFilename := fmt.Sprintf("%v-*", filename)
			f, err := ioutil.TempFile(".", tmpFilename)
			if err != nil {
				workerDie(wid, "cannot open tmp file %v: %v\n", tmpFilename, err)
			}

			sort.Sort(ByKey(finalKvs))
			nKvs := len(finalKvs)
			i := 0
			for i < nKvs {
				j := i + 1
				for j < nKvs && finalKvs[j].Key == finalKvs[i].Key {
					j++
				}

				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, finalKvs[k].Value)
				}
				output := reducef(finalKvs[i].Key, values)

				if _, err := fmt.Fprintf(f, "%v %v\n", finalKvs[i].Key, output); err != nil {
					workerDie(wid, "cannot write to %v: %v\n", f.Name(), err)
				}

				i = j
			}

			f.Close()

			if err = os.Rename(f.Name(), filename); err != nil {
				workerDie(wid, "cannot rename %v to %v: %v\n", f.Name(), filename, err)
			}
		case InvalidWorkerId:
			workerInfo(wid, "got invalid worker id while trying to request a reduction")
			return
		case Finished:
			workerInfo(wid, "got finished while trying to request a reduction")
			return
		}
	}
	{
		args := ReductionDoneArgs{
			Wid:  wid,
			TkId: tkId,
		}
		reply := ReductionDoneReply{}
		call("Master.ReductionDone", &args, &reply)
		switch reply.Response {
		case Accepted | Finished:
			workerInfo(wid, "signaled completed reduction (task id %v) and the program as finished", tkId)
		case Accepted:
			workerInfo(wid, "signaled completed reduction (task id %v)", tkId)
			goto reduction
		case InvalidTaskId:
			workerInfo(wid, "reduction of %v (old task id %v) was already reassigned", files, tkId)
			goto reduction
		case InvalidWorkerId:
			workerInfo(wid, "got invalid worker id while trying to signaling completed reduction (task id %v)", intermidiates, tkId)
		case Finished:
			workerInfo(wid, "got finished while trying to signaling completed reduction (task id %v)", intermidiates, tkId)
		}
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		log.Fatal("calling: ", err)
	}
}
