package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"sort"
	"strings"
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
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func die(wid int, format string, v ...any) {
	format = fmt.Sprintf("worker %v: %v", wid, format)
	log.Fatalf(format, v...)
}

func info(wid int, format string, v ...any) {
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

		info(wid, "registered with batch size of %v", batchSz)
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

			info(wid, "proceeding to map %v (task id %v)", file, tkId)

			f, err := os.Open(file)
			if err != nil {
				die(wid, "cannot open %v\n", file)
			}
			content, err := ioutil.ReadAll(f)
			if err != nil {
				die(wid, "cannot read %v\n", file)
			}
			f.Close()

			kvs := mapf(file, string(content))

			buckets := make([]*bucket, batchSz)

			for _, p := range kvs {
				bktid := ihash(p.Key) % batchSz
				b := buckets[bktid]
				if b == nil {
					filename := fmt.Sprintf("mr-%v-%v", wid, bktid)
					tmpFilename := fmt.Sprintf("%v-*", filename)
					f, err = ioutil.TempFile("", tmpFilename)
					if err != nil {
						die(wid, "cannot open tmp file %v\n", tmpFilename)
					}
					buckets[bktid] = &bucket{
						filename: filename,
						f:        f,
					}
				}
				if _, err = fmt.Fprintf(f, "%v %v\n", p.Key, p.Value); err != nil {
					die(wid, "cannot write to %v\n", f.Name())
				}
			}

			for btkid, b := range buckets {
				if b != nil {
					err := os.Rename(b.f.Name(), b.filename)
					if err != nil {
						die(wid, "cannot rename %v to %v\n", b.f.Name(), b.filename)
					}
					b.f.Close()
				}
				it := Intermidiate{
					File: b.filename,
					Rid:  btkid,
				}
				intermidiates = append(intermidiates, it)
			}
		case IntermidiateDone:
			info(wid, "intermidiate step is done")
			goto reduction
		case InvalidWorkerId:
			info(wid, "got invalid worker id while trying to request a mapping")
			return
		case Finished:
			info(wid, "got finished while trying to request a mapping")
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
			info(wid, "delivered intermidiate files %v (task id %v) and this phase has finished", intermidiates, tkId)
		case Accepted:
			info(wid, "delivered intermidiate files %v (task id %v)", intermidiates, tkId)
			goto mapping
		case InvalidTaskId:
			info(wid, "mapping of %v (old task id %v) was already reassigned", file, tkId)
			goto mapping
		case InvalidWorkerId:
			info(wid, "got invalid worker id while trying to deliver intermidiate files %v (task id %v)", intermidiates, tkId)
			return
		case Finished:
			info(wid, "got finished while trying to deliver intermidiate files %v (task id %v)", intermidiates, tkId)
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

			info(wid, "proceeding to reduce %v (task id %v)", files, tkId)

			var nWorkers int

			nFiles := len(files)
			nCpus := runtime.NumCPU()
			jobs := make(chan string, nFiles)
			prereduction := make(chan []KeyValue, nFiles)
			kvs := []KeyValue{}

			for _, file := range files {
				jobs <- file
			}

			if nFiles < nCpus {
				nWorkers = nFiles
			} else {
				nWorkers = nCpus
			}

			for range nWorkers {
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
							die(wid, "cannot open %v\n", file)
							prereduction <- nil
							return
						}
						content, err := ioutil.ReadAll(f)
						if err != nil {
							die(wid, "cannot read %v\n", file)
							prereduction <- nil
							return
						}
						f.Close()

						lines := strings.Split(string(content), "\n")
						nLines := len(lines) - 1
						kvs := make([]KeyValue, nLines)
						for i, line := range lines[:nLines] {
							ls := strings.Split(line, " ")
							kvs[i] = KeyValue{Key: ls[0], Value: ls[1]}
						}
						sort.Sort(ByKey(kvs))

						prevKvs := []KeyValue{}
						nKvs := len(kvs)
						i := 0
						for i < nKvs {
							j := i + 1
							for j < nKvs && kvs[j].Key == kvs[i].Key {
								j++
							}

							values := []string{}
							for k := i; k < j; k++ {
								values = append(values, kvs[k].Value)
							}
							output := reducef(kvs[i].Key, values)

							kv := KeyValue{Key: kvs[i].Key, Value: output}
							prevKvs = append(prevKvs, kv)

							i = j
						}
						prereduction <- prevKvs
					}
				}(jobs, prereduction)
			}

			for range nFiles {
				kvs = append(kvs, <-prereduction...)
			}
			close(jobs)

			filename := fmt.Sprintf("mr-out-%d", wid)
			tmpFilename := fmt.Sprintf("%v-*", filename)
			f, err := ioutil.TempFile("", tmpFilename)
			if err != nil {
				die(wid, "cannot open tmp file %v\n", tmpFilename)
			}

			sort.Sort(ByKey(kvs))
			nKvs := len(kvs)
			i := 0
			for i < nKvs {
				j := i + 1
				for j < nKvs && kvs[j].Key == kvs[i].Key {
					j++
				}

				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				output := reducef(kvs[i].Key, values)

				if _, err := fmt.Fprintf(f, "%v %v\n", kvs[i].Key, output); err != nil {
					die(wid, "cannot write to %v\n", f.Name())
				}

				i = j
			}

			if err = os.Rename(f.Name(), filename); err != nil {
				die(wid, "cannot rename %v to %v\n", f.Name(), filename)
			}
			f.Close()
		case InvalidWorkerId:
			info(wid, "got invalid worker id while trying to request a reduction")
			return
		case Finished:
			info(wid, "got finished while trying to request a reduction")
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
			info(wid, "signaled completed reduction (task id %v) and the program as finished", tkId)
		case Accepted:
			info(wid, "signaled completed reduction (task id %v)", tkId)
			goto reduction
		case InvalidTaskId:
			info(wid, "reduction of %v (old task id %v) was already reassigned", files, tkId)
			goto reduction
		case InvalidWorkerId:
			info(wid, "got invalid worker id while trying to signaling completed reduction (task id %v)", intermidiates, tkId)
		case Finished:
			info(wid, "got finished while trying to signaling completed reduction (task id %v)", intermidiates, tkId)
		}
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) {
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
