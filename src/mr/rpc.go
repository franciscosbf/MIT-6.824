package mr

//
// RPC definitions.
//

import (
	"os"
	"strconv"
)

type DeliveryResponse int

const (
	ToDo             DeliveryResponse = 1 << 0
	Accepted                          = 1 << 1
	InvalidTaskId                     = 1 << 2
	InvalidWorkerId                   = 1 << 3
	IntermidiateDone                  = 1 << 4
	Finished                          = 1 << 5
)

type Intermidiate struct {
	File string
	Rid  int
}

type GenWorkerArgs struct{}

type GenWorkerReply struct {
	Wid     int
	BatchSz int
}

type MappingRequestArgs struct {
	Wid        int
	Registered bool
}

type MappingRequestReply struct {
	File     string
	TkId     int64
	Response DeliveryResponse
	Id       int
}

type MappingDoneArgs struct {
	Intermidiates []Intermidiate
	Wid           int
	TkId          int64
}

type MappingDoneReply struct {
	Response DeliveryResponse
}

type ReductionRequestArgs struct {
	Wid int
}

type ReductionRequestReply struct {
	Files    []string
	TkId     int64
	Response DeliveryResponse
	Id       int
}

type ReductionDoneArgs struct {
	Wid  int
	TkId int64
}

type ReductionDoneReply struct {
	Response DeliveryResponse
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
