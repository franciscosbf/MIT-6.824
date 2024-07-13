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
	ToDo DeliveryResponse = iota
	Accepted
	InvalidTaskId
	IntermidiateDone
	Finished
)

type Intermidiate struct {
	File string
	Rid  int
}

type MapRequestArgs struct {
	Wid int
}

type MapRequestReply struct {
	File     string
	TkId     int64
	BatchSz  int
	Response DeliveryResponse
}

type MapDoneArgs struct {
	Files []Intermidiate
	Wid   int
	TkId  int64
}

type MapDoneReply struct {
	Response DeliveryResponse
}

type ReductionRequestArgs struct {
	Wid int
}

type ReductionRequestReply struct {
	Files    []string
	TkId     int64
	Response DeliveryResponse
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
