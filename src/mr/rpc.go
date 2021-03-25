package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type CallForWorkArgs struct {
}

type WorkType int32

const (
	MAPWORK    WorkType = 0
	REDUCEWORK WorkType = 1
)

type WorkContent struct {
	Index         int32
	Filename      string
	NumMapWork    int32
	NumReduceWork int32
}

type CallForWorkReply struct {
	HasWork  bool
	WorkType WorkType
	Content  WorkContent
}

type CallWorkDoneArgs struct {
	WorkType WorkType
	Content  WorkContent
}

type CallWorkDoneReply struct {
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
