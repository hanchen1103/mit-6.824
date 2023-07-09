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

const (
	GetTaskRPCName    = "Coordinator.GetTask"
	ReportTaskRPCName = "Coordinator.ReportTask"
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type AskTaskArgs struct {
	WorkerId int64
}

type AskTaskReply struct {
	Task *Task
}

type ReportTaskArgs struct {
	TaskId   int
	WorkerId int64
	Done     bool
}

type ReportTaskReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
