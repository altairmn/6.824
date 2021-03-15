package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "fmt"
import "strconv"

// Add your RPC definitions here.

const NO_TASK = -1
const FINISHED = -2

type TaskRequest struct {
	Id int
}

type Task struct {
	TaskType string
	TaskId int
	Filename string
	NumReduce int
}

func (task Task) String() string {
	if (task.TaskType == "Map") {
		return fmt.Sprintf("Map: id: %v, filename: %v", task.TaskId, task.Filename)
	} else if (task.TaskType == "Reduce") {
		return fmt.Sprintf("Reduce: %v; Total: %v", task.TaskId, task.NumReduce)
	}
	return ""
}

type LogResp struct {
	Id int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
