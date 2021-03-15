package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const PENDING = 0
const STARTED = 1
const DONE = 2

type Coordinator struct {
	files        []string
	mapStatus    map[string]int
	reduceStatus map[int]int
	mu           sync.Mutex
	numReduce    int
}

func (c *Coordinator) timeout(task *Task) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if task.TaskType == "Map" && c.mapStatus[task.Filename] != DONE {
		fmt.Printf("Task %v timedout, status: %v\n", task, c.mapStatus[task.Filename])
		c.mapStatus[task.Filename] = PENDING
	} else if task.TaskType == "Reduce" && c.reduceStatus[task.TaskId] != DONE {
		fmt.Printf("Task %v timedout, status: %v\n", task, c.reduceStatus[task.TaskId])
		c.reduceStatus[task.TaskId] = PENDING
	}
	c.mu.Unlock()
}

func (c *Coordinator) doneMap() bool {
	ret := true
	c.mu.Lock()
	for _, status := range c.mapStatus {
		if status != DONE {
			ret = false
			break
		}
	}
	c.mu.Unlock()
	return ret
}

func (c *Coordinator) doneReduce() bool {
	ret := true
	c.mu.Lock()
	for _, status := range c.reduceStatus {
		if status != DONE {
			ret = false
			break
		}
	}
	c.mu.Unlock()
	return ret
}

func (c *Coordinator) GetTask(req *TaskRequest, task *Task) error {
	// fmt.Println("Received request from Worker")
	task.TaskId = NO_TASK
	c.mu.Lock()
	for idx, filename := range c.files {
		if c.mapStatus[filename] == PENDING {
			task.TaskType = "Map"
			task.Filename = filename
			task.NumReduce = c.numReduce
			task.TaskId = idx
			c.mapStatus[filename] = STARTED
			go c.timeout(task)
			fmt.Printf("Sent: %v\n", task)
			break
		}
	}
	c.mu.Unlock()

	if c.doneMap() == true {
		c.mu.Lock()
		for rnum := 0; rnum <= c.numReduce; rnum++ {
			if c.reduceStatus[rnum] == PENDING {
				task.TaskType = "Reduce"
				task.NumReduce = c.numReduce
				task.TaskId = rnum
				c.reduceStatus[rnum] = STARTED
				go c.timeout(task)
				// fmt.Printf("Sent: %v\n", task)
				break
			}
		}
		c.mu.Unlock()
	}

	if c.doneMap() == true && c.doneReduce() == true {
		task.TaskId = FINISHED
	}

	return nil
}

// LogTask logs task
func (c *Coordinator) LogTask(task *Task, resp *LogResp) error {
	c.mu.Lock()
	if task.TaskType == "Map" {
		c.mapStatus[task.Filename] = DONE
	} else if task.TaskType == "Reduce" {
		c.reduceStatus[task.TaskId] = DONE
	}
	c.mu.Unlock()
	fmt.Printf("Logged task: %v\n", task)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	ret := c.doneMap() && c.doneReduce()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.

	c.files = files
	c.mapStatus = make(map[string]int)
	c.reduceStatus = make(map[int]int)
	c.numReduce = nReduce

	for _, fname := range files {
		c.mapStatus[fname] = PENDING
	}

	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = PENDING
	}

	c.server()
	return &c
}
