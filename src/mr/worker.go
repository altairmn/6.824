package mr

import "fmt"
import "log"
import "os"
import "net/rpc"
import "hash/fnv"
import "sort"
import "io/ioutil"
import "encoding/json"
import "time"
import "path/filepath"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func ExecMap(mapf func(string, string) []KeyValue, task Task) {
	// fmt.Printf("Exec %v\n", task)
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))

	encoders := make([]*json.Encoder, task.NumReduce)
	for i := 0; i < task.NumReduce; i++ {
		fname := fmt.Sprintf("mr-%v-%v", task.TaskId, i)
		// fmt.Printf("Opening file for printing %v\n", fname)
		file, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fmt.Printf("File %v failed\n", fname)
			log.Fatalf("cannot open file %v", fname)
		}
		defer file.Close()
		encoders[i] = json.NewEncoder(file)
	}
	// hash the words and put into buckets
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NumReduce
		err := encoders[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("error running encoder")
		}
	}
}

func ExecReduce(reducef func(string, []string) string, task Task) {
	// fmt.Printf("Exec %v\n", task)
	// read the files and output
	// get all the matching filenames to the task list
	// and create a decoder to read all those files one by one
	fpattern := fmt.Sprintf("mr-*-%v", task.TaskId)
	filenames, err := filepath.Glob(fpattern)

	intermediate := []KeyValue{}
	for _, fname := range filenames {
		file, err := os.Open(fname)
		if err != nil {
			fmt.Printf("File %v failed\n", fname)
			log.Fatalf("cannot open file %v", fname)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
		}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.TaskId)
	ofile, err := os.OpenFile(oname, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Printf("File %v failed\n", oname)
		log.Fatalf("cannot open file %v", oname)
	}
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	var task Task
	for {
		task = CallGetTask()
		// fmt.Printf("Received task %v\n", task)
		if task.TaskId == NO_TASK {
			time.Sleep(time.Second)
		} else if task.TaskId == FINISHED {
			os.Exit(0)
		} else if task.TaskType == "Map" {
			ExecMap(mapf, task)
			CallLogTask(&task)
		} else if task.TaskType == "Reduce" {
			ExecReduce(reducef, task)
			CallLogTask(&task)
		}
	}
}

func CallGetTask() Task {
	req := TaskRequest{}
	task := Task{}
	call("Coordinator.GetTask", &req, &task);
	return task
}

func CallLogTask(task *Task) {
	resp := LogResp{}
	call("Coordinator.LogTask", &task, &resp)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
