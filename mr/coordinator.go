package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce      int             // Number of reduce tasks
	MapTasks       chan MapTask    // Channel for uncompleted map tasks
	NPendingMap    int             // indicates how many map are yet to be complete
	NPendingReduce int             // indicates how many reduce are yet to be complete
	CompletedTasks map[string]bool // Map to check if task is completed
	ReduceTasks    chan int        // Channel for uncompleted reduce tasks
	Lock           sync.Mutex      // Lock for contolling shared variables
}

// Starting coordinator logic
func (c *Coordinator) Start(files []string) {
	fmt.Println("Starting Coordinator, adding Map Tasks to channel")

	// Prepare initial MapTasks and add them to the queue
	for _, file := range files {
		mapTask := MapTask{
			Filename:  file,
			NumReduce: c.NumReduce,
		}

		fmt.Println("MapTask", mapTask, "added to channel")

		c.MapTasks <- mapTask
		c.CompletedTasks["map_"+mapTask.Filename] = false
	}

	// Prepare reduce task
	for i := 0; i < c.NumReduce; i++ {
		c.ReduceTasks <- i
		fmt.Println("ReduceTask", i, "added to channel")
		c.CompletedTasks["reduce_"+strconv.Itoa(i)] = false
	}

	c.server()
}

// RPC that worker calls when idle (worker requests a map task)
func (c *Coordinator) RequestTask(args *JobRequestArgs, reply *JobRequestReply) error {
	fmt.Println("Assigning Task")

	c.Lock.Lock()

	if len(c.MapTasks) != 0 || c.NPendingMap != 0 {
		c.Lock.Unlock()
		mapTask := <-c.MapTasks
		fmt.Println("Map task found:", mapTask.Filename)
		*reply = JobRequestReply{
			Type: -1,
			Job:  mapTask,
		}

		go c.WaitForMapWorker(mapTask)
	} else if len(c.ReduceTasks) != 0 || c.NPendingReduce != 0 {
		c.Lock.Unlock()
		reduceTask := <-c.ReduceTasks
		fmt.Println("Reduce task found:", reduceTask)
		*reply = JobRequestReply{
			Type: reduceTask,
		}

		go c.WaitForReduceWorker(reduceTask)
	} else {
		c.Lock.Unlock()
		fmt.Println("All done.")
		*reply = JobRequestReply{
			Type: -2,
		}
	}

	return nil
}

// Goroutine will wait 5 seconds and check if map task is completed or not
func (c *Coordinator) WaitForMapWorker(task MapTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["map_"+task.Filename] == false {
		fmt.Println("Timer expired, map task ", task.Filename, " is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}

// Goroutine will wait 5 seconds and check if map task is completed or not
func (c *Coordinator) WaitForReduceWorker(bucketID int) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["reduce_"+strconv.Itoa(bucketID)] == false {
		fmt.Println("Timer expired, reduce task ", bucketID, " is not finished. Putting back in queue.")
		c.ReduceTasks <- bucketID
	}
	c.Lock.Unlock()
}

// RPC for reporting a completion of a Map task
func (c *Coordinator) MapTaskCompleted(args *MapTask, reply *JobRequestReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["map_"+args.Filename] = true
	c.NPendingMap--

	fmt.Println("Task", args, "completed")

	return nil
}

// RPC for reporting a completion of a Map task
func (c *Coordinator) ReduceTaskCompleted(args *JobRequestReply, dummy *JobRequestReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["reduce_"+strconv.Itoa(args.Type)] = true
	c.NPendingReduce--

	fmt.Println("Reduce Task", args.Type, "completed")

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	// Your code here.
	if c.NPendingMap == 0 && c.NPendingReduce == 0 {
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:      nReduce,
		MapTasks:       make(chan MapTask, 100),
		ReduceTasks:    make(chan int, nReduce),
		CompletedTasks: make(map[string]bool),
		NPendingMap:    len(files),
		NPendingReduce: nReduce,
	}

	fmt.Println("Starting coordinator")

	c.Start(files)

	return &c
}
