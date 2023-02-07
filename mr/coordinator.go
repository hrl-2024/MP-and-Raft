package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	files          []string
	nReduce        int
	mapTaskPending int
	reducePending  []int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) JobRequestRPC(args *JobRequestArgs, reply *JobRequestReply) error {
	// take care of the worker
	// check current state
	// asign a job
	if len(c.files) > 0 {
		reply.JobType = "map"
		reply.NReduce = c.nReduce

		// assign the file
		reply.File = c.files[0]
		// remove the file from c.files
		fmt.Printf("Coordinator: Removing files from pending tasks: len(c.files) = %d\n%v\n", len(c.files), c.files)
		c.files = c.files[1:len(c.files)]
		fmt.Printf("Coordinator: After removed. len(c.files) = %d\n%v\n", len(c.files), c.files)

	} else if len(c.files) == 0 && c.mapTaskPending == 0 {
		reply.JobType = "reduce"
		reply.BucketId = c.reducePending[0]

		// remove from the pending reduce task
		c.reducePending = c.reducePending[1:len(c.reducePending)]
		reply.NReduce = c.nReduce
	}

	return nil
}

// To inform the coordinator that the worker has finished its job
func (c *Coordinator) MapJobCompleteRPC(args *JobRequestArgs, reply *JobRequestReply) error {
	c.mapTaskPending--

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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.mapTaskPending = len(files)

	// will also need to know:
	// are we in map phrase or reduce phrase
	// keeping track of how many are completed (remove from files)
	// + the pending map task
	c.reducePending = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reducePending[i] = i
	}

	c.server()
	return &c
}
