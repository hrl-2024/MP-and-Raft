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

type Coordinator struct {
	// Your definitions here.
	files          []string
	nReduce        int
	mapTaskPending int
	reducePending  []int
	reduceComplete int
	workerjob      map[uint64]string
	liveWorker     map[uint64]int // key : value = workerid : 1(for done) 0 (for unfinished)
	Lock           sync.Mutex
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
		fmt.Println("Coordinator: Assigning map task")

		// lock the coordinator in case of race condition
		c.Lock.Lock()
		defer c.Lock.Unlock()

		reply.JobType = "map"
		reply.NReduce = c.nReduce

		// assign the file
		reply.File = c.files[0]
		// remove the file from c.files
		fmt.Printf("Coordinator: Removing files from pending tasks: len(c.files) = %d\n%v\n", len(c.files), c.files)
		c.files = c.files[1:len(c.files)]
		fmt.Printf("Coordinator: After removed. len(c.files) = %d\n%v\n", len(c.files), c.files)

		// keep a record of the worker's job (in case of straggler)
		c.workerjob[args.WorkerId] = reply.File
		fmt.Printf("Coordinator: worker id %d map to job on file %v", args.WorkerId, c.workerjob[args.WorkerId])

	} else if len(c.files) == 0 && c.mapTaskPending == 0 && len(c.reducePending) != 0 {
		fmt.Println("Coordinator: Assigning reduce task")

		// lock the coordinator in case of race condition
		c.Lock.Lock()
		defer c.Lock.Unlock()

		reply.JobType = "reduce"
		reply.BucketId = c.reducePending[0]

		// remove from the pending reduce task
		c.reducePending = c.reducePending[1:len(c.reducePending)]
		reply.NReduce = c.nReduce
	} else if len(c.reducePending) == 0 && c.reduceComplete == c.nReduce {
		reply.JobType = "done"
	}

	go c.WaitForWorker(args.WorkerId, c.workerjob[args.WorkerId])

	return nil
}

func (c *Coordinator) WaitForWorker(workerid uint64, file string) error {
	fmt.Printf("Coordinator is waiting for worker %v on job %v \n", workerid, file)

	c.liveWorker[workerid] = 0

	time.Sleep(time.Second * 10)

	c.Lock.Lock()
	defer c.Lock.Unlock()

	if c.liveWorker[workerid] == 0 && c.workerjob[workerid] == file {
		fmt.Printf("Coordinator: %v is not finished. Putting back in queue.\n", workerid)
		c.files = append(c.files, c.workerjob[workerid])
	}

	fmt.Printf("Coordinator: worker %v on job %v Done \n", workerid, file)

	return nil
}

// To inform the coordinator that the worker has finished its map job
func (c *Coordinator) MapJobCompleteRPC(args *JobRequestArgs, reply *JobRequestReply) error {
	fmt.Printf("Worker: %v finished job on %v.\n", args.WorkerId, c.workerjob[args.WorkerId])

	c.mapTaskPending--
	c.liveWorker[args.WorkerId] = 1

	return nil
}

// To inform the coordinator that the worker has finished its reduce job
func (c *Coordinator) ReduceJobCompleteRPC(args *JobRequestArgs, reply *JobRequestReply) error {
	c.reduceComplete++

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
	// Your code here.
	return c.reduceComplete == c.nReduce
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

	c.reduceComplete = 0

	c.workerjob = make(map[uint64]string)
	c.liveWorker = make(map[uint64]int)

	c.server()
	return &c
}
