package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	id      uint32
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// set a seed for rand so it generates a different number everytime
	// otherwise, same number all the time (number generated based on device)
	rand.Seed(time.Now().UnixNano())

	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
		id:      rand.Uint32(),
	}

	w.RequestTask()
}

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) RequestTask() {
	for {
		args := JobRequestArgs{}
		reply := JobRequestReply{}
		reply_received := call("Coordinator.RequestTask", &args, &reply)

		if !reply_received {
			fmt.Println("Worker: Coordinator has finished assigning task and died. No more work. Exiting")
			time.Sleep(time.Second) // sleep for a second for the test
			break
		}

		if reply.Type == -2 {
			break
		} else if reply.Type == -1 {
			if err := w.DoMap(reply.Job); err != nil {
				continue
			}
		} else {
			if err := w.DoReduce(reply.Type); err != nil {
				continue
			}
		}

		// Rest for a second
		time.Sleep(time.Second)
	}
}

func (w WorkerSt) DoMap(reply MapTask) error {
	fmt.Printf("%v received map job %v.\n", w.id, reply.Filename)

	file, err := os.Open(reply.Filename)
	if err != nil {
		err_str := fmt.Sprintf("cannot open %v: %v\n", reply.Filename, err)
		return errors.New(err_str)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		err_str := fmt.Sprintf("cannot read %v", reply.Filename)
		return errors.New(err_str)
	}
	file.Close()

	intermediate_result := w.mapf(reply.Filename, string(content))

	// writing the intermediate result
	for _, value := range intermediate_result {

		buket_id := ihash(value.Key) % reply.NumReduce
		filename := fmt.Sprintf("mr-worker-in-%d-%d.json", w.id, buket_id)

		// OpenFile() will create a file if not exist
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			err_str := fmt.Sprintf("Map: cannot open %v", filename)
			return errors.New(err_str)
		}
		enc := json.NewEncoder(file)

		err = enc.Encode(&value)
		if err != nil {
			err_str := fmt.Sprintf("JSON.ENCODER: cannot write %v. %v", value, err)
			return errors.New(err_str)
		}

		file.Close()
	}

	fmt.Println("Map task for ", reply.Filename, " completed")

	// inform the coordinator that this map has done
	emptyReply := JobRequestReply{}
	call("Coordinator.MapTaskCompleted", &reply, &emptyReply)

	return nil
}

func (w WorkerSt) DoReduce(bucketID int) error {
	// reduce
	fmt.Printf("%v received reduce job %d.\n", w.id, bucketID)

	// grabbing all relevant intermediate results
	filenamepattern := fmt.Sprintf("mr-worker-in-*-%d.json", bucketID)
	matches, _ := filepath.Glob(filenamepattern)

	intermediate := []KeyValue{}

	for _, filename := range matches {
		file, err_open := os.Open(filename)
		if err_open != nil {
			err_str := fmt.Sprintf("Reduce: cannot open %v", filename)
			return errors.New(err_str)
		}

		// Read all json back and append back to intermediate
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

	// reduce from main.mrsequential with modification
	sort.Sort(ByKey(intermediate))

	fmt.Printf("%v sorted all intermediate from %v.\n", w.id, filenamepattern)

	oname := fmt.Sprintf("mr-out-%d", bucketID)

	ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err_str := fmt.Sprintf("reduce: cannot open/create the output file %v", ofile)
		return errors.New(err_str)
	}

	fmt.Printf("%v created output file %v.\n", w.id, oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-X-Y
	//

	fmt.Printf("%v started reduce for %v.\n", w.id, oname)

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

		// fmt.Printf("%v doing reduce on key %v.\n", w.id, intermediate[i].Key)

		output := w.reducef(intermediate[i].Key, values)

		// fmt.Printf("%v done reducing on key %v.\n", w.id, intermediate[i].Key)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	fmt.Printf("%v done reduce for %v.\n", w.id, oname)

	ofile.Close()

	// inform the coordinator that this reduce has done
	reply := JobRequestReply{
		Type: bucketID,
	}
	dummyReply := JobRequestReply{}

	call("Coordinator.ReduceTaskCompleted", &reply, &dummyReply)

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("call():", rpcname, "dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("call():", rpcname, err)
	return false
}
