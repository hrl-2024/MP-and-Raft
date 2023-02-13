package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerStruct struct {
	mapf                func(string, string) []KeyValue
	reducef             func(string, []string) string
	id                  uint64
	intermediate_result []KeyValue
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := WorkerStruct{}

	w.mapf = mapf
	w.reducef = reducef

	w.id = rand.Uint64()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	w.RequestJob()
}

func (w *WorkerStruct) RequestJob() {
	args := JobRequestArgs{}

	args.WorkerId = w.id

	reply := JobRequestReply{}

	for call("Coordinator.JobRequestRPC", &args, &reply) {

		if reply.JobType == "map" {
			// Read the file
			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()

			// Store the intermediate result
			intermediate_result := w.mapf(reply.File, string(content))

			// Create the file for the intermediate results
			for i := 0; i < reply.NReduce; i++ {
				filename := fmt.Sprintf("mr-%d-%d.json", w.id, i)
				os.Create(filename)
			}

			// writing the intermediate result
			for _, value := range intermediate_result {

				buket_id := ihash(value.Key) % reply.NReduce
				filename := fmt.Sprintf("mr-%d-%d.json", w.id, buket_id)
				// cannot use os.Open simply because 'bad file descriptor' issue
				file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				enc := json.NewEncoder(file)

				err = enc.Encode(&value)
				if err != nil {
					log.Fatalf("JSON.ENCODER: cannot write %v\n%v", value, err)
				}

				file.Close()
			}

			// inform the coordinator that this map has done
			call("Coordinator.MapJobCompleteRPC", &args, &reply)

		} else if reply.JobType == "reduce" {
			// reduce
			fmt.Printf("Worker: %v received reduce job %v.\n\n", args.WorkerId, reply.BucketId)

			// grabbing all relevant intermediate results
			filenamepattern := fmt.Sprintf("../main/mr-*-%d.json", reply.BucketId)
			matches, _ := filepath.Glob(filenamepattern)

			intermediate := []KeyValue{}

			for _, filename := range matches {
				file, err_open := os.Open(filename)
				if err_open != nil {
					log.Fatalf("Reduce: cannot open %v", filename)
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
			}

			// reduce from main.mrsequential
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.BucketId)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
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
				output := w.reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			// inform the coordinator that this reduce has done
			call("Coordinator.ReduceJobCompleteRPC", &args, &reply)

		} else if reply.JobType == "done" {
			// exit
			break
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}
