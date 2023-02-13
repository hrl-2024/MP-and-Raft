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
		call("Coordinator.RequestTask", &args, &reply)

		if reply.Type == -2 {
			break
		} else if reply.Type == -1 {
			w.DoMap(reply.Job)
		} else {
			w.DoReduce(reply.Type)
		}
	}
}

func (w WorkerSt) DoMap(reply MapTask) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	intermediate_result := w.mapf(reply.Filename, string(content))

	// writing the intermediate result
	for _, value := range intermediate_result {

		buket_id := ihash(value.Key) % reply.NumReduce
		filename := fmt.Sprintf("mr-tmp/mr-in-%d-%d.json", w.id, buket_id)

		// OpenFile() will create a file if not exist
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Map: cannot open %v", filename)
		}
		enc := json.NewEncoder(file)

		err = enc.Encode(&value)
		if err != nil {
			log.Fatalf("JSON.ENCODER: cannot write %v\n%v", value, err)
		}

		file.Close()
	}

	fmt.Println("Map task for ", reply.Filename, " completed")

	// inform the coordinator that this map has done
	emptyReply := JobRequestReply{}
	call("Coordinator.MapTaskCompleted", &reply, &emptyReply)
}

func (w WorkerSt) DoReduce(bucketID int) {
	// reduce
	fmt.Printf("received reduce job %d.\n", bucketID)

	// grabbing all relevant intermediate results
	filenamepattern := fmt.Sprintf("../main/mr-tmp/mr-in-*-%d.json", bucketID)
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

	// reduce from main.mrsequential with modification
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-tmp/mr-out-%d-%d.json", w.id, bucketID)

	ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("reduce: cannot open/create the output file %v", ofile)
	}

	enc := json.NewEncoder(ofile)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-X-Y
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

		output_kva := KeyValue{
			Key:   intermediate[i].Key,
			Value: output,
		}

		err = enc.Encode(&output_kva)
		if err != nil {
			log.Fatalf("JSON.ENCODER: cannot write %v\n%v", output_kva, err)
		}

		// this is the correct format for each line of Reduce output.
		// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// inform the coordinator that this reduce has done
	reply := JobRequestReply{
		Type: bucketID,
	}
	dummyReply := JobRequestReply{}

	call("Coordinator.ReduceTaskCompleted", &reply, &dummyReply)
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
