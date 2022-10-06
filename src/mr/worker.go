package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
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

func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, finalFile)
}
func getIntermediateFile(mapTaskN int, redTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, redTaskN)
}
func finalizeIntermediateFile(tmpFile string, mapTaskN int, redTaskN int) {
	finalFile := getIntermediateFile(mapTaskN, redTaskN)
	os.Rename(tmpFile, finalFile)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		err, response := GetTask()
		if err != nil {
			fmt.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}
		switch response.TaskType {
		case MapTask:
			doMapTask(mapf, response)
		case ReduceTask:
			doReduceTask(reducef, response)
		case CompleteTask:
			return
		default:
			panic(fmt.Sprintf("unexpected TaskType: %v", response.TaskType))
		}

		FinishTask(response.TaskNum, response.TaskType)
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func doReduceTask(reducef func(string, []string) string, response GetTaskReply) {
	kva := []KeyValue{}
	for i := 0; i < response.NMapTasks; i++ {
		fileName := getIntermediateFile(i, response.TaskNum)
		f, err := os.Open(fileName)
		if err != nil {
			panic(fmt.Sprintf("open file %v err \n", fileName))
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}
	sort.Sort(ByKey(kva))

	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		panic(fmt.Sprintf("can't open tmpfile \n"))
	}
	tmpFileName := tmpFile.Name()

	for i := 0; i < len(kva); {
		values := []string{}
		j := i + 1
		for ; j < len(kva) && kva[j].Key == kva[j-1].Key; j++ {
		}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	finalizeReduceFile(tmpFileName, response.TaskNum)
}

func doMapTask(mapf func(string, string) []KeyValue, response GetTaskReply) {
	file, err := os.Open(response.MapFile)
	if err != nil {
		log.Fatalf("cannot open %v", response.MapFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", response.MapFile)
	}
	file.Close()

	kva := mapf(response.MapFile, string(content))

	tmpFiles := []*os.File{}
	tmpFileNames := []string{}
	encoders := []*json.Encoder{}

	for i := 0; i < response.NReduceTasks; i++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			panic(fmt.Sprintf("can't open tmpFiles"))
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFileName := tmpFile.Name()
		tmpFileNames = append(tmpFileNames, tmpFileName)
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	for _, kv := range kva {
		encoders[ihash(kv.Key)%response.NReduceTasks].Encode(&kv)
	}
	for i := 0; i < response.NReduceTasks; i++ {
		tmpFiles[i].Close()
	}
	for i := 0; i < response.NReduceTasks; i++ {
		finalizeIntermediateFile(tmpFileNames[i], response.TaskNum, i)
	}
}

func GetTask() (error, GetTaskReply) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.HandleGetTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return fmt.Errorf("call error"), reply
	}
	//fmt.Printf("reply: %v\n", reply)
	return nil, reply
}

func FinishTask(taskNum int, taskType TaskType) {
	args := FinishedTaskArgs{
		TaskType: taskType,
		TaskNum:  taskNum,
	}
	reply := FinishedTaskReply{}
	ok := call("Coordinator.HandleFinishedTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

	fmt.Println(err)
	return false
}
