package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	cond *sync.Cond

	mapFiles     []string
	nMapTasks    int
	nReduceTasks int

	mapTasksFinished []bool
	mapTasksIssued   []time.Time

	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time

	isDone bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	for {
		mapDone := true
		for m, done := range c.mapTasksFinished {
			if !done {
				if c.mapTasksIssued[m].IsZero() || time.Since(c.mapTasksIssued[m]).Seconds() > 10 {
					reply.MapFile = c.mapFiles[m]
					reply.TaskNum = m
					reply.TaskType = MapTask
					c.mapTasksIssued[m] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}
		if !mapDone {
			c.cond.Wait()
		} else {
			break
		}
	}
	for {
		redDone := true
		for m, done := range c.reduceTasksFinished {
			if !done {
				if c.reduceTasksIssued[m].IsZero() || time.Since(c.reduceTasksIssued[m]).Seconds() > 10 {
					reply.TaskNum = m
					reply.TaskType = ReduceTask
					c.reduceTasksIssued[m] = time.Now()
					return nil
				} else {
					redDone = false
				}
			}
		}
		if !redDone {
			c.cond.Wait()
		} else {
			break
		}
	}

	reply.TaskType = CompleteTask
	c.isDone = true
	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MapTask:
		c.mapTasksFinished[args.TaskNum] = true
	case ReduceTask:
		c.reduceTasksFinished[args.TaskNum] = true
	default:
		panic(fmt.Sprintf("unexpected TaskType:%v\n", args.TaskType))
	}
	c.cond.Broadcast()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{}

	c.cond = sync.NewCond(&c.mu)

	c.mapFiles = files
	c.nMapTasks = len(files)
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))

	c.nReduceTasks = nReduce
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)

	go func() {
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
