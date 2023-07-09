package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

type TaskType int

const (
	UnStarted TaskStatus = iota
	InProgress
	Abnormal
	Timeout
	Completed
)

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	MonitorInterval = time.Millisecond * 300
)

type Task struct {
	TaskId    int
	Status    TaskStatus
	Type      TaskType
	File      string
	NReduce   int
	NMap      int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	Files             []string
	NReduce           int
	Tasks             map[int]Task
	Workers           map[int]int64
	RemainMapsTasks   int
	RemainReduceTasks int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.RemainMapsTasks == 0 && c.RemainReduceTasks == 0 {
		return errors.New("no more tasks")
	}

	if c.RemainMapsTasks == 0 {
		for tid, task := range c.Tasks {
			if task.Type == ReduceTask && task.Status != Completed && task.Status != InProgress {
				task.Status = InProgress
				task.StartTime = time.Now()
				c.Tasks[tid] = task
				c.Workers[tid] = args.WorkerId
				reply.Task = &task
				//log.Printf("assign task :%+v to worker:%+v, task type:%+v", task.TaskId, args.WorkerId, task.Type)
				return nil
			}
		}
	}

	for tid, task := range c.Tasks {
		if task.Type == MapTask && task.Status != Completed && task.Status != InProgress {
			task.Status = InProgress
			task.StartTime = time.Now()
			c.Tasks[tid] = task
			c.Workers[tid] = args.WorkerId
			reply.Task = &task
			//log.Printf("assign task :%+v to worker:%+v, task type:%+v", task.TaskId, args.WorkerId, task.Type)
			return nil
		}
	}

	return errors.New("no more tasks")
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.WorkerId != c.Workers[args.TaskId] {
		return errors.New("current task has been assigned to another worker")
	}

	task := c.Tasks[args.TaskId]

	if args.Done {
		task.Status = Completed
		if task.Type == MapTask {
			c.RemainMapsTasks -= 1
		} else if task.Type == ReduceTask {
			c.RemainReduceTasks -= 1
		}
	} else {
		task.Status = Abnormal
	}
	c.Tasks[args.TaskId] = task
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if there are any ongoing map tasks
	return c.RemainReduceTasks == 0 && c.RemainMapsTasks == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:             files,
		NReduce:           nReduce,
		Tasks:             make(map[int]Task, 0),
		Workers:           make(map[int]int64),
		RemainMapsTasks:   len(files),
		RemainReduceTasks: -1,
	}

	// Your code here.
	for i, file := range files {
		mapTask := Task{
			TaskId:  i,
			Status:  UnStarted,
			Type:    MapTask,
			File:    file,
			NReduce: nReduce,
			NMap:    len(files),
		}
		c.Tasks[i] = mapTask
	}

	go c.MonitorStatus()
	c.server()
	return &c
}

func (c *Coordinator) MonitorStatus() {
	ticker := time.NewTicker(MonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.CheckStatus()
		}
	}
}

func (c *Coordinator) CheckStatus() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.RemainMapsTasks == 0 && c.RemainReduceTasks == -1 {
		c.RemainReduceTasks = c.NReduce
		reduceTasks := make(map[int]Task)
		for i := 0; i < c.NReduce; i++ {
			reduceTasks[i] = Task{
				TaskId:  i,
				Status:  UnStarted,
				Type:    ReduceTask,
				NReduce: c.NReduce,
				NMap:    len(c.Files),
			}
		}
		c.Tasks = reduceTasks
		c.Workers = make(map[int]int64)
	}

	for id, task := range c.Tasks {
		if task.Status == InProgress && time.Now().Sub(task.StartTime) > 10*time.Second {
			t := task
			t.Status = Timeout
			c.Tasks[id] = t
		}
	}
}
