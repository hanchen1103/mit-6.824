package mr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"

	"github.com/bwmarrin/snowflake"
	"golang.org/x/sync/errgroup"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerId   int64
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
}

type work interface {
	CreateFileName(taskId, partitionId int) string
	ExecuteTasks(task Task) error
}

type mapTask struct {
	mapFunc func(string, string) []KeyValue
}

type reduceTask struct {
	reduceFunc func(string, []string) string
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	node, _ := snowflake.NewNode(1)
	w := &worker{
		workerId:   node.Generate().Int64(),
		mapFunc:    mapf,
		reduceFunc: reducef,
	}
	w.handler()
}

func (w *worker) handler() {
	var reportErr error
	//log.Printf("start handler map_reduce work,worker_id:%+v", w.workerId)
	for {
		var wk work
		task, err := w.GetTask()
		if err != nil {
			//log.Println(err)
			continue
		}
		switch task.Type {
		case MapTask:
			wk = &mapTask{
				mapFunc: w.mapFunc,
			}
		case ReduceTask:
			wk = &reduceTask{
				reduceFunc: w.reduceFunc,
			}
		default:
			log.Printf("Unexpected task type: %v", task.Type)
			continue
		}
		err = wk.ExecuteTasks(*task)
		reportErr = w.ReportTask(task.TaskId, w.workerId, err == nil)
		if err != nil {
			log.Printf("Error executing task %v: %v", task.TaskId, err)
		}
		if reportErr != nil {
			log.Printf("Error reporting task status for task %v: %v", task.TaskId, reportErr)
		}
	}
}

func (w *worker) GetTask() (*Task, error) {
	args := &AskTaskArgs{w.workerId}
	reply := &AskTaskReply{}

	if ok := call(GetTaskRPCName, args, reply); !ok {
		return nil, errors.New("get new task err")
	}
	return reply.Task, nil
}

func (w *worker) ReportTask(taskId int, workerId int64, done bool) error {
	args := &ReportTaskArgs{
		TaskId:   taskId,
		WorkerId: workerId,
		Done:     done,
	}
	reply := &ReportTaskReply{}
	if ok := call(ReportTaskRPCName, args, reply); !ok {
		return errors.New("reply task err")
	}
	return nil
}

func (m *mapTask) CreateFileName(taskId, partitionId int) string {
	return fmt.Sprintf("mr-temp-%d-%d", taskId, partitionId)
}

func (r *reduceTask) CreateFileName(taskId, partitionId int) string {
	return fmt.Sprintf("mr-out-%d", taskId)
}

func (m *mapTask) ExecuteTasks(task Task) error {
	content, err := ioutil.ReadFile(task.File)
	if err != nil {
		return err
	}

	kvs := m.mapFunc(task.File, string(content))
	partitions := make(map[int][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		pid := ihash(kv.Key) % task.NReduce
		partitions[pid] = append(partitions[pid], kv)
	}

	var g errgroup.Group

	for partitionId, kvPairs := range partitions {
		pid, kps := partitionId, kvPairs
		g.Go(func() error {
			fileName := m.CreateFileName(task.TaskId, pid)
			file, err := os.Create(fileName)
			if err != nil {
				return err
			}
			defer file.Close()

			encoder := json.NewEncoder(file)
			for _, kv := range kps {
				if err := encoder.Encode(&kv); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

func (r *reduceTask) ExecuteTasks(task Task) error {
	g, ctx := errgroup.WithContext(context.Background())

	maps := make(map[string][]string)
	mapMutex := &sync.Mutex{}

	for idx := 0; idx < task.NMap; idx++ {
		i := idx
		g.Go(func() error {
			fileName := fmt.Sprintf("mr-temp-%d-%d", i, task.TaskId)
			return r.processFile(ctx, fileName, maps, mapMutex)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	outputFile, err := os.Create(r.CreateFileName(task.TaskId, 0))
	if err != nil {
		return err
	}

	defer func() {
		outputFile.Close()
		if err != nil {
			os.Remove(r.CreateFileName(task.TaskId, 0))
		}
	}()

	mapMutex.Lock()
	for key, values := range maps {
		result := r.reduceFunc(key, values)
		if _, err = fmt.Fprintf(outputFile, "%v %v\n", key, result); err != nil {
			mapMutex.Unlock()
			return err
		}
	}
	mapMutex.Unlock()
	return err
}

func (r *reduceTask) processFile(ctx context.Context, fileName string, maps map[string][]string, mapMutex *sync.Mutex) error {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		var kv KeyValue
		if err = decoder.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		mapMutex.Lock()
		maps[kv.Key] = append(maps[kv.Key], kv.Value)
		mapMutex.Unlock()
	}
	return nil
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

	//fmt.Println(err)
	return false
}
