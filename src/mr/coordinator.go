package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"
import "strconv"

type masterPhase int
const (
	MapPhase	masterPhase = 0
	ReducePhase masterPhase = 1
	FinishPhase masterPhase = 2
)

type FileState int
const (
	NotStart		FileState = 0
	Doing			FileState = 1
	Done			FileState = 2
)

type Coordinator struct {
	// Your definitions here.
	phase			masterPhase
	mutex			sync.Mutex

	mapFileState	map[string] FileState
	fileIndex		int

	// map
	fileStartTime	map[string] time.Time
	numUnDone		int

	mapResult		map[string] []string
	hashTable		map[string] FileState

	reduceResult	[]string
}

func (c *Coordinator) NoTaskResponse(reply *MapReduceReply) {
	reply.AlignTaskType = noTask
}

func (c *Coordinator) MapTaskResponse(reply *MapReduceReply) {
	var file_name string
	var file_index int
	var file_exist bool = false
	c.mutex.Lock()
	for name, done := range c.mapFileState {
		if done == NotStart {
			c.mapFileState[name] = Doing
			c.fileStartTime[name] = time.Now()
			file_name = name
			file_index = c.fileIndex
			c.fileIndex++
			file_exist = true
			break
		}
	}
	c.mutex.Unlock()
	if file_exist == true {
		reply.AlignTaskType = mapTask
		reply.IndexNum = file_index
		reply.FileName = file_name
	} else {
		c.NoTaskResponse(reply)
	}
}

func (c *Coordinator) ReduceTaskResponse(reply *MapReduceReply) {
	var hash string
	var file_index int
	var file_exist bool = false
	
	c.mutex.Lock()
	for name, done := range c.hashTable {
		if done == NotStart {
			c.hashTable[name] = Doing
			c.fileStartTime[name] = time.Now()
			hash = name
			file_index = c.fileIndex
			c.fileIndex++
			file_exist = true
			break
		}
	}
	c.mutex.Unlock()

	if file_exist == true {
		reply.AlignTaskType = reduceTask
		reply.IndexNum = file_index
		reply.FileName = hash
		reply.FileList = c.mapResult[hash]
	} else {
		c.NoTaskResponse(reply)
	}
}

func (c *Coordinator) NoTaskCallback(args *MapReduceArgs, reply *MapReduceReply)  {
	var phase masterPhase
	c.mutex.Lock()
	phase = c.phase
	c.mutex.Unlock()

	switch(phase){
		case MapPhase:
			c.MapTaskResponse(reply)
		case ReducePhase:
			c.ReduceTaskResponse(reply)
		default:
			c.NoTaskResponse(reply)
	}
}

func (c *Coordinator) MapDoneCallback(args *MapReduceArgs, reply *MapReduceReply) {
	var phase masterPhase = MapPhase
	c.mutex.Lock()
	switch(c.mapFileState[args.FileName]) {
		case NotStart:
			fallthrough
		case Doing:
			c.mapFileState[args.FileName] = Done
			c.numUnDone--
			if c.numUnDone == 0 {
				c.phase = ReducePhase
				c.numUnDone = len(c.hashTable)
			}
			for hash, oFileName := range args.HashTable {
				hashStr := strconv.Itoa(hash)
				c.hashTable[hashStr] = NotStart
				c.mapResult[hashStr] = append(c.mapResult[hashStr], oFileName) 
			}
	}	
	phase = c.phase
	c.mutex.Unlock()
	
	switch(phase) {
		case MapPhase:
			c.MapTaskResponse(reply)
		case ReducePhase:
			c.ReduceTaskResponse(reply)
		case FinishPhase:
			c.NoTaskResponse(reply)
	}
}

func (c *Coordinator) ReduceDoneCallback(args *MapReduceArgs, reply *MapReduceReply) {
	var phase masterPhase
	c.mutex.Lock()
	switch(c.hashTable[args.FileName]) {
		case NotStart:
			fallthrough
		case Doing:
			c.hashTable[args.FileName] = Done
			c.numUnDone--
			if c.numUnDone == 0 {
				c.phase = FinishPhase
			}
			c.reduceResult = append(c.reduceResult, args.ReduceResFileName)
	}
	phase = c.phase
	c.mutex.Unlock()
	
	switch(phase) {
		case ReducePhase:
			c.ReduceTaskResponse(reply)
		case FinishPhase:
			c.NoTaskResponse(reply)
		case MapPhase:
			fmt.Println("\033[35merror: MapPhase can't exist in ReduceDoneCallback\033[0m")
	}

}

func (c *Coordinator) MapReduce(args *MapReduceArgs, reply *MapReduceReply) error {
	switch(args.ArgsType){
		case requestArgs:
			c.NoTaskCallback(args, reply)
		case doneMapArgs:
			c.MapDoneCallback(args, reply)
		case doneReduceArgs:
			c.ReduceDoneCallback(args, reply)
	}

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

func (c *Coordinator) ExamMapTime() {
	for filename, done := range c.mapFileState {
		if done == Doing {
			eclapsed := time.Now().Unix() - c.fileStartTime[filename].Unix()
			if eclapsed > 10 {
				c.mapFileState[filename] = NotStart
			}
		}
	}
}

func (c *Coordinator) ExamReduceTime() {
	for filename, done := range c.hashTable {
		if done == Doing {
			eclapsed := time.Now().Unix() - c.fileStartTime[filename].Unix()
			if eclapsed > 10 {
				fmt.Println("\033[35m", filename, ": overtimed!!!", "\033[0m")
				c.hashTable[filename] = NotStart
			}
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	switch(c.phase) {
		case FinishPhase:
			ret = true
		case MapPhase:
			c.ExamMapTime()
		case ReducePhase:
			c.ExamReduceTime()
	}
	c.mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.phase = MapPhase

	c.mapFileState = make(map[string] FileState)
	c.fileIndex = 0
	
	c.fileStartTime = make(map[string] time.Time)
	c.numUnDone = len(files)

	c.mapResult = make(map[string] []string)
	c.hashTable = make(map[string] FileState)

	for _, file_name := range files {
		c.mapFileState[file_name] = NotStart
	}

	c.server()
	return &c
}
