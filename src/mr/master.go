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
type Master struct {
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

// Your code here -- RPC handlers for the worker to call.

func (m *Master) NoTaskResponse(reply *MapReduceReply) {
	reply.AlignTaskType = noTask
}

func (m *Master) MapTaskResponse(reply *MapReduceReply) {
	var file_name string
	var file_index int
	var file_exist bool = false
	m.mutex.Lock()
	for name, done := range m.mapFileState {
		if done == NotStart {
			m.mapFileState[name] = Doing
			m.fileStartTime[name] = time.Now()
			file_name = name
			file_index = m.fileIndex
			m.fileIndex++
			file_exist = true
			break
		}
	}
	m.mutex.Unlock()
	if file_exist == true {
		reply.AlignTaskType = mapTask
		reply.IndexNum = file_index
		reply.FileName = file_name
	} else {
		m.NoTaskResponse(reply)
	}
}

func (m *Master) ReduceTaskResponse(reply *MapReduceReply) {
	var hash string
	var file_index int
	var file_exist bool = false
	
	m.mutex.Lock()
	for name, done := range m.hashTable {
		if done == NotStart {
			m.hashTable[name] = Doing
			m.fileStartTime[name] = time.Now()
			hash = name
			file_index = m.fileIndex
			m.fileIndex++
			file_exist = true
			break
		}
	}
	m.mutex.Unlock()

	if file_exist == true {
		reply.AlignTaskType = reduceTask
		reply.IndexNum = file_index
		reply.FileName = hash
		reply.FileList = m.mapResult[hash]
	} else {
		m.NoTaskResponse(reply)
	}
}

func (m *Master) NoTaskCallback(args *MapReduceArgs, reply *MapReduceReply)  {
	var phase masterPhase
	m.mutex.Lock()
	phase = m.phase
	m.mutex.Unlock()

	switch(phase){
		case MapPhase:
			m.MapTaskResponse(reply)
		case ReducePhase:
			m.ReduceTaskResponse(reply)
		default:
			m.NoTaskResponse(reply)
	}
}

func (m *Master) MapDoneCallback(args *MapReduceArgs, reply *MapReduceReply) {
	var phase masterPhase = MapPhase
	m.mutex.Lock()
	switch(m.mapFileState[args.FileName]) {
		case NotStart:
			fallthrough
		case Doing:
			m.mapFileState[args.FileName] = Done
			m.numUnDone--
			if m.numUnDone == 0 {
				m.phase = ReducePhase
				m.numUnDone = len(m.hashTable)
			}
			for hash, oFileName := range args.HashTable {
				hashStr := strconv.Itoa(hash)
				m.hashTable[hashStr] = NotStart
				m.mapResult[hashStr] = append(m.mapResult[hashStr], oFileName) 
			}
	}	
	phase = m.phase
	m.mutex.Unlock()
	
	switch(phase) {
		case MapPhase:
			m.MapTaskResponse(reply)
		case ReducePhase:
			m.ReduceTaskResponse(reply)
		case FinishPhase:
			m.NoTaskResponse(reply)
	}
}

func (m *Master) ReduceDoneCallback(args *MapReduceArgs, reply *MapReduceReply) {
	var phase masterPhase
	m.mutex.Lock()
	switch(m.hashTable[args.FileName]) {
		case NotStart:
			fallthrough
		case Doing:
			m.hashTable[args.FileName] = Done
			m.numUnDone--
			if m.numUnDone == 0 {
				m.phase = FinishPhase
			}
			m.reduceResult = append(m.reduceResult, args.ReduceResFileName)
	}
	phase = m.phase
	m.mutex.Unlock()
	
	switch(phase) {
		case ReducePhase:
			m.ReduceTaskResponse(reply)
		case FinishPhase:
			m.NoTaskResponse(reply)
		case MapPhase:
			fmt.Println("\033[35merror: MapPhase can't exist in ReduceDoneCallback\033[0m")
	}

}

func (m *Master) MapReduce(args *MapReduceArgs, reply *MapReduceReply) error {
	switch(args.ArgsType){
		case requestArgs:
			m.NoTaskCallback(args, reply)
		case doneMapArgs:
			m.MapDoneCallback(args, reply)
		case doneReduceArgs:
			m.ReduceDoneCallback(args, reply)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


func (m *Master) ExamMapTime() {
	for filename, done := range m.mapFileState {
		if done == Doing {
			eclapsed := time.Now().Unix() - m.fileStartTime[filename].Unix()
			if eclapsed > 10 {
				m.mapFileState[filename] = NotStart
			}
		}
	}
}

func (m *Master) ExamReduceTime() {
	for filename, done := range m.hashTable {
		if done == Doing {
			eclapsed := time.Now().Unix() - m.fileStartTime[filename].Unix()
			if eclapsed > 10 {
				fmt.Println("\033[35m", filename, ": overtimed!!!", "\033[0m")
				m.hashTable[filename] = NotStart
			}
		}
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mutex.Lock()
	switch(m.phase) {
		case FinishPhase:
			ret = true
		case MapPhase:
			m.ExamMapTime()
		case ReducePhase:
			m.ExamReduceTime()
	}
	m.mutex.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.phase = MapPhase

	m.mapFileState = make(map[string] FileState)
	m.fileIndex = 0
	
	m.fileStartTime = make(map[string] time.Time)
	m.numUnDone = len(files)

	m.mapResult = make(map[string] []string)
	m.hashTable = make(map[string] FileState)

	for _, file_name := range files {
		m.mapFileState[file_name] = NotStart
	}

	m.server()
	return &m
}
