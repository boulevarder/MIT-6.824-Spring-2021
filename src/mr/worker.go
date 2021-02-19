package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "io"
import "os"
import "strconv"
import "time"
import "strings"
import "sort"

const NReduce = 10
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

type workerPhase int
const (
	startPhase		workerPhase = 0
	doMapPhase		workerPhase = 1
	doReducePhase	workerPhase = 2
	noTaskPhase		workerPhase = 3
)

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var reply MapReduceReply
	var tryTimes int = 0
	var task workerPhase = startPhase
	for {
		switch task {
			case startPhase:
				reply = CallWithoutTask()
			case doMapPhase:
				reply = CallResponseMap(mapf, reply.FileName, reply.IndexNum)
			case doReducePhase:
				reply = CallResponseReduce(reducef, reply.FileList, reply.FileName, reply.IndexNum)
		}
		switch reply.AlignTaskType {
			case noTask:
				fmt.Println("\033[37mtask state: noTask, sleep\033[0m")
				tryTimes++
				time.Sleep(time.Second)
				task = startPhase
			case mapTask:
				tryTimes = 0
				task = doMapPhase
			case reduceTask:
				tryTimes = 0
				task = doReducePhase
			default:
				fmt.Println("\033[35merror, AlignTaskType is what????\033[0m")
		}
		time.Sleep(time.Second)
		if tryTimes >= 5{
			break
		}
	}
}


func CallResponseMap(mapf func(string, string) []KeyValue,
		filename string, fileIndex int) MapReduceReply {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	args := MapReduceArgs{}
	args.ArgsType = doneMapArgs
	args.FileName = filename
	args.HashTable = make(map[int]string)
	var fileNameMapContent = make(map[string]string)
	for _, elem := range kva {
		oFilename := strconv.Itoa(fileIndex)
		oFilename += "-"
		hashV := ihash(elem.Key) %  NReduce
		oFilename += strconv.Itoa(hashV)

		content := elem.Key + " " + elem.Value + "\n"
		fileNameMapContent[oFilename] += content
		args.HashTable[hashV] = oFilename
	}
	for filename, content := range fileNameMapContent {
		f, err := os.Create(filename)
		if err != nil {
			fmt.Println("\033[35mcreate failed:", filename, "\033[0m")
		}
		defer f.Close()
		io.WriteString(f, content)
	}
	reply := MapReduceReply{}

	if mapReduceCall("Coordinator.MapReduce", &args, &reply) == false {
		fmt.Println("\033[35mCallResponseMap: failed\033[0m")
	}
	return reply
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func CallResponseReduce(reducef func(string, []string) string,
			fileList []string, srcHash string, fileIndex int) MapReduceReply {
	intermediate := []KeyValue {}
	for _, filename := range fileList {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		words := strings.Fields(string(content))
		for i:=0; i < len(words); i+=2 {
			intermediate = append(intermediate, KeyValue{words[i], words[i+1]})
		}
		// os.Remove(filename)
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-"+strconv.Itoa(fileIndex)
	ofile, _ := os.Create(oname)

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
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()	

	args := MapReduceArgs{}
	args.ArgsType = doneReduceArgs
	args.FileName = srcHash
	args.ReduceResFileName = oname
	reply := MapReduceReply{}

	if mapReduceCall("Coordinator.MapReduce", &args, &reply) == false {
		fmt.Println("\033[35mCallResponseReduce: failed\033[0m")	
	}
	return reply
}

func CallWithoutTask() MapReduceReply {
	args := MapReduceArgs{}
	args.ArgsType = requestArgs

	reply := MapReduceReply{}

	if mapReduceCall("Coordinator.MapReduce", &args, &reply) == false {
		fmt.Println("\033[35mCallWithoutTask: failed\033[0m")
	}

	return reply
}


func mapReduceCall(rpcname string, args *MapReduceArgs,
		reply *MapReduceReply) bool {

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
	return false
}