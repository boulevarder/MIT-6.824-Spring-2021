package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"



// Add your RPC definitions here.
type argsType int
const (
	requestArgs		argsType = 1
	doneMapArgs		argsType = 2
	doneReduceArgs	argsType = 3
)

type alignTaskType int
const (
	noTask			alignTaskType = 1
	mapTask			alignTaskType = 2
	reduceTask		alignTaskType = 3
)

// Add your RPC definitions here.
type MapReduceArgs struct {
	ArgsType		argsType
	FileName			string
	ReduceResFileName 	string
	HashTable		map[int] string
}

type MapReduceReply struct {
	AlignTaskType	alignTaskType
	IndexNum		int
	FileName		string
	FileList		[]string
	hash			int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
