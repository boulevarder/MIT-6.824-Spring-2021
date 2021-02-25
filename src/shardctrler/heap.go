package shardctrler

// import (
// 	"fmt"
// )

type HeapNode struct {
	gid		int
	ref		int
}

func swapHeapNode(heapArray []*HeapNode, index1 int, index2 int) {
	pt := heapArray[index1]
	heapArray[index1] = heapArray[index2]
	heapArray[index2] = pt
}

func compareNodeFirstGreater(node1 *HeapNode, node2 *HeapNode) bool {
	return node1.ref > node2.ref;
}

func buildSmallRootHeap(heapArray []*HeapNode) {
    for index := 1; index < len(heapArray); index++ {
		for index / 2 >= 0 && compareNodeFirstGreater(heapArray[index/2], heapArray[index]) {
            var pt *HeapNode
            pt = heapArray[index/2]
            heapArray[index/2] = heapArray[index]
            heapArray[index] = pt
            index /= 2
        }
    }
}

func adjustSmallRootHeap(heapArray []*HeapNode) {
	index := 0
	left_index := 1
	len_array := len(heapArray)
	for {
		if left_index >= len_array {
			break
		}
		small_index := left_index;
		if left_index + 1 < len_array && compareNodeFirstGreater(heapArray[left_index], heapArray[left_index+1]) {
			small_index = left_index + 1
		}
		if compareNodeFirstGreater(heapArray[small_index], heapArray[index]) {
			break
		}
		swapHeapNode(heapArray, index, small_index)

		index = small_index
		left_index = 2*index+1;
	}
}

func rootAddOneAndReturnGid(heapArray []*HeapNode) int {
	heapArray[0].ref++
	gid := heapArray[0].gid
	adjustSmallRootHeap(heapArray)
	return gid
}

// func deleteRoot(heapArray []*HeapNode) []*HeapNode {
// 	heapArray[0] = heapArray[len(heapArray)-1]
// 	heapArray = heapArray[:len(heapArray)-1]
// 	return heapArray
// }

// func main() {
//     array := make([]*HeapNode, 0)
//     for i := 10; i >= 0; i-- {
//         node := new(HeapNode)
//         node.gid = i 
//         node.ref = i 

//         array = append(array, node)
//     }   
//     buildSmallRootHeap(array)
//     for i := 0; i < 100; i++ {
//         rootAddOneAndReturnGid(array)
//     }   

//     for i := 0; i < 11; i++ {
//         fmt.Println(":", array[i])
//     }   
// }
