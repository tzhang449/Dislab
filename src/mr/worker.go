package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	for true {
		reply, err := CallForWork()
		if err {
			log.Fatal("call for work error")
		} else if reply.HasWork {

			if reply.WorkType == MAPWORK {
				{
					err := DoMapWork(mapf, reply.Content)
					if err != nil {
						log.Fatal("do map work error: ", err)
					}
				}

				{
					_, err = CallWorkDone(reply)
					if err {
						log.Fatal("work done reply error")
					}
				}

			} else if reply.WorkType == REDUCEWORK {
				{
					err := DoReduceWork(reducef, reply.Content)
					if err != nil {
						log.Fatal("do reduce work error: ", err)
					}
				}

				{
					_, err = CallWorkDone(reply)
					if err {
						log.Fatal("work done reply error")
					}
				}
				//DoReduceWork(reducef, reply.content)

			} else {
				log.Fatalf("work type error(%v)", reply.WorkType)
			}

		} else {
			time.Sleep(1 * time.Second)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

/*
	ask the master for a work
	either map work or reduce work will be ok
*/
func CallForWork() (CallForWorkReply, bool) {
	args := CallForWorkArgs{}
	reply := CallForWorkReply{}

	err := !call("Master.GetWork", &args, &reply)
	return reply, err
}

/*
	call to tell master the work is done
*/
func CallWorkDone(workInfo CallForWorkReply) (CallWorkDoneReply, bool) {
	args := CallWorkDoneArgs{
		WorkType: workInfo.WorkType,
		Content:  workInfo.Content,
	}

	reply := CallWorkDoneReply{}

	err := !call("Master.WorkDone", &args, &reply)
	return reply, err
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

func DoMapWork(mapf func(string, string) []KeyValue, workContent WorkContent) error {
	//load file and call mapf
	filename := workContent.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("DoMapWork() cannot open %v (%v)", filename, err)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("DoMapWork() cannot read %v (%v)", filename, err)
		return err
	}
	file.Close()
	kva := mapf(filename, string(content))

	//split kv by hash values and save them to intermediate files
	intermediate := [][]KeyValue{}

	for i := int32(0); i < workContent.NumReduceWork; i++ {
		intermediate = append(intermediate, []KeyValue{})
	}
	for i := int32(0); i < int32(len(kva)); i++ {
		id := ihash(kva[i].Key) % int(workContent.NumReduceWork)
		intermediate[id] = append(intermediate[id], kva[i])
	}
	for i := int32(0); i < workContent.NumReduceWork; i++ {
		tmpFile, err := ioutil.TempFile(".", "tmp-")
		if err != nil {
			log.Print("DoMapWork() cannot create temporary file", err)
			return err
		}

		// clean up the file afterwards
		//defer os.Remove(tmpFile.Name())
		//fmt.Println("created File: " + tmpFile.Name())

		// writing to the file
		enc := json.NewEncoder(tmpFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Print("DoMapWork cannot write to tmp file", err)
				return err
			}
		}
		err = os.Rename(tmpFile.Name(), "mr-"+strconv.Itoa(int(workContent.Index))+"-"+strconv.Itoa(int(i)))
		if err != nil {
			log.Print("DoMapWork os rename error", err)
			return err
		}
	}
	return nil
}

func DoReduceWork(reducef func(string, []string) string, workContent WorkContent) error {
	//load intermediate results, file naming from mr-0-rNum to mr-nMap-rNum
	intermediate := []KeyValue{}
	for i := int32(0); i < workContent.NumMapWork; i++ {
		filename := "mr-" + strconv.Itoa(int(i)) + "-" + strconv.Itoa(int(workContent.Index))
		file, err := os.Open(filename)
		if err != nil {
			log.Print("DoReduceWork() open map file error", err)
			return err
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(int(workContent.Index))
	ofile, err := os.Create(oname)
	if err != nil {
		log.Print("DoReduceWork() os create error:", err)
		return err
	}

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-rNum.

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return nil
}
