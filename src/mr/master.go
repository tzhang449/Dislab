package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MRStateType int32

const (
	MAP     MRStateType = 0
	REDUCE  MRStateType = 1
	ALLDONE MRStateType = 2
)

type MRState struct {
	state          MRStateType
	nMapIN_PROG    int32
	nMapDone       int32
	nReduceIN_PROG int32
	nReduceDone    int32
}

type TaskStateType int32

const (
	IDLE        TaskStateType = 0
	IN_PROGRESS TaskStateType = 1
	COMPLETED   TaskStateType = 2
)

type MapTask struct {
	index    int32
	fileName string
	state    TaskStateType
}

type ReduceTask struct {
	index int32
	state TaskStateType
}

type Master struct {
	// Your definitions here.

	// We first try the simplest lock, get the program run first
	mu sync.Mutex

	mrState        MRState
	mapTaskList    []MapTask
	reduceTaskList []ReduceTask
	nMap           int32
	nReduce        int32
}

// Your code here -- RPC handlers for the worker to call.

// this func is called by the worker to get either map or reduce work,
// depending on the state of Master
func (m *Master) GetWork(args *CallForWorkArgs, reply *CallForWorkReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mrState.state == MAP {
		//log.Print("GetWork() called for map work")
		if m.mrState.nMapIN_PROG+m.mrState.nMapDone == m.nMap {
			reply.HasWork = false
			return nil
		}
		for i := int32(0); i < m.nMap; i++ {
			if m.mapTaskList[i].state == IDLE {
				m.mapTaskList[i].state = IN_PROGRESS
				m.mrState.nMapIN_PROG++
				//send the work
				reply.HasWork = true
				reply.WorkType = MAPWORK
				reply.Content.Index = m.mapTaskList[i].index
				reply.Content.Filename = m.mapTaskList[i].fileName
				reply.Content.NumMapWork = m.nMap
				reply.Content.NumReduceWork = m.nReduce
				return nil
			}
		}
		//should not reach here
		return errors.New("master GetWork() get map work error")
	} else if m.mrState.state == REDUCE {
		//log.Print("GetWork() called for reduce work")
		if m.mrState.nReduceIN_PROG+m.mrState.nReduceDone == m.nReduce {
			reply.HasWork = false
			return nil
		}
		for i := int32(0); i < m.nReduce; i++ {
			if m.reduceTaskList[i].state == IDLE {
				m.reduceTaskList[i].state = IN_PROGRESS
				m.mrState.nReduceIN_PROG++
				//send the work
				reply.HasWork = true
				reply.WorkType = REDUCEWORK
				reply.Content.Index = m.reduceTaskList[i].index
				reply.Content.NumMapWork = m.nMap
				reply.Content.NumReduceWork = m.nReduce
				return nil
			}
		}
		//should not reach here
		return errors.New("master GetWork() get reduce work error")
	} else if m.mrState.state == ALLDONE {
		reply.HasWork = false
		return nil
	} else {
		//should not reach here
		return errors.New("master GetWork() m.mrState.state error")
	}
}

func (m *Master) WorkDone(args *CallWorkDoneArgs, reply *CallWorkDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.WorkType == MAPWORK {
		state := m.mapTaskList[args.Content.Index].state

		if state == COMPLETED {
			return nil
		}
		m.mrState.nMapDone++
		if state == IN_PROGRESS {
			m.mrState.nMapIN_PROG--
		}
		m.mapTaskList[args.Content.Index].state = COMPLETED
		if m.mrState.nMapDone == m.nMap {
			m.mrState.state = REDUCE
		}

		return nil
	} else if args.WorkType == REDUCEWORK {
		state := m.reduceTaskList[args.Content.Index].state

		if state == COMPLETED {
			return nil
		}
		m.mrState.nReduceDone++
		if state == IN_PROGRESS {
			m.mrState.nReduceIN_PROG--
		}
		m.reduceTaskList[args.Content.Index].state = COMPLETED
		if m.mrState.nReduceDone == m.nReduce {
			m.mrState.state = ALLDONE
		}

		return nil
	} else {
		return errors.New("master WorkDone() args.WorkType error")
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mrState.state == ALLDONE {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	m := Master{}

	// Your code here.
	// init master

	m.mrState.nMapIN_PROG = 0
	m.mrState.nMapDone = 0
	m.mrState.nReduceIN_PROG = 0
	m.mrState.nReduceDone = 0
	m.mrState.state = MAP
	m.nMap = int32(len(files))
	m.nReduce = int32(nReduce)

	m.mapTaskList = make([]MapTask, 0)
	for i := int32(0); i < m.nMap; i++ {
		m.mapTaskList = append(m.mapTaskList, MapTask{index: i, fileName: files[i], state: IDLE})
	}
	m.reduceTaskList = make([]ReduceTask, 0)
	for i := int32(0); i < m.nReduce; i++ {
		m.reduceTaskList = append(m.reduceTaskList, ReduceTask{index: i, state: IDLE})
	}

	m.server()
	return &m
}
