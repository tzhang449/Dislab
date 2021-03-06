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

type MRStateType int32

const (
	MAP     MRStateType = 0
	REDUCE  MRStateType = 1
	ALLDONE MRStateType = 2
)

//state of the mr task
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

//map task data structure
type MapTask struct {
	index    int32
	fileName string
	state    TaskStateType
	timer    *time.Timer
}

//reduce task data structure
type ReduceTask struct {
	index int32
	state TaskStateType
	timer *time.Timer
}

type Master struct {
	// Your definitions here.

	// We first try the simplest lock, get the program run first
	mu sync.Mutex

	// master stores the state of the task, lists for mr tasks and their total number
	mrState        MRState
	mapTaskList    []MapTask
	reduceTaskList []ReduceTask
	nMap           int32
	nReduce        int32
}

// Your code here -- RPC handlers for the worker to call.

// this func is called by the worker to get either map or reduce work,
// depending on the state of the task
func (m *Master) GetWork(args *CallForWorkArgs, reply *CallForWorkReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mrState.state == MAP {
		//log.Print("GetWork() called for map work")

		//no more task avaiable, the worker shall wait
		if m.mrState.nMapIN_PROG+m.mrState.nMapDone == m.nMap {
			reply.HasWork = false
			return nil
		}

		for i := int32(0); i < m.nMap; i++ {
			if m.mapTaskList[i].state == IDLE {
				//this work is available
				m.mapTaskList[i].state = IN_PROGRESS

				//fire the timer, this creates another go routine that calls timeOutFunc once timeout,
				//we wait 10 seconds here, as is described in the hints
				timeOutFunc := m.genTimeOutFunc(MAPWORK, i)
				m.mapTaskList[i].timer = time.AfterFunc(10*time.Second, timeOutFunc)
				m.mrState.nMapIN_PROG++

				//send the task
				reply.HasWork = true
				reply.WorkType = MAPWORK
				reply.Content.Index = m.mapTaskList[i].index
				reply.Content.Filename = m.mapTaskList[i].fileName
				reply.Content.NumMapWork = m.nMap
				reply.Content.NumReduceWork = m.nReduce
				log.Print("map task assigned:", i)
				return nil
			}
		}
		//should not reach here
		return errors.New("master GetWork() get map work error")
	} else if m.mrState.state == REDUCE {
		//log.Print("GetWork() called for reduce work")

		//no more task avaiable, the worker shall wait
		if m.mrState.nReduceIN_PROG+m.mrState.nReduceDone == m.nReduce {
			reply.HasWork = false
			return nil
		}
		for i := int32(0); i < m.nReduce; i++ {
			if m.reduceTaskList[i].state == IDLE {
				//this work is available
				m.reduceTaskList[i].state = IN_PROGRESS

				//fire the timer, this creates another go routine that calls timeOutFunc once timeout
				timeOutFunc := m.genTimeOutFunc(REDUCEWORK, i)
				m.reduceTaskList[i].timer = time.AfterFunc(10*time.Second, timeOutFunc)
				m.mrState.nReduceIN_PROG++

				//send the task
				reply.HasWork = true
				reply.WorkType = REDUCEWORK
				reply.Content.Index = m.reduceTaskList[i].index
				reply.Content.NumMapWork = m.nMap
				reply.Content.NumReduceWork = m.nReduce
				log.Print("reduce task assigned:", i)
				return nil
			}
		}
		//should not reach here
		return errors.New("master GetWork() get reduce work error")
	} else if m.mrState.state == ALLDONE {
		//All done here
		reply.HasWork = false
		return nil
	} else {
		//should not reach here
		return errors.New("master GetWork() m.mrState.state error")
	}
}

//this func is called by the worker to report a completed task.
func (m *Master) WorkDone(args *CallWorkDoneArgs, reply *CallWorkDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.WorkType == MAPWORK {
		// map work done
		state := m.mapTaskList[args.Content.Index].state

		if state == COMPLETED {
			return nil
		}
		m.mrState.nMapDone++
		if state == IN_PROGRESS {
			m.mrState.nMapIN_PROG--
		}
		m.mapTaskList[args.Content.Index].state = COMPLETED
		log.Print("map task:", args.Content.Index, " done")

		//we need to stop the timer here. Note that the timer may not be exactly the same timer that invoked by the worker, since this worker could be timeout before another worker assigned with this task. This will not effect the behavior of the program.
		if m.mapTaskList[args.Content.Index].timer != nil {
			m.mapTaskList[args.Content.Index].timer.Stop()
		}
		if m.mrState.nMapDone == m.nMap {
			m.mrState.state = REDUCE
		}

		return nil
	} else if args.WorkType == REDUCEWORK {
		//same step as map work
		state := m.reduceTaskList[args.Content.Index].state

		if state == COMPLETED {
			return nil
		}
		m.mrState.nReduceDone++
		if state == IN_PROGRESS {
			m.mrState.nReduceIN_PROG--
		}
		m.reduceTaskList[args.Content.Index].state = COMPLETED
		log.Print("reduce task:", args.Content.Index, " done")
		if m.reduceTaskList[args.Content.Index].timer != nil {
			m.reduceTaskList[args.Content.Index].timer.Stop()
		}
		if m.mrState.nReduceDone == m.nReduce {
			m.mrState.state = ALLDONE
		}

		return nil
	} else {
		return errors.New("master WorkDone() args.WorkType error")
	}
}

//generate a timeOut func, used for time.AfterFunc() for crash recovery
func (m *Master) genTimeOutFunc(workType WorkType, index int32) func() {
	return func() {
		m.timeOut(workType, index)
	}
}

//the timeout function used to reset the timeout work
func (m *Master) timeOut(workType WorkType, index int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Print("timeout! worktype=", workType, " index=", index)
	if workType == MAPWORK {
		//a map work has timed out, basic logic is to check whether state is COMPLETED or not,
		//then reset the work and remove the timer
		state := m.mapTaskList[index].state
		if state != COMPLETED {
			if state == IN_PROGRESS {
				m.mrState.nMapIN_PROG--
			}
			m.mapTaskList[index].state = IDLE
			m.mapTaskList[index].timer = nil
		}
	} else if workType == REDUCEWORK {
		state := m.reduceTaskList[index].state
		if state != COMPLETED {
			if state == IN_PROGRESS {
				m.mrState.nReduceIN_PROG--
			}
			m.reduceTaskList[index].state = IDLE
			m.reduceTaskList[index].timer = nil
		}
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
