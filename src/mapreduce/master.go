package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	alive bool
}

// KillWorkers Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	// do map
	jobLoop(mr, true)

	// do reduce
	jobLoop(mr, false)

	fmt.Println("Master: MapReduce Done, available workers: ", mr.available)

	return mr.KillWorkers()
}

func jobLoop(mr *MapReduce, doMap bool) {
	loop := 0

	if doMap {
		loop = mr.nMap
	} else {
		loop = mr.nReduce
	}

	for i := 0; i < loop; i++ {
		for true {
			worker := <-mr.registerChannel
			// default: working well
			_, flag := mr.Workers[worker]
			if !flag {
				// for new worker, available + 1
				mr.Workers[worker] = &WorkerInfo{worker, true}
				mr.available += 1
			}

			ok := callMapReduce(mr, worker, i, doMap)
			if ok {
				// working well, recycle idle worker
				go func() {
					mr.registerChannel <- worker
				}()
				break
			} else {
				// failure, available - 1
				mr.Workers[worker].alive = false
				mr.available -= 1
			}
		}
		//job is done
		if doMap {
			fmt.Println("Finish map job ", i)
		} else {
			fmt.Println("Finish reduce job ", i)
		}

	}
	if doMap {
		fmt.Println("Finish all map jobs")
	} else {
		fmt.Println("Finish all reduce jobs")
	}
}

func callMapReduce(mr *MapReduce, worker string, jobNumber int, doMap bool) bool {
	// doMap: true for map job, false for reduce job
	args := &DoJobArgs{}
	var reply DoJobReply

	args.File = mr.file
	args.JobNumber = jobNumber

	if doMap {
		args.Operation = Map
		args.NumOtherPhase = mr.nReduce
	} else {
		args.Operation = Reduce
		args.NumOtherPhase = mr.nMap
	}

	return call(worker, "Worker.DoJob", args, &reply)
}
