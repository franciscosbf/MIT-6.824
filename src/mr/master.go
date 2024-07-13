package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const timeout = time.Second * 10

type workType int

const (
	mapFile workType = iota
	reduceFiles
)

type task struct {
	input []string
	wt    workType
}

type worker struct {
	beat chan struct{}
	dead atomic.Bool

	intermidiateDone chan struct{}

	wid int
	tk  atomic.Pointer[task]
}

func newWorkerMetaInfo(wid int) *worker {
	return &worker{
		beat:             make(chan struct{}, 1),
		wid:              wid,
		intermidiateDone: make(chan struct{}, 1),
	}
}

func (w *worker) alive() {
	select {
	case w.beat <- struct{}{}:
	default:
	}
}

func (w *worker) setTaskMarker(tk *task) {
	w.tk.Store(tk)
}

func (w *worker) removeTaskMarker() {
	w.tk.Store(nil)
}

func (w *worker) setDead() {
	w.dead.Store(true)
}

func (w *worker) isDead() bool {
	return w.dead.Load()
}

func (w *worker) reborn(
	rMappings chan<- mapping,
	rReductions chan<- reduction,
	pCard *punchCard,
) {
	if !w.dead.CompareAndSwap(true, false) {
		return
	}

	w.heartbeat(rMappings, rReductions, pCard)
}

func (w *worker) heartbeat(
	rMappings chan<- mapping,
	rReductions chan<- reduction,
	pCard *punchCard,
) {
	go func() {
		timer := time.NewTimer(timeout)

		for {
			select {
			case <-timer.C:
				w.setDead()

				pCard.forceRemove(w.wid)

				if wtk := w.tk.Load(); wtk != nil {
					switch wtk.wt {
					case mapFile:
						rMappings <- wtk.input[0]
					case reduceFiles:
						rReductions <- wtk.input
					}

					w.tk.CompareAndSwap(wtk, nil)
				}

				return
			case <-w.beat:
				if !timer.Stop() {
					<-timer.C
				}

				timer.Reset(timeout)
			}
		}
	}()
}

func (w *worker) alertIntermidiateDone() {
	select {
	case w.intermidiateDone <- struct{}{}:
	default:
	}
}

type workers struct {
	wks map[int]*worker

	sync.RWMutex
}

func newWorkers() workers {
	return workers{wks: make(map[int]*worker)}
}

func (w *workers) get(wid int) (*worker, bool) {
	w.Lock()
	defer w.Unlock()

	if wm, exists := w.wks[wid]; exists {
		return wm, true
	}

	wm := newWorkerMetaInfo(wid)

	w.wks[wm.wid] = wm

	return wm, false
}

func (w *workers) interate(f func(*worker)) {
	w.Lock()
	defer w.Unlock()

	for _, wm := range w.wks {
		f(wm)
	}
}

type mapping = string

type mappings struct {
	files []string

	sync.Mutex
}

func newMappings(files []string) *mappings {
	ms := &mappings{
		files: files,
	}

	return ms
}

func (m *mappings) pop() (mapping, bool) {
	m.Lock()
	defer m.Unlock()

	if len(m.files) == 0 {
		return "", false
	}

	file := m.files[0]
	m.files = m.files[1:]

	return file, true
}

type reduction = []string

type reductions struct {
	cache map[int]int
	tmps  [][]string

	sync.Mutex
}

func newReductions() *reductions {
	return &reductions{cache: make(map[int]int)}
}

func (r *reductions) append(files []Intermidiate) int {
	r.Lock()
	defer r.Unlock()

	newBatches := 0

	for _, inf := range files {
		if idx, ok := r.cache[inf.Rid]; ok {
			r.tmps[idx] = append(r.tmps[idx], inf.File)
		} else {
			r.cache[inf.Rid] = len(r.tmps)
			r.tmps = append(r.tmps, []string{inf.File})
			newBatches++
		}
	}

	return newBatches
}

func (r *reductions) pop() (reduction, bool) {
	r.Lock()
	defer r.Unlock()

	if len(r.tmps) == 0 {
		return nil, false
	}

	intermidiates := r.tmps[0]
	r.tmps = r.tmps[1:]

	return intermidiates, true
}

type counter struct {
	c atomic.Int64
}

func newCounter() *counter {
	return &counter{}
}

func (bc *counter) set(v int) {
	bc.c.Store(int64(v))
}

func (bc *counter) inc(v int) {
	bc.c.Add(int64(v))
}

func (bc *counter) dec() {
	bc.c.Add(-1)
}

func (bc *counter) reached() bool {
	return bc.c.Load() <= 0
}

type punchCard struct {
	ids map[int]int64

	sync.Mutex
}

func newPunchCard() *punchCard {
	return &punchCard{ids: make(map[int]int64)}
}

func (t *punchCard) set(wid int) int64 {
	t.Lock()
	defer t.Unlock()

	id := time.Now().UnixMicro()
	t.ids[wid] = id

	return id
}

func (t *punchCard) forceRemove(wid int) {
	t.Lock()
	defer t.Unlock()

	delete(t.ids, wid)
}

func (t *punchCard) remove(wid int, id int64) bool {
	t.Lock()
	defer t.Unlock()

	existingId, deleted := t.ids[wid]
	if !deleted && existingId != id {
		return false
	}
	delete(t.ids, wid)

	return deleted
}

type Master struct {
	mappings   *mappings
	reductions *reductions

	mappingsC   *counter
	reductionsC *counter

	pcard *punchCard

	rMapping   chan mapping
	rReduction chan reduction

	intermidiateDone chan struct{}
	doneAlert        chan struct{}
	done             atomic.Bool

	workers workers
	batchSz int
}

func (m *Master) MapRequest(
	arg *MapRequestArgs,
	reply *MapRequestReply,
) (err error) {
	if m.done.Load() {
		*reply = MapRequestReply{Response: Finished}

		return
	}

	if m.mappingsC.reached() {
		*reply = MapRequestReply{Response: IntermidiateDone}

		return
	}

	w := m.checkWorkerState(arg.Wid)

	if file, has := m.mappings.pop(); has {
		tkId := m.pcard.set(arg.Wid)

		tk := &task{
			input: []string{file},
			wt:    mapFile,
		}
		w.setTaskMarker(tk)

		*reply = MapRequestReply{
			File:     file,
			TkId:     tkId,
			BatchSz:  m.batchSz,
			Response: ToDo,
		}

		return
	}

	if file, hasMore := m.lookupMappingTask(w); hasMore {
		tkId := m.pcard.set(arg.Wid)

		*reply = MapRequestReply{
			File:     file,
			TkId:     tkId,
			BatchSz:  m.batchSz,
			Response: ToDo,
		}
	} else {
		*reply = MapRequestReply{Response: IntermidiateDone}
	}

	return
}

func (m *Master) MapDone(
	arg *MapDoneArgs,
	reply *MapDoneReply,
) (err error) {
	if m.done.Load() {
		*reply = MapDoneReply{Response: Finished}

		return
	}

	if m.mappingsC.reached() {
		*reply = MapDoneReply{Response: IntermidiateDone}

		return
	}

	if !m.pcard.remove(arg.Wid, arg.TkId) {
		*reply = MapDoneReply{Response: InvalidTaskId}

		return
	}

	m.mappingsC.dec()

	w := m.checkWorkerState(arg.Wid)

	w.removeTaskMarker()

	newBatches := m.reductions.append(arg.Files)
	m.reductionsC.inc(newBatches)

	if m.mappingsC.reached() {
		m.intermidiateDone <- struct{}{}

		*reply = MapDoneReply{Response: IntermidiateDone}
	} else {
		*reply = MapDoneReply{Response: Accepted}
	}

	return
}

func (m *Master) ReductionRequest(
	arg *ReductionRequestArgs,
	reply *ReductionRequestReply,
) (err error) {
	if m.done.Load() {
		*reply = ReductionRequestReply{Response: Finished}

		return
	}

	w := m.checkWorkerState(arg.Wid)

	if files, has := m.reductions.pop(); has {
		tkId := m.pcard.set(arg.Wid)

		tk := &task{
			input: files,
			wt:    mapFile,
		}
		w.setTaskMarker(tk)

		*reply = ReductionRequestReply{
			Files:    files,
			TkId:     tkId,
			Response: ToDo,
		}

		return
	}

	if files, hasMore := m.lookupReductionTask(); hasMore {
		tkId := m.pcard.set(arg.Wid)

		*reply = ReductionRequestReply{
			Files:    files,
			TkId:     tkId,
			Response: ToDo,
		}
	} else {
		*reply = ReductionRequestReply{Response: Finished}
	}

	return
}

func (m *Master) ReductionDone(
	arg *ReductionDoneArgs,
	reply *ReductionDoneReply,
) (err error) {
	if m.done.Load() {
		*reply = ReductionDoneReply{Response: Finished}

		return
	}

	if !m.pcard.remove(arg.Wid, arg.TkId) {
		*reply = ReductionDoneReply{Response: InvalidTaskId}

		return
	}

	m.reductionsC.dec()

	w := m.checkWorkerState(arg.Wid)

	w.removeTaskMarker()

	if m.reductionsC.reached() {
		m.done.Store(true)

		m.doneAlert <- struct{}{}

		*reply = ReductionDoneReply{Response: Finished}
	} else {
		*reply = ReductionDoneReply{Response: Accepted}
	}

	return
}

func (m *Master) lookupMappingTask(w *worker) (mapping, bool) {
	if !m.mappingsC.reached() {
		select {
		case file := <-m.rMapping:
			return file, true
		case <-w.intermidiateDone:
		}
	}

	return "", false
}

func (m *Master) lookupReductionTask() (reduction, bool) {
	if !m.reductionsC.reached() {
		select {
		case files := <-m.rReduction:
			return files, true
		case <-m.doneAlert:
			close(m.doneAlert)
		}
	}

	return []string{}, false
}

func (m *Master) checkWorkerState(wid int) *worker {
	w, exists := m.workers.get(wid)

	if exists {
		if w.isDead() {
			w.reborn(
				m.rMapping,
				m.rReduction,
				m.pcard,
			)
		} else {
			w.alive()
		}
	} else {
		w.heartbeat(
			m.rMapping,
			m.rReduction,
			m.pcard,
		)
	}

	return w
}

func (m *Master) intermidiateVigilant() {
	go func() {
		<-m.intermidiateDone

		m.workers.interate(func(w *worker) {
			w.alertIntermidiateDone()
		})
	}()
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	sockname := masterSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	// Your code here.
	select {
	case <-m.doneAlert:
		close(m.doneAlert)
		return true
	default:
		return false
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	batchTasks := len(files)

	m := &Master{
		mappings:   newMappings(files),
		reductions: newReductions(),

		pcard: newPunchCard(),

		mappingsC:   newCounter(),
		reductionsC: newCounter(),

		rMapping:   make(chan mapping, batchTasks),
		rReduction: make(chan reduction, batchTasks),

		intermidiateDone: make(chan struct{}, 1),
		doneAlert:        make(chan struct{}, 1),

		workers: newWorkers(),
		batchSz: nReduce,
	}

	m.mappingsC.inc(batchTasks)
	m.intermidiateVigilant()

	m.server()

	return m
}
