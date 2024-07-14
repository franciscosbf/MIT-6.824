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

func newWorker(wid int) *worker {
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

func (w *worker) reborn(m *Master) {
	if !w.dead.CompareAndSwap(true, false) {
		return
	}

	w.heartbeat(m)
}

func (w *worker) heartbeat(m *Master) {
	go func() {
		timer := time.NewTimer(timeout)

		for {
			select {
			case <-timer.C:
				w.setDead()

				m.forceUnpin(w.wid)

				if wtk := w.tk.Load(); wtk != nil {
					w.tk.CompareAndSwap(wtk, nil)

					switch wtk.wt {
					case mapFile:
						m.rescheduleMapping(wtk.input[0])
					case reduceFiles:
						m.rescheduleReduction(wtk.input)
					}
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

func (w *workers) gen() *worker {
	w.Lock()
	defer w.Unlock()

	nw := newWorker(len(w.wks))
	w.wks[nw.wid] = nw

	return nw
}

func (w *workers) get(wid int) (*worker, bool) {
	w.Lock()
	defer w.Unlock()

	if wm, exists := w.wks[wid]; exists {
		return wm, true
	}

	return nil, false
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
	cache         map[int]int
	intermidiates [][]string

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
			r.intermidiates[idx] = append(r.intermidiates[idx], inf.File)
		} else {
			r.cache[inf.Rid] = len(r.intermidiates)
			r.intermidiates = append(r.intermidiates, []string{inf.File})
			newBatches++
		}
	}

	return newBatches
}

func (r *reductions) pop() (reduction, bool) {
	r.Lock()
	defer r.Unlock()

	if len(r.intermidiates) == 0 {
		return nil, false
	}

	intermidiates := r.intermidiates[0]
	r.intermidiates = r.intermidiates[1:]

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

	rMappings   chan mapping
	rReductions chan reduction

	intermidiateDone chan struct{}
	doneAlert        chan struct{}
	done             atomic.Bool

	workers workers
	batchSz int
}

func (m *Master) GenWorker(
	args *GenWorkerArgs,
	reply *GenWorkerReply,
) error {
	w := m.workers.gen()
	w.heartbeat(m)

	*reply = GenWorkerReply{
		Wid:     w.wid,
		BatchSz: m.batchSz,
	}

	return nil
}

func (m *Master) MappingRequest(
	args *MappingRequestArgs,
	reply *MappingRequestReply,
) (err error) {
	if m.finished() {
		*reply = MappingRequestReply{Response: Finished}

		return
	}

	if m.isMappingsDone() {
		*reply = MappingRequestReply{Response: IntermidiateDone}

		return
	}

	w, exists := m.checkWorkerState(args.Wid)
	if !exists {
		*reply = MappingRequestReply{Response: InvalidWorkerId}

		return
	}

	if file, has := m.mappings.pop(); has {
		tkId := m.pinNewTask(args.Wid)

		tk := &task{
			input: []string{file},
			wt:    mapFile,
		}
		w.setTaskMarker(tk)

		*reply = MappingRequestReply{
			File:     file,
			TkId:     tkId,
			Response: ToDo,
		}

		return
	}

	if file, hasMore := m.lookupMappingTask(w); hasMore {
		tkId := m.pinNewTask(args.Wid)

		*reply = MappingRequestReply{
			File:     file,
			TkId:     tkId,
			Response: ToDo,
		}
	} else {
		*reply = MappingRequestReply{Response: IntermidiateDone}
	}

	return
}

func (m *Master) MappingDone(
	args *MappingDoneArgs,
	reply *MappingDoneReply,
) (err error) {
	if m.finished() {
		*reply = MappingDoneReply{Response: Finished}

		return
	}

	if m.isMappingsDone() {
		*reply = MappingDoneReply{Response: IntermidiateDone}

		return
	}

	if !m.unpinTask(args.Wid, args.TkId) {
		*reply = MappingDoneReply{Response: InvalidTaskId}

		return
	}

	m.checkMarkMappings()

	w, exists := m.checkWorkerState(args.Wid)
	if !exists {
		*reply = MappingDoneReply{Response: InvalidWorkerId}

		return
	}

	w.removeTaskMarker()

	m.storeIntermidiates(args.Intermidiates)

	if m.isMappingsDone() {
		m.alertMappingsDone()

		*reply = MappingDoneReply{Response: Accepted | IntermidiateDone}
	} else {
		*reply = MappingDoneReply{Response: Accepted}
	}

	return
}

func (m *Master) ReductionRequest(
	args *ReductionRequestArgs,
	reply *ReductionRequestReply,
) (err error) {
	if m.finished() {
		*reply = ReductionRequestReply{Response: Finished}

		return
	}

	w, exists := m.checkWorkerState(args.Wid)
	if !exists {
		*reply = ReductionRequestReply{Response: InvalidWorkerId}

		return
	}

	if files, has := m.reductions.pop(); has {
		tkId := m.pinNewTask(args.Wid)

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
		tkId := m.pinNewTask(args.Wid)

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
	args *ReductionDoneArgs,
	reply *ReductionDoneReply,
) (err error) {
	if m.finished() {
		*reply = ReductionDoneReply{Response: Finished}

		return
	}

	if !m.unpinTask(args.Wid, args.TkId) {
		*reply = ReductionDoneReply{Response: InvalidTaskId}

		return
	}

	m.checkMarkReductions()

	w, exists := m.checkWorkerState(args.Wid)
	if !exists {
		*reply = ReductionDoneReply{Response: InvalidWorkerId}

		return
	}

	w.removeTaskMarker()

	if m.isReductionsDone() {
		m.markAsFinished()
		m.alertReductionsDone()

		*reply = ReductionDoneReply{Response: Accepted | Finished}
	} else {
		*reply = ReductionDoneReply{Response: Accepted}
	}

	return
}

func (m *Master) lookupMappingTask(w *worker) (mapping, bool) {
	if !m.mappingsC.reached() {
		select {
		case file := <-m.rMappings:
			return file, true
		case <-w.intermidiateDone:
		}
	}

	return "", false
}

func (m *Master) lookupReductionTask() (reduction, bool) {
	if !m.reductionsC.reached() {
		select {
		case files := <-m.rReductions:
			return files, true
		case <-m.doneAlert:
			close(m.doneAlert)
		}
	}

	return []string{}, false
}

func (m *Master) storeIntermidiates(intermidiates []Intermidiate) {
	newBatches := m.reductions.append(intermidiates)
	m.reductionsC.inc(newBatches)
}

func (m *Master) pinNewTask(wid int) int64 {
	return m.pcard.set(wid)
}

func (m *Master) unpinTask(wid int, tkId int64) bool {
	return m.pcard.remove(wid, tkId)
}

func (m *Master) forceUnpin(wid int) {
	m.pcard.forceRemove(wid)
}

func (m *Master) rescheduleMapping(file string) {
	m.rMappings <- file
}

func (m *Master) rescheduleReduction(files []string) {
	m.rReductions <- files
}

func (m *Master) checkWorkerState(wid int) (*worker, bool) {
	w, exists := m.workers.get(wid)

	if !exists {
		return nil, false
	}

	if w.isDead() {
		w.reborn(m)
	} else {
		w.alive()
	}

	return w, true
}

func (m *Master) intermidiateVigilant() {
	go func() {
		<-m.intermidiateDone

		m.workers.interate(func(w *worker) {
			w.alertIntermidiateDone()
		})
	}()
}

func (m *Master) checkMarkMappings() {
	m.mappingsC.dec()
}

func (m *Master) checkMarkReductions() {
	m.reductionsC.dec()
}

func (m *Master) markAsFinished() {
	m.done.Store(true)
}

func (m *Master) finished() bool {
	return m.done.Load()
}

func (m *Master) isMappingsDone() bool {
	return m.mappingsC.reached()
}

func (m *Master) alertMappingsDone() {
	select {
	case m.intermidiateDone <- struct{}{}:
	default:
	}
}

func (m *Master) isReductionsDone() bool {
	return m.reductionsC.reached()
}

func (m *Master) alertReductionsDone() {
	select {
	case m.doneAlert <- struct{}{}:
	default:
	}
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

		rMappings:   make(chan mapping, batchTasks),
		rReductions: make(chan reduction, batchTasks),

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
