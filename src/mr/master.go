package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const timeout = time.Second * 10

type workType int

const (
	mapFile workType = iota
	reduceFiles
)

func masterInfo(wid int, format string, v ...interface{}) {
	format = fmt.Sprintf("master %v: %v", wid, format)
	log.Printf(format, v...)
}

type atomicBool struct {
	b uint32
}

func (ab *atomicBool) load() bool {
	return atomic.LoadUint32(&ab.b) == 1
}

func (ab *atomicBool) set(b bool) {
	if b {
		atomic.StoreUint32(&ab.b, 1)
	} else {
		atomic.StoreUint32(&ab.b, 0)
	}
}

func (ab *atomicBool) compareAndSwap(old bool, new bool) bool {
	var ov, nv uint32

	if old {
		ov = 1
	}
	if new {
		nv = 1
	}

	return atomic.CompareAndSwapUint32(&ab.b, ov, nv)
}

type task struct {
	m  mapping
	r  reduction
	wt workType
}

type worker struct {
	beat chan struct{}
	dead atomicBool

	wid int
	tk  unsafe.Pointer
}

func newWorker(wid int) *worker {
	return &worker{
		beat: make(chan struct{}, 1),
		wid:  wid,
	}
}

func (w *worker) alive() {
	select {
	case w.beat <- struct{}{}:
	default:
	}
}

func (w *worker) setTaskMarker(tk *task) {
	atomic.StorePointer(&w.tk, unsafe.Pointer(tk))
}

func (w *worker) removeTaskMarker() {
	atomic.StorePointer(&w.tk, unsafe.Pointer(nil))
}

func (w *worker) setDead() {
	w.dead.set(true)
}

func (w *worker) isDead() bool {
	return w.dead.load()
}

func (w *worker) reborn(m *Master) {
	if !w.dead.compareAndSwap(true, false) {
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
				m.increaseDeadWorkers()

				m.forceUnpin(w.wid)

				if m.allDead() {
					m.finalize()
				}

				if ptr := atomic.LoadPointer(&w.tk); ptr != nil {
					atomic.CompareAndSwapPointer(&w.tk, ptr, nil)

					wtk := (*task)(ptr)
					switch wtk.wt {
					case mapFile:
						m.rescheduleMapping(wtk.m)
					case reduceFiles:
						m.rescheduleReduction(wtk.r)
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

type workers struct {
	wks        map[int]*worker
	registered int64

	sync.RWMutex
}

func newWorkers() *workers {
	return &workers{wks: make(map[int]*worker)}
}

func (w *workers) gen() *worker {
	w.Lock()
	defer w.Unlock()

	nw := newWorker(len(w.wks))
	w.wks[nw.wid] = nw

	atomic.AddInt64(&w.registered, 1)

	return nw
}

func (w *workers) get(wid int) (*worker, bool) {
	w.RLock()
	defer w.RUnlock()

	if wm, exists := w.wks[wid]; exists {
		return wm, true
	}

	return nil, false
}

func (w *workers) interate(f func(*worker)) {
	w.RLock()
	defer w.RUnlock()

	for _, wm := range w.wks {
		f(wm)
	}
}

func (w *workers) registeredWorkers() int64 {
	return atomic.LoadInt64(&w.registered)
}

type mapping struct {
	file string
	id   int
}

type mappings struct {
	files []string
	next  int

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
		return mapping{}, false
	}

	file := m.files[0]
	m.files = m.files[1:]

	mfile := mapping{
		file: file,
		id:   m.next,
	}

	m.next++

	return mfile, true
}

type reduction struct {
	files []string
	id    int
}

type reductions struct {
	cache         map[int]int
	intermidiates []reduction

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
			r.intermidiates[idx].files = append(r.intermidiates[idx].files, inf.File)
		} else {
			r.cache[inf.Rid] = len(r.intermidiates)
			r.intermidiates = append(r.intermidiates, reduction{
				id:    len(r.intermidiates),
				files: []string{inf.File},
			})
			newBatches++
		}
	}

	return newBatches
}

func (r *reductions) pop() (reduction, bool) {
	r.Lock()
	defer r.Unlock()

	if len(r.intermidiates) == 0 {
		return reduction{}, false
	}

	intermidiates := r.intermidiates[0]
	r.intermidiates = r.intermidiates[1:]

	return intermidiates, true
}

type counter struct {
	c int64
}

func newCounter() *counter {
	return &counter{}
}

func (bc *counter) set(v int) {
	atomic.StoreInt64(&bc.c, int64(v))
}

func (bc *counter) add(v int) {
	atomic.AddInt64(&bc.c, int64(v))
}

func (bc *counter) inc() {
	atomic.AddInt64(&bc.c, 1)
}

func (bc *counter) dec() {
	atomic.AddInt64(&bc.c, -1)
}

func (bc *counter) zeroed() bool {
	return bc.get() <= 0
}

func (bc *counter) get() int64 {
	return atomic.LoadInt64(&bc.c)
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

	id := time.Now().UnixNano()
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

	mappingsC    *counter
	reductionsC  *counter
	deadWorkersC *counter

	pcard *punchCard

	rMappings   chan mapping
	rReductions chan reduction

	intermidiateDone   chan struct{}
	intermidiateSignal chan struct{}
	programDone        chan struct{}
	exitSignal         chan struct{}
	done               atomicBool

	workers *workers
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

	if m.areMappingsDone() {
		*reply = MappingRequestReply{Response: IntermidiateDone}

		return
	}

	w, exists := m.checkWorkerState(args.Wid)
	if !exists {
		*reply = MappingRequestReply{Response: InvalidWorkerId}

		return
	}

	var (
		mfile mapping
		has   bool
	)

	if mfile, has = m.mappings.pop(); has {
	} else if mfile, has = m.lookupMappingTask(); has {
	} else {
		*reply = MappingRequestReply{Response: IntermidiateDone}

		return
	}

	tkId := m.pinNewTask(args.Wid)

	tk := &task{
		m:  mfile,
		wt: mapFile,
	}
	w.setTaskMarker(tk)

	*reply = MappingRequestReply{
		Id:       mfile.id,
		File:     mfile.file,
		TkId:     tkId,
		Response: ToDo,
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

	if m.areMappingsDone() {
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

	if m.areMappingsDone() {
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

	var (
		rfiles reduction
		has    bool
	)

	if rfiles, has = m.reductions.pop(); has {
	} else if rfiles, has = m.lookupReductionTask(); has {
	} else {
		*reply = ReductionRequestReply{Response: Finished}

		return
	}

	tkId := m.pinNewTask(args.Wid)

	tk := &task{
		r:  rfiles,
		wt: mapFile,
	}
	w.setTaskMarker(tk)

	*reply = ReductionRequestReply{
		Id:       rfiles.id,
		Files:    rfiles.files,
		TkId:     tkId,
		Response: ToDo,
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

	if m.areReductionsDone() {
		m.finalize()

		*reply = ReductionDoneReply{Response: Accepted | Finished}
	} else {
		*reply = ReductionDoneReply{Response: Accepted}
	}

	return
}

func (m *Master) lookupMappingTask() (mapping, bool) {
	if !m.mappingsC.zeroed() {
		select {
		case mfile := <-m.rMappings:
			return mfile, true
		case <-m.intermidiateSignal:
		}
	}

	return mapping{}, false
}

func (m *Master) lookupReductionTask() (reduction, bool) {
	if !m.reductionsC.zeroed() {
		select {
		case files := <-m.rReductions:
			return files, true
		case <-m.exitSignal:
		}
	}

	return reduction{}, false
}

func (m *Master) storeIntermidiates(intermidiates []Intermidiate) {
	newBatches := m.reductions.append(intermidiates)
	m.reductionsC.add(newBatches)
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

func (m *Master) rescheduleMapping(mfile mapping) {
	m.rMappings <- mfile
}

func (m *Master) rescheduleReduction(rfiles reduction) {
	m.rReductions <- rfiles
}

func (m *Master) checkWorkerState(wid int) (*worker, bool) {
	w, exists := m.workers.get(wid)

	if !exists {
		return nil, false
	}

	if w.isDead() {
		w.reborn(m)
		m.decreaseDeadWorkers()
	} else {
		w.alive()
	}

	return w, true
}

func (m *Master) intermidiateVigilant() {
	go func() {
		<-m.intermidiateDone

		close(m.intermidiateSignal)
	}()
}

func (m *Master) endVigilant() {
	go func() {
		<-m.programDone

		close(m.exitSignal)
	}()
}

func (m *Master) checkMarkMappings() {
	m.mappingsC.dec()
}

func (m *Master) checkMarkReductions() {
	m.reductionsC.dec()
}

func (m *Master) finished() bool {
	return m.done.load()
}

func (m *Master) areMappingsDone() bool {
	return m.mappingsC.zeroed()
}

func (m *Master) alertMappingsDone() {
	select {
	case m.intermidiateDone <- struct{}{}:
	default:
	}
}

func (m *Master) finalize() {
	if !m.done.compareAndSwap(false, true) {
		return
	}

	select {
	case m.programDone <- struct{}{}:
	default:
	}
}

func (m *Master) areReductionsDone() bool {
	return m.reductionsC.zeroed()
}

func (m *Master) increaseDeadWorkers() {
	m.deadWorkersC.inc()
}

func (m *Master) decreaseDeadWorkers() {
	m.deadWorkersC.inc()
}

func (m *Master) allDead() bool {
	return m.deadWorkersC.get() == m.workers.registeredWorkers()
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
	case <-m.exitSignal:
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

		intermidiateDone:   make(chan struct{}, 1),
		intermidiateSignal: make(chan struct{}),
		programDone:        make(chan struct{}, 1),
		exitSignal:         make(chan struct{}),

		workers: newWorkers(),
		batchSz: nReduce,
	}

	m.mappingsC.add(batchTasks)
	m.intermidiateVigilant()
	m.endVigilant()

	m.server()

	return m
}
