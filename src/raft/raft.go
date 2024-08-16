package raft

import (
	"../labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

const (
	heartbeatTimeout = time.Millisecond * 100

	electionTimeoutMin = time.Millisecond * 350
	electionTimeoutMax = time.Millisecond * 500

	batchSz = 128

	applyCommandsTimeout = time.Millisecond * 100
)

func genElectionTimeout() time.Duration {
	return time.Duration(
		rand.Intn(int(electionTimeoutMax)+1-int(electionTimeoutMin)) + int(electionTimeoutMin))
}

func calcMajority(nPeers int) int {
	return nPeers / 2
}

type peerState int

const (
	follower peerState = iota
	candidate
	leader
)

func (ps peerState) String() string {
	switch ps {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	}

	return ""
}

type hearbeatState int

const (
	stopHeartbeat hearbeatState = iota
	resumeHeartbeat
	immediateHeartbeat
)

type electionTimeoutState int

const (
	stopElectionTimeout electionTimeoutState = iota
	resumeElectionTimeout
	resetElectionTimeout
)

type appendState int

const (
	appendSent appendState = iota
	appendNotSent
	backToFollower
	abort
)

type Entry struct {
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (rva *RequestVoteArgs) String() string {
	return fmt.Sprintf(
		"{Term: %v, CandidateId: %v, LastLogIndex: %v, LastLogTerm: %v}",
		rva.Term, rva.CandidateId, rva.LastLogIndex, rva.LastLogTerm)
}

type RequestVoteReply struct {
	// Your data here (2A).

	Term        int
	VoteGranted bool
}

func (rva *RequestVoteReply) String() string {
	return fmt.Sprintf(
		"{Term: %v, VoteGranted: %v}",
		rva.Term, rva.VoteGranted)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

func (rva *AppendEntriesArgs) String() string {
	return fmt.Sprintf(
		"{Term: %v, LeaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v, Entries: %v, LeaderCommit: %v}",
		rva.Term, rva.LeaderId, rva.PrevLogIndex, rva.PrevLogTerm, rva.Entries, rva.LeaderCommit)
}

type ConflictingEntry struct {
	Term  int
	Index int
}

type RecoveryTrace struct {
	Entry ConflictingEntry
	Len   int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	Trace RecoveryTrace
}

func (rva *AppendEntriesReply) String() string {
	return fmt.Sprintf(
		"{Term: %v, Success: %v}",
		rva.Term, rva.Success)
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // receives applied messages

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	majority              int
	state                 peerState
	signalKill            chan struct{}
	signalHeartbeat       chan hearbeatState
	signalElectionTimeout chan electionTimeoutState
	signalElectionHalt    chan struct{}
	electing              int32
	batches               chan *AppendEntriesArgs
	sending               []*int32
	sendingAlert          []*sync.Cond

	// Persistent state
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) setFirstRecoveryEntry(
	argsToUpdate *AppendEntriesArgs,
	trace *RecoveryTrace,
) (peerNextIndex int) {
	go func() {
		argsToUpdate.PrevLogIndex = peerNextIndex - 1
		argsToUpdate.PrevLogTerm = rf.log[argsToUpdate.PrevLogIndex].Term
		argsToUpdate.Entries = rf.log[peerNextIndex:]
	}()

	if trace.Entry == (ConflictingEntry{}) {
		peerNextIndex = trace.Len

		return
	}

	entry := trace.Entry

	low, high := 0, len(rf.log)-1
	index := -1
	for low <= high {
		mid := low + (high-low)/2
		if rf.log[mid].Term == entry.Term {
			index = mid

			break
		}
		if rf.log[mid].Term < entry.Term {
			low++
		} else {
			high--
		}
	}

	if index == -1 {
		peerNextIndex = entry.Index

		return
	}

	i := index
	for last := len(rf.log) - 1; i < last; i++ {
		if rf.log[i+1].Term != entry.Term {
			break
		}
	}
	peerNextIndex = i

	return
}

func (rf *Raft) fireHeartbeat() {
	rf.mu.Lock()
	prevLogIndex := len(rf.log) - 1
	rargs := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      []Entry{},
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	for pi := 0; pi < len(rf.peers); pi++ {
		if pi == rf.me {
			continue
		}

		go func(pi int) {
			if !atomic.CompareAndSwapInt32(rf.sending[pi], 0, 1) {
				return
			}

			args := *rargs
			rf.mu.Lock()
			prevLogIndex := rf.nextIndex[pi] - 1
			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = rf.log[prevLogIndex].Term
			args.Entries = rf.log[prevLogIndex+1:]
			rf.mu.Unlock()

			var success bool
			defer func() {
				atomic.StoreInt32(rf.sending[pi], 0)
				rf.sendingAlert[pi].Signal()

				if !success {
					return
				}

				rf.mu.Lock()
				rf.commitIdxCheck()
				rf.tryToApplyCommands()
				rf.mu.Unlock()
			}()

			for {
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}

				DPrintf("%v - %v tried to sent to %v AppendEntries RPC as part of heartbeat: %v",
					rf.state, rf.me, pi, &args)

				if !rf.peers[pi].Call("Raft.AppendEntries", &args, &reply) {
					return
				}

				DPrintf("%v - %v received from %v AppendEntries RPC reply as part of heartbeat: %v",
					rf.state, rf.me, pi, &reply)

				rf.mu.Lock()
				if reply.Success {
					rf.nextIndex[pi] = len(rf.log)
					rf.matchIndex[pi] = args.PrevLogIndex + len(args.Entries)
					rf.mu.Unlock()

					success = true

					return
				}
				if reply.Term > rf.currentTerm {
					rf.revertToFollower(args.Term)
					rf.mu.Unlock()

					return
				}
				rf.nextIndex[pi] = rf.setFirstRecoveryEntry(&args, &reply.Trace)
				rf.mu.Unlock()
			}
		}(pi)
	}
}

func (rf *Raft) hearbeat() {
	go func() {
		var state hearbeatState

	stopped:
		for {
			select {
			case <-rf.signalKill:
				return
			case state = <-rf.signalHeartbeat:
			}

			switch state {
			case immediateHeartbeat:
				rf.fireHeartbeat()
			case resumeHeartbeat:
			default:
				continue
			}

			break
		}

		DPrintf("%v - %v proceeds to resume heartbeat", rf.state, rf.me)

		for {
			timer := time.After(heartbeatTimeout)

			select {
			case <-rf.signalKill:
				return
			case <-timer:
				rf.fireHeartbeat()
			case state = <-rf.signalHeartbeat:
			}

			switch state {
			case stopHeartbeat:
				DPrintf("%v - %v stopped heartbeat", rf.state, rf.me)

				goto stopped
			case immediateHeartbeat:
				rf.fireHeartbeat()
			case resumeHeartbeat:
			}
		}
	}()
}

func (rf *Raft) stopHeartbeat() {
	rf.signalHeartbeat <- stopHeartbeat
}

func (rf *Raft) resumeHeartBeat() {
	rf.signalHeartbeat <- resumeHeartbeat
}

func (rf *Raft) immediateHeartBeat() {
	rf.signalHeartbeat <- immediateHeartbeat
}

func (rf *Raft) fireVote() bool {
	atomic.StoreInt32(&rf.electing, 1)
	electionHalted := false

	defer func() {
		atomic.StoreInt32(&rf.electing, 0)

		for {
			select {
			case <-rf.signalElectionHalt: // drain channel
			default:
				return
			}
		}
	}()

	DPrintf("%v - %v started an election", rf.state, rf.me)

retry:
	for {
		rf.mu.Lock()
		rf.currentTerm++
		rf.state = candidate
		rf.votedFor = rf.me
		lastLogIndex := len(rf.log) - 1
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.log[lastLogIndex].Term,
		}
		rf.mu.Unlock()

		accepted := make(chan struct{}, len(rf.peers))
		timeout := genElectionTimeout()
		timer := time.After(timeout)
		approved := 0

		DPrintf("%v - %v set election timeout voting to %v", rf.state, rf.me, timeout)

		for pi := 0; pi < len(rf.peers); pi++ {
			if pi == rf.me {
				continue
			}

			go func(pi int) {
				DPrintf("%v - %v tried to send to %v RequestVote RPC: %v", rf.state, rf.me, pi, &args)

				reply := RequestVoteReply{}

				if !rf.peers[pi].Call("Raft.RequestVote", &args, &reply) {
					return
				}

				DPrintf("%v - %v received from %v RequestVote RPC reply: %v",
					rf.state, rf.me, pi, &reply)

				if reply.VoteGranted {
					DPrintf("%v - %v got vote granted from %v", rf.state, rf.me, pi)

					accepted <- struct{}{}

					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.revertToFollower(args.Term)

					if !electionHalted {
						rf.haltElection()
						electionHalted = true
					}
				}
			}(pi)
		}

		for {
			select {
			case <-rf.signalElectionHalt:
				return false
			case <-accepted:
				if approved++; approved < rf.majority {
					continue
				}

				rf.mu.Lock()
				rf.state = leader
				for pi := 0; pi < len(rf.peers); pi++ {
					rf.matchIndex[pi] = 0
					rf.nextIndex[pi] = len(rf.log)
				}
				rf.immediateHeartBeat()
				rf.mu.Unlock()

				DPrintf("%v - %v has become leader with a majority of the votes", rf.state, rf.me)

				return true
			case <-timer:
				goto retry
			case <-rf.signalElectionTimeout: // drain channel
			}
		}
	}
}

func (rf *Raft) electionTimeout() {
	go func() {
		timeout := genElectionTimeout()
		timer := time.After(timeout)

		DPrintf("%v - %v started election timeout with %v", rf.state, rf.me, timeout)

		var state electionTimeoutState

		for {
			select {
			case <-rf.signalKill:
				return
			case <-timer:
				rf.stopHeartbeat()

				if elected := rf.fireVote(); elected {
					for {
						select {
						case <-rf.signalKill:
							return
						case state = <-rf.signalElectionTimeout:
						}

						if state == resumeElectionTimeout {

							DPrintf("%v - %v resumed election timeout", rf.state, rf.me)
							break
						}
					}
				}

				DPrintf("%v - %v proceeds to restart election", rf.state, rf.me)
			case state = <-rf.signalElectionTimeout:
				switch state {
				case stopElectionTimeout:
					for {
						select {
						case <-rf.signalKill:
							return
						case state = <-rf.signalElectionTimeout:
							DPrintf("%v - %v resumed election timeout", rf.state, rf.me)
						}

						if state == resumeElectionTimeout {
							break
						}
					}
				case resumeElectionTimeout:
					continue
				case resetElectionTimeout:
				}
			}

			timeout := genElectionTimeout()
			timer = time.After(timeout)

			DPrintf("%v - %v reseted election timeout with %v", rf.state, rf.me, timeout)
		}
	}()
}

func (rf *Raft) resumeElectionTimeout() {
	rf.signalElectionTimeout <- resumeElectionTimeout
}

func (rf *Raft) resetElectionTimeout() {
	rf.signalElectionTimeout <- resetElectionTimeout
}

func (rf *Raft) stopElectionTimeout() {
	rf.signalElectionTimeout <- stopElectionTimeout
}

func (rf *Raft) haltElection() {
	if atomic.LoadInt32(&rf.electing) == 0 {
		return
	}

	rf.signalElectionHalt <- struct{}{}
}

func (rf *Raft) revertToFollower(term int) {
	rf.stopHeartbeat()
	rf.resumeElectionTimeout()
	rf.releasePendingSenders()

	DPrintf("%v - %v went back to follower state at term %v", rf.state, rf.me, term)

	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
}

func (rf *Raft) commitIdxCheck() {
	N := len(rf.log) - 1
check:
	for ; N > 0; N-- {
		if rf.log[N].Term == rf.currentTerm && N > rf.commitIndex {
			greater := 0
			for _, mi := range rf.matchIndex {
				if mi >= N {
					if greater++; greater == rf.majority {
						rf.commitIndex = N

						break check
					}
				}
			}
		}
	}
}

func (rf *Raft) tryToApplyCommands() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		DPrintf("%v - %v applied entry %v at index %v",
			rf.state, rf.me, rf.log[rf.lastApplied], rf.lastApplied)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

func (rf *Raft) batchAppendEntries() {
	go func() {
		for {
		waiting:
			select {
			case <-rf.signalKill:
				return
			case rargs := <-rf.batches:
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					continue
				}
				rf.mu.Unlock()

				sendAck := make(chan appendState, len(rf.peers))

				for pi := 0; pi < len(rf.peers); pi++ {
					if pi == rf.me {
						continue
					}

					go func(pi int) {
						rf.sendingAlert[pi].L.Lock()
						for !atomic.CompareAndSwapInt32(rf.sending[pi], 0, 1) {
							rf.sendingAlert[pi].Wait()
						}
						rf.sendingAlert[pi].L.Unlock()

						defer atomic.StoreInt32(rf.sending[pi], 0)

						args := *rargs
						rf.mu.Lock()
						prevLogIndex := rf.nextIndex[pi] - 1
						args.PrevLogIndex = prevLogIndex
						args.PrevLogTerm = rf.log[prevLogIndex].Term
						args.Entries = rf.log[prevLogIndex+1:]
						rf.mu.Unlock()

						for {
							reply := AppendEntriesReply{}

							rf.mu.Lock()
							if rf.state != leader {
								sendAck <- abort
								rf.mu.Unlock()

								return
							}
							send := len(rf.log)-1 >= rf.nextIndex[pi]
							rf.mu.Unlock()

							if !send {
								sendAck <- appendNotSent

								return
							}

							DPrintf("%v - %v tried to send to %v AppendEntries RPC as part of forward: %v",
								rf.state, rf.me, pi, &args)

							if !rf.peers[pi].Call("Raft.AppendEntries", &args, &reply) {
								sendAck <- appendNotSent

								return
							}

							DPrintf("%v - %v received from %v AppendEntries RPC reply as part of forward: %v",
								rf.state, rf.me, pi, &reply)

							rf.mu.Lock()
							if reply.Success {
								rf.nextIndex[pi] = len(rf.log)
								rf.matchIndex[pi] = args.PrevLogIndex + len(args.Entries)
								rf.mu.Unlock()

								sendAck <- appendSent

								return
							}
							if reply.Term > rf.currentTerm {
								rf.revertToFollower(args.Term)
								rf.mu.Unlock()

								sendAck <- backToFollower

								return
							}
							rf.nextIndex[pi] = rf.setFirstRecoveryEntry(&args, &reply.Trace)
							rf.mu.Unlock()
						}
					}(pi)
				}

				withMajority := false
				confirmed := 0
			confirmations:
				for i := len(rf.peers) - 1; i > 0; i-- {
					switch <-sendAck {
					case appendSent:
						if confirmed++; confirmed == rf.majority {
							withMajority = true

							break confirmations
						}
					case appendNotSent:
					case backToFollower, abort:
						goto waiting
					}
				}

				if !withMajority {
					continue
				}

				rf.mu.Lock()
				rf.commitIdxCheck()
				rf.tryToApplyCommands()
				rf.mu.Unlock()
			}
		}
	}()
}

func (rf *Raft) releasePendingSenders() {
	for _, sa := range rf.sendingAlert {
		sa.Broadcast()
	}
}

func (rf *Raft) RequestVote(
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) {
	// Your code here (2A, 2B).

	DPrintf("%v - %v received RequestVote RPC: %v", rf.state, rf.me, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		reply.Term = rf.currentTerm
	}()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.haltElection()
		rf.revertToFollower(args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		args.LastLogIndex < len(rf.log) &&
		((rf.log[args.LastLogIndex].Term != args.LastLogTerm &&
			args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
			(rf.log[args.LastLogIndex].Term == args.LastLogTerm &&
				args.LastLogIndex+1 >= len(rf.log))) {

		rf.votedFor = args.CandidateId

		if rf.state != leader {
			rf.resetElectionTimeout()
		}

		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	DPrintf("%v - %v received AppendEntries RPC: %v", rf.state, rf.me, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		reply.Term = rf.currentTerm
	}()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term >= rf.currentTerm {
		if rf.state == candidate || args.Term > rf.currentTerm {
			rf.haltElection()
			rf.revertToFollower(args.Term)
		}
	}

	if rf.state != leader {
		rf.resetElectionTimeout()
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Trace.Len = len(rf.log)

		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		entry := &reply.Trace.Entry
		entry.Term = rf.log[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for ; index > 0; index-- {
			if rf.log[index].Term != entry.Term {
				break
			}
		}
		entry.Index = index + 1

		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]

	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		if lastNewEntryIndex := len(rf.log) - 1; args.LeaderCommit < lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
	}

	rf.tryToApplyCommands()

	reply.Success = true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).

	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	if isLeader = rf.state == leader; !isLeader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := len(rf.log) - 1
	entry := Entry{Term: term, Command: command}
	rargs := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      []Entry{entry},
		LeaderCommit: rf.commitIndex,
	}
	rf.log = append(rf.log, entry)
	rf.mu.Unlock()

	rf.batches <- rargs

	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	// Your code here, if desired.

	if atomic.CompareAndSwapInt32(&rf.dead, 0, 1) {
		close(rf.signalKill)
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	rf.majority = calcMajority(len(peers))
	rf.state = follower
	rf.signalKill = make(chan struct{})
	rf.signalHeartbeat = make(chan hearbeatState, 1)
	rf.signalElectionTimeout = make(chan electionTimeoutState, 1)
	rf.signalElectionHalt = make(chan struct{}, 1)
	rf.batches = make(chan *AppendEntriesArgs, batchSz)
	rf.sending = make([]*int32, len(peers))
	rf.sendingAlert = make([]*sync.Cond, len(peers))
	for pi := 0; pi < len(rf.peers); pi++ {
		rf.sending[pi] = new(int32)
		rf.sendingAlert[pi] = sync.NewCond(&sync.Mutex{})
	}

	rf.log = []Entry{{}}
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(peers))
	for pi := 0; pi < len(rf.peers); pi++ {
		rf.nextIndex[pi] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.hearbeat()
	rf.electionTimeout()
	rf.batchAppendEntries()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
