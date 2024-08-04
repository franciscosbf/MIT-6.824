package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

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
	heartbeatTimeout = time.Millisecond * 200

	electionTimeoutMin = time.Millisecond * 450
	electionTimeoutMax = time.Millisecond * 600

	batchSz = 128
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
	resumeHeartBeat
)

type electionTimeoutState int

const (
	stopElectionTimeout electionTimeoutState = iota
	resetElectionTimeout
	resumeElectionTimeout
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

type AppendEntriesReply struct {
	Term    int
	Success bool
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
	applyCh               chan ApplyMsg

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

func (rf *Raft) fireHeartbeat() {
	rf.mu.Lock()
	prevLogIndex := len(rf.log) - 1
	args := AppendEntriesArgs{
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
			reply := AppendEntriesReply{}

			DPrintf("%v - %v sent to %v AppendEntries RPC: %v", rf.state, rf.me, pi, &args)

			if !rf.peers[pi].Call("Raft.AppendEntries", &args, &reply) {
				return
			}

			DPrintf("%v - %v received from %v AppendEntries RPC reply: %v", rf.state, rf.me, pi, &reply)

			if reply.Success {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.revertToFollower(args.Term)
			}
			rf.mu.Unlock()
		}(pi)
	}
}

func (rf *Raft) hearbeat() {
	go func() {
		timer := time.After(heartbeatTimeout)

		for {
			select {
			case <-rf.signalKill:
				return
			case <-timer:
				rf.fireHeartbeat()
			case state := <-rf.signalHeartbeat:
				switch state {
				case stopHeartbeat:
					for {
						if state := <-rf.signalHeartbeat; state == resumeHeartBeat {
							break
						}
					}
				case resumeHeartBeat:
					continue
				}
			}

			timer = time.After(heartbeatTimeout)
		}
	}()
}

func (rf *Raft) stopHeartbeat() {
	go func() {
		rf.signalHeartbeat <- stopHeartbeat
	}()
}

func (rf *Raft) resumeHeartBeat() {
	go func() {
		rf.signalHeartbeat <- resumeHeartBeat
	}()
}

func (rf *Raft) fireVote() bool {
	atomic.StoreInt32(&rf.electing, 1)
	var electionHalted int32

	func() {
		atomic.StoreInt32(&rf.electing, 0)
	}()

	DPrintf("%v - %v started an election", rf.state, rf.me)

	for {
	retry:
		rf.mu.Lock()
		rf.currentTerm++
		rf.state = candidate
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

		for pi, peer := range rf.peers {
			if pi == rf.me {
				continue
			}

			go func(peer *labrpc.ClientEnd, pi int) {
				DPrintf("%v - %v sent to %v RequestVote RPC: %v", rf.state, rf.me, pi, &args)

				reply := RequestVoteReply{}

				if !peer.Call("Raft.RequestVote", &args, &reply) {
					return
				}

				DPrintf("%v - %v received from %v RequestVote RPC reply: %v", rf.state, rf.me, pi, &reply)

				if reply.VoteGranted {
					DPrintf("%v - %v got vote granted from %v", rf.state, rf.me, pi)

					accepted <- struct{}{}

					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					if atomic.CompareAndSwapInt32(&electionHalted, 0, 1) {
						rf.haltElection()
					}

					rf.revertToFollower(args.Term)
				}
			}(peer, pi)
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
				rf.mu.Unlock()

				rf.fireHeartbeat()
				rf.resumeHeartBeat()

				DPrintf("%v - %v has become leader with a majority of the votes", rf.state, rf.me)

				return true
			case <-timer:
				goto retry
			}
		}
	}
}

func (rf *Raft) electionTimeout() {
	go func() {
		timeout := genElectionTimeout()
		timer := time.After(timeout)

		DPrintf("%v - %v started election timeout with %v", rf.state, rf.me, timeout)

		for {
			select {
			case <-rf.signalKill:
				return
			case <-timer:
				rf.stopHeartbeat()

				if elected := rf.fireVote(); elected {
					for {
						if state := <-rf.signalElectionTimeout; state == resumeElectionTimeout {
							break
						}
					}
				}
			case state := <-rf.signalElectionTimeout:
				switch state {
				case resumeElectionTimeout:
					continue
				case stopElectionTimeout:
					for {
						if state := <-rf.signalElectionTimeout; state == resumeElectionTimeout {
							break
						}
					}
				case resetElectionTimeout:
				}
			}

			timeout := genElectionTimeout()
			timer = time.After(timeout)

			DPrintf("%v - %v reseted election timeout with %v", rf.state, rf.me, timeout)
		}
	}()
}

func (rf *Raft) stopElectionTimeout() {
	go func() {
		rf.signalElectionTimeout <- stopElectionTimeout
	}()
}

func (rf *Raft) resetElectionTimeout() {
	go func() {
		rf.signalElectionTimeout <- resetElectionTimeout
	}()
}

func (rf *Raft) resumeElectionTimeout() {
	go func() {
		rf.signalElectionTimeout <- resumeElectionTimeout
	}()
}

func (rf *Raft) haltElection() {
	if atomic.LoadInt32(&rf.electing) == 0 {
		return
	}

	go func() {
		rf.signalElectionHalt <- struct{}{}
	}()
}

func (rf *Raft) revertToFollower(term int) {
	rf.stopHeartbeat()

	DPrintf("%v - %v went back to follower state at term %v", rf.state, rf.me, term)

	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
}

func (rf *Raft) evaluateTermOnRPC(term int) {
	if term > rf.currentTerm {
		rf.resumeElectionTimeout()

		rf.revertToFollower(term)
	} else {
		rf.resetElectionTimeout()
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

	rf.evaluateTermOnRPC(args.Term)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		len(rf.log)-1 == args.LastLogIndex &&
		rf.log[args.LastLogIndex].Term == args.LastLogTerm {

		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
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

	if args.Term >= rf.currentTerm {
		rf.evaluateTermOnRPC(args.Term)

		if len(rf.log) > args.PrevLogIndex &&
			rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {

			reply.Success = true

			// TODO: remaining (steps 3., 4. and 5.)
		}
	}
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
	index = rf.commitIndex
	term = rf.currentTerm
	if isLeader = rf.state == leader; !isLeader {
		rf.mu.Unlock()
		return
	}
	entry := Entry{Term: term, Command: command}
	prevLogIndex := len(rf.log) - 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      []Entry{entry},
		LeaderCommit: rf.commitIndex,
	}
	sent := make(chan bool, len(rf.peers))
	rf.log = append(rf.log, entry)
	rf.mu.Unlock()

	go func() {
		for pi := 0; pi < len(rf.peers); pi++ {
			if pi == rf.me {
				continue
			}

			go func(pi int) {
				// TODO: send
				_ = args
				reply := AppendEntriesReply{}
				_ = reply
			}(pi)
		}

		withMajority := false
		for i, confirmed := len(rf.peers)-1, 0; i >= 0; i-- {
			if <-sent {
				if confirmed++; confirmed < rf.majority {
					withMajority = true

					break
				}
			}
		}

		if !withMajority {
			// TODO: failed, do nothing?

			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		N := len(rf.log) - 1
		for ; N > 0; N-- {
			if rf.log[N].Term == rf.currentTerm && N > rf.commitIndex {
				greater := 0
				for _, mi := range rf.matchIndex {
					if mi >= N {
						if greater++; greater == rf.majority {
							rf.commitIndex = N

							return
						}
					}
				}
			}
		}
	}()

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

	// Your initialization code here (2A, 2B, 2C).

	rf.majority = calcMajority(len(peers))
	rf.state = follower
	rf.signalKill = make(chan struct{})
	rf.signalHeartbeat = make(chan hearbeatState)
	rf.signalElectionTimeout = make(chan electionTimeoutState)
	rf.signalElectionHalt = make(chan struct{})
	rf.applyCh = applyCh

	rf.log = []Entry{{}}
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(peers))
	for pi := 0; pi < len(rf.peers); pi++ {
		rf.nextIndex[pi] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.hearbeat()
	rf.stopHeartbeat()
	rf.electionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
