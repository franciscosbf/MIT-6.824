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

	electionTimeoutMin = time.Millisecond * 600
	electionTimeoutMax = time.Millisecond * 750
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
	for pi, peer := range rf.peers {
		if pi == rf.me {
			continue
		}

		go func(peer *labrpc.ClientEnd, pi int) {
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

			DPrintf("%v sent to %v AppendEntries RPC as heartbeat: %v", rf.me, pi, &args)

			reply := AppendEntriesReply{}

			if !peer.Call("Raft.AppendEntries", &args, &reply) {
				return
			}

			DPrintf("%v received from %v AppendEntries RPC as heartbeat reply: %v", rf.me, pi, &reply)

			if !reply.Success {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.revertToFollower(args.Term)
				}
				rf.mu.Unlock()
			}
		}(peer, pi)
	}
}

func (rf *Raft) hearbeat() {
	go func() {
		// timer := time.NewTimer(heartbeatTimeout)
		// timer := time.NewTimer(timeout)
		timer := time.After(heartbeatTimeout)

		for {
			select {
			case <-rf.signalKill:
				return
				// case <-timer.C:
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

			// if !timer.Stop() {
			// 	<-timer.C // drain channel
			// }

			// timer.Reset(heartbeatTimeout)
			timer = time.After(heartbeatTimeout)
		}
	}()
}

func (rf *Raft) stopHeartbeat() {
	rf.signalHeartbeat <- stopHeartbeat
}

func (rf *Raft) resumeHeartBeat() {
	rf.signalHeartbeat <- resumeHeartBeat
}

func (rf *Raft) fireVote() bool {
	atomic.StoreInt32(&rf.electing, 1)

	defer func() {
		atomic.StoreInt32(&rf.electing, 0)

		select {
		case <-rf.signalElectionHalt: // drain channel
		default:
		}
	}()

	DPrintf("%v started an election", rf.me)

	for {
	retry:
		rf.mu.Lock()
		rf.currentTerm++
		rf.state = candidate
		rf.mu.Unlock()

		accepted := make(chan struct{}, len(rf.peers))

		for pi, peer := range rf.peers {
			if pi == rf.me {
				continue
			}

			go func(peer *labrpc.ClientEnd, pi int) {
				rf.mu.Lock()
				lastLogIndex := len(rf.log) - 1
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  rf.log[lastLogIndex].Term,
				}
				rf.mu.Unlock()

				DPrintf("%v sent to %v RequestVote RPC: %v", rf.me, pi, &args)

				reply := RequestVoteReply{}

				if !peer.Call("Raft.RequestVote", &args, &reply) {
					return
				}

				DPrintf("%v received from %v RequestVote RPC reply: %v", rf.me, pi, &reply)

				if reply.VoteGranted {
					DPrintf("%v got vote granted from %v", rf.me, pi)

					accepted <- struct{}{}

					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.haltElection()

					rf.revertToFollower(args.Term)
				}
			}(peer, pi)
		}

		timer := time.After(genElectionTimeout())
		approved := 0

		for {
			select {
			case <-rf.signalElectionHalt:
				return false
			case <-accepted:
				if approved++; approved < rf.majority {
					continue
				}

				DPrintf("%v has become leader with a majority of the votes", rf.me)

				rf.mu.Lock()
				rf.state = leader
				rf.mu.Unlock()

				rf.fireHeartbeat()
				rf.resumeHeartBeat()

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
		// timer := time.NewTimer(timeout)
		timer := time.After(timeout)

		DPrintf("%v started election timeout with %v", rf.me, timeout)

		for {
			select {
			case <-rf.signalKill:
				return
				// case <-timer.C:
			case <-timer:
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
					continue // resume only when stopped
				case stopElectionTimeout:
					for {
						if state := <-rf.signalElectionTimeout; state == resumeElectionTimeout {
							break
						}
					}
				case resetElectionTimeout:
				}
			}

			// if !timer.Stop() {
			// 	<-timer.C // drain channel
			// }

			timeout := genElectionTimeout()
			// timer.Reset(timeout)
			timer = time.After(timeout)

			DPrintf("%v reseted election timeout with %v", rf.me, timeout)
		}
	}()
}

func (rf *Raft) stopElectionTimeout() {
	rf.signalElectionTimeout <- stopElectionTimeout
}

func (rf *Raft) resetElectionTimeout() {
	rf.signalElectionTimeout <- resetElectionTimeout
}

func (rf *Raft) resumeElectionTimeout() {
	rf.signalElectionTimeout <- resumeElectionTimeout
}

func (rf *Raft) haltElection() {
	if atomic.LoadInt32(&rf.electing) == 0 {
		return
	}

	select {
	case rf.signalElectionHalt <- struct{}{}:
	default:
	}
}

func (rf *Raft) isHeartbeat(args *AppendEntriesArgs) bool {
	return len(args.Entries) == 0
}

func (rf *Raft) revertToFollower(term int) {
	rf.stopHeartbeat()

	DPrintf("%v went back to follower state at term %v", rf.me, term)

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

func (rf *Raft) RequestVote(
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) {
	// Your code here (2A, 2B).

	DPrintf("%v received RequestVote RPC: %v", rf.me, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		reply.Term = rf.currentTerm
	}()

	if args.Term >= rf.currentTerm {
		rf.evaluateTermOnRPC(args.Term)

		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			len(rf.log)-1 == args.LastLogIndex &&
			rf.log[args.LastLogIndex].Term == args.LastLogTerm {

			reply.VoteGranted = true

			rf.votedFor = args.CandidateId
		}
	}
}

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	DPrintf("%v received AppendEntries RPC: %v", rf.me, args)

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

			if rf.isHeartbeat(args) {
				return
			}

			// TODO: remaining (steps 3., 4. and 5.)
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// func (rf *Raft) sendRequestVote(
// 	server int,
// 	args *RequestVoteArgs,
// 	reply *RequestVoteReply,
// ) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

	nPeers := len(peers)

	rf.majority = calcMajority(nPeers)
	rf.state = follower
	rf.signalKill = make(chan struct{})
	rf.signalHeartbeat = make(chan hearbeatState, nPeers)
	rf.signalElectionTimeout = make(chan electionTimeoutState, nPeers)
	rf.signalElectionHalt = make(chan struct{}, 1)

	rf.log = []Entry{{}}
	rf.votedFor = -1
	rf.nextIndex = make([]int, nPeers)
	for i := 0; i < nPeers; i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, nPeers)

	rf.hearbeat()
	rf.stopHeartbeat()
	rf.electionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
