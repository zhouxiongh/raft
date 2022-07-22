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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A

	state State
	//appendEntryCh chan ApplyMsg
	heartbeat    time.Duration
	electionTime time.Time

	// Persistent state on all servers
	currentTerm int
	votedFor    int

	// 2B
	log Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	// Reinitialized after election
	// nextIndex 是领导将发送给该跟随者的下一个日志条目的索引
	nextIndex []int
	// matchIndex 对于每个服务器，已知在服务器上被复制的最高日志条目的索引
	matchIndex []int

	applyCh chan ApplyMsg
	// 在 commitIndex 更新后应用于状态机
	applyCond *sync.Cond
}

type Log []Entry

type Entry struct {
	Command interface{}
	Index   int
	Term    int
}

func makeEmptyLog() []Entry {
	return make([]Entry, 0)
}

type State int32

const (
	Follower State = iota
	Leader
	Candidate
)

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := rf.state == Leader
	term := rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		// error
	}
	if err := e.Encode(rf.votedFor); err != nil {
		// error
	}
	if err := e.Encode(rf.log); err != nil {
		// error
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	//var term = rf.currentTerm
	//var vote = rf.votedFor
	//var log = rf.log
	if err := d.Decode(&rf.currentTerm); err != nil {
		// error
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		// error
	}
	if err := d.Decode(&rf.log); err != nil {
		// error
	}
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// AppendEntriesArgs Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
type AppendEntriesArgs struct {
	// 2A
	Term     int
	LeaderId int
	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dInfo, "AppendEntries: %+v", args)
	reply.Term = rf.currentTerm
	reply.Success = false
	// All servers rule2
	if args.Term > rf.currentTerm {
		DPrintf(dLog, "... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
		//reply.Term = rf.currentTerm
		//return
	}
	// RPC rule1
	if args.Term < rf.currentTerm {
		return
	}
	// RPC rule 2
	if rf.log.last().Index < args.PrevLogIndex {
		return
	}
	// candidate rule3 如果收到来自新领导的AppendEntries RPC：转换为跟随者
	if rf.state == Candidate {
		rf.becomeFollower(args.Term)
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		xTerm := rf.log[args.PrevLogIndex].Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log[xIndex-1].Term != xTerm {
				break
			}
		}
		return
	}

	for idx, e := range args.Entries {
		// RPC rule 4
		if e.Index <= rf.log.last().Index && e.Term != rf.log[e.Index].Term {
			rf.log[e.Index] = e
		}
		// RPC rule 3
		if e.Index > rf.log.last().Index {
			rf.log = append(rf.log, args.Entries[idx:]...)
			rf.persist()
			break
		}
	}

	// RPC rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.log.last().Index,
			args.LeaderCommit)
		rf.apply()
	}
	rf.resetElectionTimer()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(sererId int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	ok := rf.peers[sererId].Call("Raft.AppendEntries", args, reply)
	return ok
}

// send append entry to all peers
func (rf *Raft) appendEntries() {
	// Your code here 2A,2B
	for peer := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}

		nextIndex := rf.nextIndex[peer]
		if nextIndex <= 0 {
			nextIndex = 1
		}
		prevLog := rf.log[nextIndex-1]
		entries := rf.log[nextIndex:]
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		go rf.leaderSendEntries(peer, &args)
	}
}

// send append entry and handle reply
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// All servers rule2
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if !reply.Success {
		return
	}
	// update next index and match index
	if args.Term == rf.currentTerm {
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.matchIndex[serverId] = max(match, rf.matchIndex[serverId])
			rf.nextIndex[serverId] = max(next, rf.nextIndex[serverId])
		}
		//log needed copy to majority peers before commit
		rf.leaderCommit()
	}

	// increase commit index directly and apply command
	//rf.commitIndex++
	//rf.apply()

}

func (rf *Raft) leaderCommit() {
	if rf.state != Leader {
		return
	}
	for n := rf.commitIndex + 1; n <= rf.log.last().Index; n++ {
		// check term
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		counter := 1
		for peerId := 0; peerId < len(rf.peers); peerId++ {
			// increase counter
			if peerId != rf.me && rf.matchIndex[peerId] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				rf.apply()
				break
			}
		}
	}
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here
	// 2A
	Term        int
	CandidateId int
	// 2B
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// All server rule2
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	// rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// rule 2
	if (rf.votedFor == args.CandidateId || rf.votedFor == -1) &&
		(args.LastLogTerm > rf.log.last().Term ||
			args.LastLogTerm == rf.log.last().Term &&
				args.LastLogIndex >= rf.log.last().Index) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

//
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.log.last().Index + 1
	term, isLeader := rf.currentTerm, rf.state == Leader

	if !isLeader {
		return -1, term, isLeader
	}
	entry := Entry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	return index, term, isLeader
}

//func (log Log) append(entry Entry) {
//	log = append(log, entry)
//}

func (log Log) last() Entry {
	return log[len(log)-1]
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// (2A)
		time.Sleep(rf.heartbeat)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.appendEntries()
		}
		if time.Now().After(rf.electionTime) {
			rf.leaderElection()
		}
		rf.mu.Unlock()

	}
}

// send msg to apply chan
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		// all servers rule1
		if rf.commitIndex > rf.lastApplied && rf.log.last().Index > rf.lastApplied {
			rf.lastApplied++
			DPrintf(dLog, "commit index = %d, last applied = %d",
				rf.commitIndex, rf.lastApplied)
			// send apply msg to chan
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

// wake up apply
func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

// 每个候选人在选举开始时重新启动其随机的选举超时，并等待超时过后再开始下一次选举
func (rf *Raft) leaderElection() {
	DPrintf(dTimer, "%d start election\n", rf.me)
	rf.currentTerm++
	rf.state = Candidate
	rf.resetElectionTimer()
	rf.votedFor = rf.me
	rf.persist()
	// 2C

	// init RequestVote args
	term := rf.currentTerm
	voteArg := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.last().Index,
		LastLogTerm:  rf.log.last().Term,
	}

	var becomeLeader sync.Once
	voteCounter := 1
	for serverId := range rf.peers {
		if serverId != rf.me {
			go rf.candidateRequestVote(serverId, voteArg, &voteCounter, &becomeLeader)
		}
	}
}

func (rf *Raft) becomeFollower(term int) {
	DPrintf(dLog, "becomes Follower with term=%d, log=%v", term, rf.log)
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
	rf.resetElectionTimer()
}

func (rf *Raft) candidateRequestVote(server int, args *RequestVoteArgs,
	voteCounter *int, becomeLeader *sync.Once) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// handle reply
	// All Servers rule2
	if reply.Term > args.Term {
		// fmt.Printf("%d reply term > args term", server)
		rf.becomeFollower(reply.Term)
		return
	}
	if reply.Term < args.Term {
		// fmt.Printf("%d reply term < args term", server)
		return
	}
	if !reply.VoteGranted {
		// fmt.Printf("%d refuse vote to %d\n", server, rf.me)
		return
	}
	DPrintf(dVote, "%d vote to %d\n", server, rf.me)
	*voteCounter++
	// (a)它赢得了选举
	if *voteCounter > len(rf.peers)/2 && rf.state == Candidate &&
		rf.currentTerm == args.Term {
		becomeLeader.Do(func() {
			DPrintf(dLeader, "S%d become leader!", rf.me)
			rf.state = Leader
			rf.appendEntries()
		})
	}

}

// resetElectionTimer
// 选举超时时间是从一个固定的时间间隔中随机选择的(当前时间+150~300ms)
func (rf *Raft) resetElectionTimer() {
	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)
	duration := time.Duration(r.Intn(150)+150) * time.Millisecond
	rf.electionTime = time.Now().Add(duration)
	DPrintf(dTimer, " election time ", rf.electionTime)
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	// 2A
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeat = 100 * time.Millisecond
	rf.resetElectionTimer()

	// 2B
	rf.log = makeEmptyLog()
	rf.log = append(rf.log, Entry{Command: -1, Index: 0, Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// 2C

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start apply goroutine to apply command to state machine
	go rf.applier()

	return rf
}
