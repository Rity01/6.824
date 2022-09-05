package raft

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// 定义state
type State string

// 定义state和时间常量
const (
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
)

//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 修改为条件变量通知，而不是使用循环不断判断。
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	state        State
	electionTime time.Time

	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	waitingSnapshot []byte
	waitingIndex    int
	WaitingTerm     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		//   error...
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// 立即返回，不用等待是否真的append log entry成功
// index是假设添加成功，command将出现的位置，term是当前的term
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	// 此Raft peer有可能是过期的leader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
	} else {
		// 添加到log当中
		entry := Entry{}
		entry.Term = rf.currentTerm
		entry.Command = command
		rf.log = append(rf.log, entry)
		// log.Printf("[%d] start command,term[%d],index[%d],isleader[%t]", rf.me, rf.currentTerm, rf.getLastLogIndex(), isLeader)
		Debug(dLog, "S%d start command,term[%d],index[%d],isleader[%t]", rf.me, rf.currentTerm, rf.getLastLogIndex(), isLeader)
		// *************persist
		rf.persist()
		// 不然3A过慢
		go rf.sendEntriesOrHeartHeat()
	}
	index = rf.getLastLogIndex()
	term = rf.currentTerm
	return index, term, isLeader
}

//
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

// 创建raft，包括初始化代码，会启动长期运行的gotoutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = 0

	// 什么时候调用stop
	rf.electionTimer = time.NewTimer(ElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(heartbeatTimeout())

	rf.log = make([]Entry, 1)
	rf.log[0].Term = 0
	rf.log[0].Offset = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// 添加commited logs
	go rf.applier()
	return rf
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		// leader此处应该怎么处理
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.electionTimer.Reset(ElectionTimeout())
			} else {
				go rf.electionL()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			// log.Printf("%d heartbeat", rf.me)
			if rf.state == Leader {
				go rf.sendEntriesOrHeartHeat()
				rf.heartbeatTimer.Reset(heartbeatTimeout())
			}
			rf.mu.Unlock()
		}

	}
}

// 条件变量通知的时候不在等待怎么办
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = 0
	if rf.lastApplied+1 <= rf.log[0].Offset {
		rf.lastApplied = rf.log[0].Offset
	}
	for !rf.killed() {
		if rf.waitingSnapshot != nil {
			// log.Printf("%d snapshot", rf.me)
			Debug(dInfo, "S%d snapshot", rf.me)
			am := ApplyMsg{}
			am.SnapshotValid = true
			am.Snapshot = rf.waitingSnapshot
			am.SnapshotIndex = rf.waitingIndex
			am.SnapshotTerm = rf.WaitingTerm

			rf.waitingSnapshot = nil

			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()
		} else if rf.lastApplied+1 <= rf.commitIndex && rf.lastApplied+1 <= rf.getLastLogIndex() && rf.lastApplied+1 > rf.log[0].Offset {
			rf.lastApplied += 1
			am := ApplyMsg{}
			am.CommandValid = true
			am.CommandIndex = rf.lastApplied
			am.Command = rf.log[rf.IndexToPos(rf.lastApplied)].Command
			rf.mu.Unlock()
			// log.Printf("----commit----")
			rf.applyCh <- am
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			// log.Printf("[%d] applier wake up,snapshot[%v],lastApplied[%d],LastLogIndex[%d]", rf.me, rf.waitingSnapshot, rf.lastApplied, rf.getLastLogIndex())
		}
	}
}

// func (rf *Raft) applier() {
// 	rf.mu.Lock()
// 	rf.lastApplied = 0
// 	if rf.lastApplied+1 <= rf.log[0].Offset {
// 		rf.lastApplied = rf.log[0].Offset
// 	}
// 	rf.mu.Unlock()
// 	for rf.killed() == false {
// 		rf.mu.Lock()
// 		if rf.lastApplied+1 <= rf.log[0].Offset {
// 			rf.lastApplied = rf.log[0].Offset
// 		}
// 		var lastApplied int
// 		var commitIndex int

// 		// 等待期间前面的log可能已经被压缩
// 		for rf.commitIndex <= rf.lastApplied {
// 			rf.applyCond.Wait()
// 			log.Printf("[%d] applier wake up", rf.me)
// 		}

// 		commitIndex = rf.commitIndex
// 		lastApplied = rf.lastApplied
// 		// log.Printf("[%d] entry size:[%v],commitIndex:[%v],lastApplied:[%d],offset:[%d]", rf.me, commitIndex-lastApplied, commitIndex, lastApplied, rf.log[0].Offset)
// 		entries := make([]Entry, commitIndex-lastApplied)
// 		copy(entries, rf.log[rf.IndexToPos(lastApplied+1):rf.IndexToPos(commitIndex+1)])
// 		log.Printf("[%d] entry:[%v],commitIndex:[%v],lastApplied:[%d],offset:[%d]", rf.me, entries, commitIndex, lastApplied, rf.log[0].Offset)
// 		// 把这个提前，防止和Installsnapshot冲突
// 		rf.lastApplied = commitIndex
// 		rf.mu.Unlock()

// 		// log.Printf("commit,[%v]", entries)
// 		Debug(dCommit, "S%d commit,size:%d,[%v] -> [%v],lastapplied:%d,commitIndex:%d", rf.me, len(entries), entries[0], entries[len(entries)-1], lastApplied, commitIndex)
// 		for i, entry := range entries {
// 			// rf.mu.Lock()
// 			// if lastApplied+i+1 <= rf.log[0].Offset {
// 			// 	break
// 			// }
// 			msg := ApplyMsg{}
// 			msg.CommandValid = true
// 			msg.CommandIndex = lastApplied + i + 1
// 			msg.Command = entry.Command
// 			rf.applyCh <- msg
// 			log.Printf("---------[%d]commit[%d]--------", rf.me, msg.CommandIndex)
// 			// rf.mu.Unlock()
// 		}
// 		// Debug(dCommit, "S%d commit,size:%d,[%v] -> [%v] -------OK", rf.me, len(entries), entries[0], entries[len(entries)-1])
// 		// rf.mu.Lock()

// 		// 有可能过时
// 		// rf.lastApplied = commitIndex

// 		// rf.mu.Unlock()
// 	}
// }
