package raft

import (
	"bytes"

	"6.824/labgob"
)

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// log.Printf("[%d] snapshot index:[%d] start +++++", rf.me, index)
	rf.mu.Lock()
	// log.Printf("[%d] snapshot index:[%d] lock ---------", rf.me, index)
	// defer rf.mu.Unlock()

	if index <= rf.log[0].Offset {
		rf.mu.Unlock()
		return
	}
	var trimedLog []Entry
	if index < rf.getLastLogIndex() {
		trimedLog = rf.log[rf.IndexToPos(index+1):]
	} else {
		// 上层保证不设置index超出范围吗
		trimedLog = make([]Entry, 0)
	}

	rf.log[0].Term = rf.log[rf.IndexToPos(index)].Term
	rf.log[0].Offset = index

	rf.log = rf.log[:1]
	rf.log = append(rf.log, trimedLog...)
	// bug，一定要把persister放在锁里面，不然rf结构有可能发生变化
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	// log.Printf("[%d] snapshot index:[%d]", rf.me, index)
	Debug(dLog, "S%d snapshot index:[%d]", rf.me, index)
	rf.mu.Unlock()
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotResponse struct {
	Term      int
	NextIndex int
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer log.Printf("install snapshot start over")
	response.Term = rf.currentTerm
	response.NextIndex = 0
	if request.Term < rf.currentTerm {
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.state = Follower
		rf.persist()

	}
	rf.electionTimer.Reset(ElectionTimeout())

	// outdated snapshot
	// if request.LastIncludedIndex <= rf.commitIndex {
	// 	return
	// }
	if request.LastIncludedIndex < rf.log[0].Offset {
		// log.Printf("*****error implementation for nextindex:*****")
		response.NextIndex = rf.log[0].Offset + 1
		return
	}

	// log.Printf("[%d] install snapshot start logoffset[%d],LastIncludedIndex[%d]", rf.me, rf.log[0].Offset, request.LastIncludedIndex)
	Debug(dLog, "S%d install snapshot start logoffset[%d],LastIncludedIndex[%d]", rf.me, rf.log[0].Offset, request.LastIncludedIndex)
	// rf.applyCh <- ApplyMsg{
	// 	CommandValid:  false,
	// 	SnapshotValid: true,
	// 	Snapshot:      request.Snapshot,
	// 	SnapshotTerm:  request.LastIncludedTerm,
	// 	SnapshotIndex: request.LastIncludedIndex,
	// }

	var trimedLog []Entry
	if request.LastIncludedIndex < rf.getLastLogIndex() && rf.log[rf.IndexToPos(request.LastIncludedIndex)].Term == request.LastIncludedTerm {
		trimedLog = rf.log[rf.IndexToPos(request.LastIncludedIndex+1):]
	} else {
		trimedLog = make([]Entry, 0)
	}
	rf.log[0].Term = request.LastIncludedTerm
	rf.log[0].Offset = request.LastIncludedIndex
	rf.log = rf.log[:1]
	rf.log = append(rf.log, trimedLog...)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), request.Snapshot)

	// 选择较大的commitIndex 默认snapshot一定commit了
	// if request.LastIncludedIndex > rf.commitIndex {
	// 	rf.commitIndex = request.LastIncludedIndex
	// }

	// 为了通过测试，当commit小于快照时不提交
	if rf.lastApplied <= rf.log[0].Offset {
		rf.waitingSnapshot = request.Snapshot
		rf.waitingIndex = request.LastIncludedIndex
		rf.WaitingTerm = request.LastIncludedTerm
		rf.lastApplied = request.LastIncludedIndex
		// log.Printf("[%d] install sanp change lastApplied [%d]", rf.me, rf.lastApplied)
		rf.applyCond.Signal()
		// log.Printf("[%d] commit snapshot,lastApplied:[%d],offset:[%d]", rf.me, rf.lastApplied, rf.log[0].Offset)
		// go func() {
		// 	rf.applyCh <- ApplyMsg{
		// 		CommandValid:  false,
		// 		SnapshotValid: true,
		// 		Snapshot:      request.Snapshot,
		// 		SnapshotTerm:  request.LastIncludedTerm,
		// 		SnapshotIndex: request.LastIncludedIndex,
		// 	}
		// 	rf.mu.Lock()
		// 	log.Printf("------[%d]commit snapshot lastInclude[%d]--------", rf.me, request.LastIncludedIndex)
		// 	if request.LastIncludedIndex > rf.lastApplied {
		// 		rf.lastApplied = request.LastIncludedIndex
		// 	}
		// 	rf.mu.Unlock()
		// }()

		// // rf.commitIndex = rf.log[0].Offset
		// rf.lastApplied = rf.log[0].Offset
		// log.Printf("[%d] commit snapshot:change commitIndex:[%d],lastApplied[%d]", rf.me, rf.commitIndex, rf.lastApplied)
	}

}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}
