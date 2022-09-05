package raft

import (
	"math/rand"
	"time"
)

const (
	// 更改成一秒
	electionTimeout = 1 * time.Second
)

func ElectionTimeout() time.Duration {
	ms := rand.Int63() % 300
	timeout := time.Duration(ms) * time.Millisecond
	timeout += electionTimeout
	return timeout
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 图2第一点：如果参数中的term小于currentTerm,直接返回false
	if args.Term < rf.currentTerm {
		// 返回后的currentTerm应该如何利用
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 图2第二点：如果votedFor是null或者CandidateId，并且candidate的log更加up_to_date，那么投票成功
	if ((rf.votedFor == -1 || rf.votedFor == args.CandidateId) || args.Term > rf.currentTerm) && rf.IsUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// log.Printf("%d votefor %d,argsterm:%d,argIndex:%d,rflogterm:%d", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.getLastLogTerm())
		rf.votedFor = args.CandidateId
		// *************persist
		rf.persist()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		// 投票后重置election time，否则（既更改了term，又产生了新的选举，则新的leader处于前一个leadeer还没发送heartbeats之前，--->则汇聚ote时，还需考虑term）
		rf.electionTimer.Reset(ElectionTimeout())
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	// 图2：对于所有server的准则。此时votedFor指向什么
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.persist()
	}
}

// 如果遇到server不可达，丢包等情况，Call会等待一段时间并返回false，如果是server端处理需要长时间，
// 那么会一直等待，所以应该避免在发送rpc时加锁，以免发生死锁
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) electionL() {
	rf.mu.Lock()
	// log.Printf("%d start election at term[%d]", rf.me, rf.currentTerm)
	Debug(dTimer, "S%d start elecion at term[%d]", rf.me, rf.currentTerm)
	rf.electionTimer.Reset(ElectionTimeout())
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	// persist
	rf.persist()

	term := rf.currentTerm
	canId := rf.me
	// 添加2B需要的参数
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	// log.Printf("%d start election log:%v,lastLogIndex:%d,lastLogTerm%d", rf.me, rf.log, lastLogIndex, lastLogTerm)
	rf.mu.Unlock()

	votes := 1
	done := false

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {

			args := RequestVoteArgs{}
			args.Term = term
			args.CandidateId = canId
			args.LastLogIndex = lastLogIndex
			args.LastLogTerm = lastLogTerm
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok && reply.VoteGranted {
				rf.mu.Lock()
				// log.Printf("[%d] get vote  from [%d] at term[%d]", rf.me, server, args.Term)
				votes++
				if votes > len(rf.peers)/2 && done == false && rf.currentTerm == term {
					done = true
					rf.becomeLeader()
				}

				rf.mu.Unlock()
			}

		}(server)
	}

}

func (rf *Raft) becomeLeader() {
	// log.Printf("[%d] become leader", rf.me)
	Debug(dLeader, "S%d become leader", rf.me)
	rf.state = Leader
	rf.heartbeatTimer.Reset(heartbeatTimeout())
	// 图2：需要对matchIndex和nextIndex重新初始化
	rf.matchIndex = make([]int, len(rf.peers))
	for server, _ := range rf.peers {
		if server != rf.me {
			rf.nextIndex[server] = rf.getLastLogIndex() + 1
		}
	}
	// *************persist
	rf.persist()
}
