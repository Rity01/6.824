package raft

import (
	"sort"
	"time"
)

// AppendEntries RPC的参数
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// AppendEntries RPC的回复
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func heartbeatTimeout() time.Duration {
	timeout := time.Duration(50) * time.Millisecond
	return timeout
}

// 处理AppendEntries请求
func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("%d receive append from %d", rf.me, args.LeaderId)
	// 图2(1)：如果term小于currentTerm直接返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.electionTimer.Reset(ElectionTimeout())
	// 更新之前记住变量
	term := rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// heartbeat通知更新commitIndex，加上辅助的判断条件
	if args.LeaderCommit > rf.commitIndex && args.PrevLogIndex == rf.getLastLogIndex() && args.PrevLogTerm == rf.getLastLogTerm() && len(args.Entries) == 0 {
		if rf.getLastLogIndex() < args.LeaderCommit {
			rf.commitIndex = rf.getLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// 通知调用applier
		rf.applyCond.Signal()
	}

	// IndexToPos可能返回负数
	if rf.IndexToPos(args.PrevLogIndex) < 0 {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 图2(2)：如果log不包含在PrevLogIndex处term和PrevLogTerm匹配的log entry，返回false
	if /*len(args.Entries) > 0 &&*/ rf.getLastLogIndex() < args.PrevLogIndex || rf.log[rf.IndexToPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Term = term
		reply.Success = false
		// log.Printf("[%d] append from [%d] error,index:%d,term%d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		Debug(dInfo, "S%d append from [%d] error,index:%d,term%d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	// 图2(3)添加并同步log
	if len(args.Entries) > 0 {
		reply.Term = term
		reply.Success = true

		// bug,本来觉得可以省略写法，结果正确复制的log被删减（因为有一些rpc更晚到）
		for index, entry := range args.Entries {
			if len(args.Entries)+args.PrevLogIndex > rf.getLastLogIndex() || entry.Term != rf.log[rf.IndexToPos(args.PrevLogIndex+1+index)].Term {
				rf.log = rf.log[:rf.IndexToPos(args.PrevLogIndex+1)]
				rf.log = append(rf.log, args.Entries...)
				break
			}
		}
		// *************persist
		rf.persist()

		// log.Printf("[%d] <---- [%d],nowlog:[%d]entrysize：[%d]", rf.me, args.LeaderId, rf.getLastLogIndex(), len(args.Entries))
		// log.Printf("[%d] <---- [%d],nowlog:[%d]entrysize：[%d],entry[%v],lastapplied:%d", rf.me, args.LeaderId, rf.getLastLogIndex(), len(args.Entries), args.Entries, rf.lastApplied)
		Debug(dInfo, "S%d <---- [%d],nowlog:[%d]entrysize：[%d],entry[%v],lastapplied:%d", rf.me, args.LeaderId, rf.getLastLogIndex(), len(args.Entries), args.Entries, rf.lastApplied)
		// 修改日志后通知
		if args.LeaderCommit > rf.commitIndex {
			if rf.getLastLogIndex() < args.LeaderCommit {
				rf.commitIndex = rf.getLastLogIndex()
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			// 通知调用applier
			rf.applyCond.Signal()
		}
	}

}

// 发送心跳函数
func (rf *Raft) sendEntriesOrHeartHeat() {
	for server, _ := range rf.peers {
		if server != rf.me {
			// log.Printf("%d send --> server%d", rf.me, server)
			go rf.sendEntriesToOne(server)
		}
	}
}

func (rf *Raft) sendEntriesToOne(peer int) {
	// log.Printf("lastLogEntry [%d],nextIndex[%d]", lastLogEntry(rf.log), rf.nextIndex[peer])
	rf.mu.Lock()
	// bug在调用进入这个函数一瞬间，state可能变为follower，其他状态会有影响吗
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	// heartbeat也需要添加index和term比较，之前可能是基于时间来写applier，遗漏了这个bug
	args.PrevLogIndex = rf.getLastLogIndex()
	args.PrevLogTerm = rf.getLastLogTerm()
	reply := &AppendEntriesReply{}
	// log.Printf("lastLogEntry [%d],nextIndex[%d]", lastLogEntry(rf.log), rf.nextIndex[peer])
	// nextIndex初始化为last log index + 1，append成功后也要修改
	if rf.getLastLogIndex() < rf.nextIndex[peer] {
		// 发送心跳，entries没定义 或者和改为空
		go rf.peers[peer].Call("Raft.AppendEntriesHandler", args, reply)
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[peer] <= rf.log[0].Offset {
		request := &InstallSnapshotRequest{}
		request.LastIncludedIndex = rf.log[0].Offset
		request.LastIncludedTerm = rf.log[0].Term
		request.Term = rf.currentTerm
		request.LeaderId = rf.me
		request.Snapshot = rf.persister.snapshot
		response := &InstallSnapshotResponse{}
		lastlogidx := rf.getLastLogIndex()
		rf.mu.Unlock()
		// log.Printf("------[%d] at state[%v] matchIndex:%v sending snapshort to[%d]", rf.me, rf.state, rf.matchIndex, peer)
		ok := rf.peers[peer].Call("Raft.InstallSnapshot", request, response)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if ok {
			// 如果snapshort比接受方大，OK，但是如果更小呢？？
			// 如果有晚到的，也要做判断?
			if response.Term <= args.Term {
				if response.NextIndex != 0 && response.NextIndex > rf.nextIndex[peer] {
					// bug 此处应该是加1
					rf.nextIndex[peer] = lastlogidx + 1
					if response.NextIndex < lastlogidx+1 {
						rf.nextIndex[peer] = response.NextIndex
					}
					// log.Printf("[%d] send to[%d],nextIndex[%d],responseIndex[%d]", rf.me, peer, rf.nextIndex[peer], response.NextIndex)
					rf.matchIndex[peer] = rf.log[0].Offset
				} else if response.NextIndex == 0 {
					rf.nextIndex[peer] = rf.log[0].Offset + 1
					rf.matchIndex[peer] = rf.log[0].Offset
				}

				// log.Printf("------[%d] at state[%v] matchIndex:%v after send snapshort to[%d]", rf.me, rf.state, rf.matchIndex, peer)

				// 更新commitIndex
				index := rf.commitIndexFunc()
				// 不明白为什么要加log[N].term == currentTerm这个限制 --->figure8
				if index > rf.commitIndex && rf.log[rf.IndexToPos(index)].Term == rf.currentTerm {
					rf.commitIndex = index
				}
				// log.Printf("[%d] commitindex:[%d]", args.LeaderId, rf.commitIndex)
			} else {
				if response.Term > args.Term {
					rf.currentTerm = reply.Term
					// log.Printf("[%d] change term to [%d]", rf.me, rf.currentTerm)
					rf.state = Follower
					// log.Printf("[%d] become Follower", rf.me)
					// *************persist
					rf.persist()
				}
			}
		}
		return
	}

	// 否则发送entry，从rf.nextIndex[peer]开始
	// entry定义成copy，go语言语法查询
	// 忘记给snapshot加锁了，但是就算使用copy结果依然有问题！！！
	// log.Printf("[%d]nextIndex:%v", rf.me, rf.nextIndex)
	// entry := rf.log[rf.IndexToPos(rf.nextIndex[peer]):]
	temp := make([]Entry, len(rf.log))
	copy(temp, rf.log)
	// log.Printf("-----log len[%d],temp len[%d],idx:[%d],offset:[%d],nextIndex:[%d]", len(rf.log), len(temp), rf.IndexToPos(rf.nextIndex[peer]), rf.log[0].Offset, rf.nextIndex[peer])
	entry := temp[rf.IndexToPos(rf.nextIndex[peer]):]
	args.Entries = entry
	// PrevLogIndex随着nextIndex而变化
	args.PrevLogTerm = rf.log[rf.IndexToPos(rf.nextIndex[peer])-1].Term
	args.PrevLogIndex = rf.nextIndex[peer] - 1
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.AppendEntriesHandler", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			// rf.nextIndex[peer] = lastLogEntry(rf.log) + 1
			// matchIndex用于更新commitIndex
			// rf.matchIndex[peer] = lastLogEntry(rf.log)
			// bug 正确做法
			// 如果过时的reply更晚到？
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1

			// log.Printf("[%d] recerive from [%d],log:[%d],nextIndex:%v,matchIndex:%v", rf.me, peer, rf.getLastLogIndex(), rf.nextIndex, rf.matchIndex)
			Debug(dInfo, "S%d recerive from [%d],log:[%d],nextIndex:%v,matchIndex:%v", rf.me, peer, rf.getLastLogIndex(), rf.nextIndex, rf.matchIndex)
			// 更新commitIndex
			index := rf.commitIndexFunc()

			// 不加log[N].term == currentTerm这个限制 --->figure8
			if index > rf.commitIndex && rf.log[rf.IndexToPos(index)].Term == rf.currentTerm {
				rf.commitIndex = index
				// 通知调用applier
				// log.Printf("leader decide to commit: index[%d]", index)
				Debug(dInfo, "S%d leader decide to commit: index[%d]", rf.me, index)
				rf.applyCond.Signal()
			}

		} else {
			if reply.Term > args.Term {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
			} else {
				// 此处应该有个时间问题，如果先发送的晚接收，nextIndex应该怎么修改,此处逻辑不够简洁
				// log.Printf("nextIndex change:%d-->%d", rf.nextIndex[peer], args.PrevLogIndex)
				if args.PrevLogIndex == rf.log[0].Offset {
					rf.nextIndex[peer] = 1
					return
				}
				if rf.nextIndex[peer] == args.PrevLogIndex+1 {
					// rf.nextIndex[peer] = 1 + rf.log[0].Offset
					// rf.nextIndex[peer] = args.PrevLogIndex
					rf.nextIndex[peer] = 1 + rf.log[0].Offset
				}
			}
		}
	}

}

func (rf *Raft) commitIndexFunc() int {
	major := len(rf.peers) / 2
	// 把rf.me的matchIndex设置
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	temp := make([]int, len(rf.matchIndex))
	copy(temp, rf.matchIndex)
	sort.Ints(temp)
	// log.Printf("matchIndex:%v", temp)
	return temp[major]
}
