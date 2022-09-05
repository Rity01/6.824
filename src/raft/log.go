package raft

// 定义日志结构
type Entry struct {
	Term    int
	Command interface{}
	Offset  int
}

func (rf *Raft) getLastLogIndex() int {
	// log下标从1开始
	return len(rf.log) - 1 + rf.log[0].Offset
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIndex()
	return rf.log[rf.IndexToPos(idx)].Term
}

func (rf *Raft) PosToIndex(pos int) int {
	return pos + rf.log[0].Offset
}

func (rf *Raft) IndexToPos(index int) int {
	return index - rf.log[0].Offset
}

// 判断参数中指明的Index和Term是否比rf本身的log uptodate
func (rf *Raft) IsUpToDate(LastLogTerm int, LastLogIndex int) bool {
	upToDate := false
	if LastLogTerm > rf.getLastLogTerm() {
		upToDate = true
	} else if LastLogTerm == rf.getLastLogTerm() {
		if LastLogIndex >= rf.getLastLogIndex() {
			upToDate = true
		}
	}
	return upToDate
}
