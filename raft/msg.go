package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	log "github.com/sirupsen/logrus"
)

func (r *Raft) NewRequestVoteMsg(to uint64) pb.Message {
	var LastLog = r.RaftLog.LastLog()
	return pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: LastLog.Term,
		Index:   LastLog.Index,
	}
}
func (r *Raft) NewHeartbeatMsg(to, commit uint64) pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	}
}

func (r *Raft) NewResponseVoteMsg(to uint64, reject bool) pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		To:      to,
		From:    r.id,
		Reject:  reject,
	}
}

func (r *Raft) NewAppendMsg(to uint64) pb.Message {
	pr, ok := r.Prs[to]
	if !ok {
		log.Panicf("don't have this node %d ?", pr)
	}
	if pr.Next-1 == r.RaftLog.LastIndex() {
		log.Panicf("you should check you want to send")
	}

	prevLog, err := r.RaftLog.entryAt(pr.Next - 1)

	if err != nil {
		// todo: send snap?
		log.Panic(r.info(), "send to ", to, " can not get next ", err)
	}
	log.Info(pr.Next, r.RaftLog.LastIndex())

	return pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   prevLog.Index,
		LogTerm: prevLog.Term,
		Commit:  r.RaftLog.committed,
		Entries: r.RaftLog.slice(pr.Next, r.RaftLog.LastIndex()),
	}
}
func (r *Raft) NewResponseAppendMsg(to, index uint64, reject bool) pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		To:      to,
		From:    r.id,
		Reject:  reject,
		Index:   index,
	}
}
