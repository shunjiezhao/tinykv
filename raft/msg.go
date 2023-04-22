package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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

func (r *Raft) NewResponseVoteMsg(to uint64, reject bool) pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Reject:  reject,
	}
}
