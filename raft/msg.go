package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) NewHeartbeatMsg(to uint64) pb.Message {
	if r.State != StateLeader {
		log.Panicf("you state %s not leader", r.Info())
	}
	return pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  util.RaftInvalidIndex,
	}
}
func (r *Raft) NewRespHeartbeatMsg(to uint64) pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		Commit:  r.RaftLog.committed,
	}
}

func (r *Raft) NewRequestVoteMsg(to uint64) pb.Message {
	if r.State != StateCandidate {
		log.Panicf("you state %s not candidate", r.Info())
	}
	var LastLog = r.RaftLog.LastLog()
	return pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		LogTerm: LastLog.Term,
		Index:   LastLog.Index,
	}
}
func (r *Raft) NewRespVoteMsg(to uint64, reject bool) pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Reject:  reject,
	}
}

func (r *Raft) NewAppendMsg(to uint64) pb.Message {
	if r.State != StateLeader {
		log.Panicf("you state %s not leader", r.Info())
	}
	pr, ok := r.Prs[to]
	if !ok {
		log.Panicf("don't have this node %d ?", pr)
	}

	log.Infof("%s send to %d PR{%d:%d}", r.Info(), to, pr.Match, pr.Next)
	prevLog, err := r.RaftLog.entryAt(pr.Next - 1)

	if err != nil {
		if errors.Is(err, ErrCompacted) {
			log.Infof("%s send to %d {%d:%d} compacted", r.Info(), to, pr.Next, r.RaftLog.LastIndex())
			storage := r.RaftLog.storage
			snapshot, err := storage.Snapshot()
			if err != nil {
				if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
					log.Infof("%s send to %d {%d:%d} snapshot temporarily unavailable", r.Info(), to, pr.Next, r.RaftLog.LastIndex())
					return pb.Message{
						MsgType: pb.MessageType_MsgAppend,
						To:      to,
						LogTerm: r.RaftLog.LastTerm(),
						Index:   r.RaftLog.LastIndex(),
						Commit:  r.RaftLog.committed,
					}
				}
				log.Panicf("%s send to %d {%d:%d} snapshot error %s", r.Info(), to, pr.Next, r.RaftLog.LastIndex(), err)
			}
			return pb.Message{
				MsgType:  pb.MessageType_MsgSnapshot,
				To:       to,
				Snapshot: &snapshot,
			}
		}
	}
	log.Infof("%s send log to %d {%d:%d}", r.Info(), to, pr.Next, r.RaftLog.LastIndex())
	if prevLog == nil {
		log.Panicf("%s send log to %d {%d:%d} prevLog is nil", r.Info(), to, pr.Next, r.RaftLog.LastIndex())
	}

	return pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Index:   prevLog.Index,
		LogTerm: prevLog.Term,
		Commit:  r.RaftLog.committed,
		Entries: r.RaftLog.slice(pr.Next, r.RaftLog.LastIndex()),
	}
}
func (r *Raft) NewRespAppendMsg(to, index uint64, reject bool) pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		Reject:  reject,
		Index:   index,
	}
}

func (r *Raft) NewTimeOutMsg(to uint64) pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		Term:    r.Term,
	}
}

func (r *Raft) NewHupMsg() pb.Message {
	if r.State != StateFollower {
		log.Panicf("you state %s not leader", r.Info())
	}
	return pb.Message{
		MsgType: pb.MessageType_MsgHup,
	}
}
func (r *Raft) SendSnapshot(to uint64) {
	if r.State != StateLeader {
		log.Panicf("you state %s not leader", r.Info())
	}
	storage := r.RaftLog.storage
	pr := r.Prs[to]
	snapshot, err := storage.Snapshot()
	if err != nil {
		if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
			log.Infof("%s send to %d {%d:%d} snapshot temporarily unavailable", r.Info(), to, pr.Next, r.RaftLog.LastIndex())
			r.send(r.NewAppendMsg(to))
			return
		}
		log.Panicf("%s send to %d {%d:%d} snapshot error %s", r.Info(), to, pr.Next, r.RaftLog.LastIndex(), err)
	}
	log.Infof("%s send to %d snapshot", r.Info(), to)
	r.send(pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		Snapshot: &snapshot,
	})
}
