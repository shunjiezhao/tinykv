package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type stepFunc func(r *Raft, m pb.Message) error

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	log.Infof("%s receive msg: %s", r.info(), MessageStr(r, m))

	switch {
	case m.Term == 0: //local
	case r.Term > m.Term: // 过时的
		log.Debug("out dated")
		return nil
	case r.Term < m.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}

	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.Lead == None)
		log.Errorf("can vote: %v, log is old: %v", canVote, r.myLogIsOld(m.LogTerm, m.Index))
		if canVote && r.myLogIsOld(m.LogTerm, m.Index) {
			r.electionElapsed = 0
			r.Vote = m.From
			r.send(r.NewRespVoteMsg(m.From, false))
		} else {
			r.send(r.NewRespVoteMsg(m.From, true))
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.bckstHeart()
		}

	default:
		err := r.step(r, m)
		if err != nil {
			log.Errorf("")
		}
	}
	// Your Code Here (2A).
	return nil
}
func stepFollower(r *Raft, m pb.Message) error {

	log.Debugf("receive msg: %s", MessageStr(r, m))
	if r.State != StateFollower {
		log.Panicf("%s", r.info())
	}
	r.becomeFollower(m.Term, m.From)
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)

	}
	return nil
}
func stepCandidate(r *Raft, m pb.Message) error {
	if r.State != StateCandidate {
		log.Panicf("%s", r.info())
	}

	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped

	case pb.MessageType_MsgBeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)

	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, m.MsgType, !m.Reject) //Reject = true stand not vote
		log.Infof("%s has received %s %d votes and %d vote rejections result: %v", r.info(), MessageStr(r, m), gr, rj, res)

		switch res {
		case VoteWon:
			if r.State == StateCandidate {
				r.becomeLeader()
				r.bcastAppend(false)
			}
		case VoteLost:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None)
		}

	}
	return nil
}
func stepLeader(r *Raft, m pb.Message) error {
	if r.State != StateLeader {
		log.Panicf("%s", r.info())
	}

	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.handleProse(m)
	case pb.MessageType_MsgBeat:
		r.Visit(func(idx int, to uint64) {
			r.sendHeartbeat(to)
		}, false)
	case pb.MessageType_MsgAppendResponse:
		// 1. handle reject
		log.Debugf("get from %d reject: %v", m.From, m.Reject)
		pr := r.Prs[m.From]
		if m.Reject == false {
			pr.Next = max(pr.Next, m.Index+1)
			pr.Match = max(pr.Match, m.Index)
			log.Infof("%s has received %s index: %d", r.info(), MessageStr(r, m), m.Index)
			// 2. update commit
			oldCommit := r.RaftLog.committed
			newCommit := r.updateCommit()
			if oldCommit < newCommit {
				log.Debugf("get commit :%d", newCommit)
				r.bcastAppend(false)
			}

		} else {
			pr.Next--
			log.Infof("reject")
		}

	case pb.MessageType_MsgHeartbeatResponse:
		// 1. 追赶日志
		r.sendAppend(m.From)
	}

	return nil
}
