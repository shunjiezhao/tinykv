package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type stepFunc func(r *Raft, m pb.Message) error

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	log.Debugf("%s receive msg: %s", r.Info(), MessageStr(r, m))

	switch {
	case m.Term == 0: //local
	case r.Term > m.Term: // 过时的
		log.Debug("out dated")
		return nil
	case r.Term < m.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			log.Infof("append")
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
	case pb.MessageType_MsgPropose:
		// if leader want to transfer
		if r.State != StateLeader || r.leadTransferee != None {
			return ErrProposalDropped
		}
		r.handleProse(m)
		r.bcastAppend(false, false)
	default:
		err := r.step(r, m)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	// Your Code Here (2A).
	return nil
}
func stepFollower(r *Raft, m pb.Message) error {
	log.Debugf("receive msg: %s", MessageStr(r, m))
	if r.State != StateFollower {
		log.Panicf("%s", r.Info())
	}
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgTimeoutNow:
		// need to hup
		if r.Alive() {
			mustBeNil(r.Step(r.NewHupMsg()))
		}
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.Lead
		r.send(m)
	}
	return nil
}
func stepCandidate(r *Raft, m pb.Message) error {
	if r.State != StateCandidate {
		log.Panicf("%s", r.Info())
	}

	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped

	case pb.MessageType_MsgBeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)

	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, m.MsgType, !m.Reject) //Reject = true stand not vote
		log.Infof("%s has received %s %d votes and %d vote rejections result: %v", r.Info(), MessageStr(r, m), gr, rj, res)

		switch res {
		case VoteWon:
			if r.State == StateCandidate {
				r.becomeLeader()
				r.bcastAppend(false, false)
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
		log.Panicf("%s", r.Info())
	}

	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bckstHeart()
	case pb.MessageType_MsgAppendResponse:
		// 1. handle reject
		log.Debugf("get from %d reject: %v", m.From, m.Reject)
		pr := r.Prs[m.From]
		if m.Reject == false {
			pr.Next = max(pr.Next, m.Index+1)
			pr.Match = max(pr.Match, m.Index)
			log.Infof("%s has received %s index: %d", r.Info(), MessageStr(r, m), m.Index)
			// 2. update commit
			oldCommit := r.RaftLog.committed
			newCommit := r.updateCommit()
			if oldCommit < newCommit {
				log.Debugf("get commit :%d", newCommit)
				r.bcastAppend(false, true)
			}
			if r.leadTransferee == m.From && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
				log.Debugf("%s sent MsgTimeoutNow to %d because already up-to-date", r.Info(), m.From)
				r.sendTimeoutNow(m.From)
				r.heartbeatElapsed = 0 // reset heartbeatElapsed
			}
		} else {
			pr.Next--
			log.Infof("rejectLog")
		}
		r.sendAppend(m.From, false)

	case pb.MessageType_MsgHeartbeatResponse:
		// 1. 追赶日志
		r.sendAppend(m.From, false)

	case pb.MessageType_MsgTransferLeader:
		if _, ok := r.Prs[m.From]; !ok {
			log.Warnf("%s no progress of %d", r.Info(), m.From)
			return ErrProposalDropped // not in prs
		}
		newTransfer, oldTransfer := m.From, r.leadTransferee
		if oldTransfer != None {
			if newTransfer == oldTransfer {
				// transfer is in progress
				log.Warnf("%s leader transfer is in progress, ignore %s", r.Info(), MessageStr(r, m))
				return nil
			}
			// abort previous transfer
			r.resetTransfer(None)
		}
		if newTransfer == r.id {
			log.Infof("%s is already leader. Ignored transferring request from self", r.Info())
			return nil
		}
		// transfer
		// 1. reset transfer
		if err := r.resetTransfer(newTransfer); err != nil {
			return err
		}
		// 2. reset timeout
		r.heartbeatElapsed = 0
		// compare log
		if r.Prs[newTransfer].Match == r.RaftLog.LastIndex() {
			// log is up-to-date
			log.Debugf("%s sent MsgTimeoutNow to %s because already up-to-date", r.Info(), MessageStr(r, m))
			r.sendTimeoutNow(newTransfer)
		} else {
			r.send(r.NewAppendMsg(newTransfer)) // send append msg
		}
	}

	return nil
}

func (r *Raft) resetTransfer(id uint64) error {
	if id == r.leadTransferee {
		log.Infof("%s  previous leader transfer = next", r.Info())
		return nil
	}
	if id == r.id {
		log.Panicf("%s is already leader. Ignored transferring request from self", r.Info())
	}
	if id == None {
		log.Infof("%s  cancel leader transfer", r.Info())
		r.leadTransferee = None
		return nil
	}
	for _, peer := range r.peers {
		if peer == id {
			log.Infof("%s  leader transfer { %d -> %d }", r.Info(), r.leadTransferee, id)
			r.leadTransferee = id
			return nil
		}

	}
	return ErrProposalDropped
}

func (r *Raft) sendTimeoutNow(id uint64) {
	r.send(r.NewTimeOutMsg(id))
}
