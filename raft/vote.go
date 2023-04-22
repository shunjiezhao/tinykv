package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result VoteResult) {
	if v {
		log.Infof("%s received %s from %x at term %d", r.info(), t, id, r.Term)
	} else {
		log.Infof("%s received %s rejection from %x at term %d", r.info(), t, id, r.Term)
	}
	r.RecordVote(id, v)
	return r.TallyVotes()
}
func (r *Raft) RecordVote(id uint64, v bool) {
	_, ok := r.votes[id]
	if !ok {
		r.votes[id] = v
	} else {
		log.Debugf("%x already exist", id)
	}
}

func (r *Raft) TallyVotes() (granted int, rejected int, _ VoteResult) {
	ids := nodes(r)
	for _, id := range ids {
		v, voted := r.votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	result := r.VoteResult()
	log.Debugf("%s Tally Votes %v %v %s", r.info(), granted, rejected, result)
	return granted, rejected, result
}

type VoteResult uint8

var voteResultStr = [...]string{
	VotePending: "VotePending",
	VoteLost:    "VoteLost",
	VoteWon:     "VoteWon",
}

func (v VoteResult) String() string {
	return voteResultStr[v]
}

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = 1 + iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)

func (r *Raft) VoteResult() VoteResult {
	servers := nodes(r)
	if len(servers) == 0 {
		// By convention, the elections on an empty config win. This comes in
		// handy with joint quorums because it'll make a half-populated joint
		// quorum behave like a majority quorum.
		log.Panicf("v is 0")
		return VoteWon
	}

	var votedCnt int //vote counts for yes.
	var missing int
	for _, id := range servers {
		v, ok := r.votes[id]
		if !ok {
			missing++
			continue
		}
		if v {
			votedCnt++
		}
	}

	q := len(servers)/2 + 1
	log.Infof("get q: %d voteCnt: %d missing: %d", q, votedCnt, missing)
	if votedCnt >= q {
		return VoteWon
	}
	if votedCnt+missing >= q {
		return VotePending
	}
	return VoteLost
}

func (r *Raft) handleVote(m pb.Message) {
	var reject = false
	if r.Term > m.Term {
		log.Panicf("term is ==")
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None) // 当前过时, 就将变成 follower
	}
	// 比较 谁更新
	if r.compareLogIsMeNew(&m) {
		log.Debugf("log is not new")
		reject = true
		goto send
	}

	if r.Vote == None || r.Vote == m.From { // not or repeat
		r.Vote = m.From
	}
	// send response
send:
	r.send(r.NewResponseVoteMsg(m.From, reject))
	return
}

// true: me new
func (r *Raft) compareLogIsMeNew(m *pb.Message) bool {
	lastLog := r.RaftLog.LastLog()
	if m.LogTerm != lastLog.Term {
		return m.LogTerm < lastLog.Term // from < to ? false : true
	}
	// term ==
	if m.Index < lastLog.Index {
		return true
	}
	// index from >= to
	return false
}
