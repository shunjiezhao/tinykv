package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result VoteResult) {
	if v {
		log.Infof("%s received %s from %x at term %d", r.Info(), t, id, r.Term)
	} else {
		log.Infof("%s received %s rejection from %x at term %d", r.Info(), t, id, r.Term)
	}
	r.RecordVote(id, v)
	return r.TallyVotes()
}
func (r *Raft) RecordVote(id uint64, v bool) {
	_, ok := r.votes[id]
	if !ok {
		r.votes[id] = v
		log.Debugf("%s record vote %x %v", r.Info(), id, v)
	} else {
		log.Debugf("%s record vote %x already exist", r.Info(), id)
	}
}

func (r *Raft) TallyVotes() (granted int, rejected int, _ VoteResult) {
	ids := r.peers
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
	servers := r.peers
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
	log.Infof("%s get q: %d voteCnt: %d missing: %d", r.Info(), q, votedCnt, missing)
	if votedCnt >= q {
		return VoteWon
	}
	if votedCnt+missing >= q {
		return VotePending
	}
	return VoteLost
}

// true: me new
func (r *Raft) myLogIsOld(term, index uint64) bool {
	l := r.RaftLog.LastLog()
	return term > l.Term || (term == l.Term && index >= l.Index)
}
