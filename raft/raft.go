// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}
type stepFunc func(r *Raft, m pb.Message) error
type Raft struct {
	id    uint64
	peers []uint64
	step  stepFunc

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

var rd = rand.NewSource(time.Now().Unix())

func randN(n int) int {
	return int(rd.Int63()) % n
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).
	raft := &Raft{
		id:               c.ID,
		peers:            c.peers,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		State:            StateFollower,
		votes:            map[uint64]bool{},
		msgs:             []pb.Message{},
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick + randN(c.ElectionTick), // [el, 2*el-1]
	}
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{}
	}
	log.Debugf("New Raft Config %+v", c)
	raft.becomeFollower(raft.Term, None)
	//log.Debugf("New Raft %+v\n", raft)
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	if to == r.id {
		log.Panicf("send your self ?")
	}
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed) // 匹配, 自己
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	}
	r.send(msg)
	log.Debugf("append msg %s", MessageStr(r, msg))
}

func (r *Raft) Visit(f func(idx int, to uint64), sendMe bool) {
	ids := nodes(r)
	for idx, to := range ids {
		if r.id == to && sendMe == false {
			continue
		}
		f(idx, to)
	}

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	r.heartbeatElapsed++
	r.electionElapsed++
	// Your Code Here (2A).
	if r.State == StateLeader {
		// 发送心跳
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.step(r, pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
		return
	}

	// follow , candidate
	if r.electionElapsed >= r.electionTimeout {
		r.becomeCandidate()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// 1. 任期
	r.Term = term
	// 2. 投票
	r.Vote = None
	// 3. leadId
	r.Lead = lead
	r.electionElapsed = 0   // 清空选举超时
	r.State = StateFollower // 状态改变
	r.step = stepFollower
	if r.Term != 0 {
		log.Infof("%s became %s at term %d", r.info(), r.State, r.Term)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.step = stepCandidate
	r.Term++
	r.Vote = r.id
	r.votes = map[uint64]bool{} // RESET
	log.Infof("%s became candidate at term %d", r.info(), r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.Vote == None || r.State != StateCandidate {
		log.Panicf("vote:%v state:%s", r.Vote, r.State)
	}
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// 0. state -> Leader
	r.State = StateLeader
	r.step = stepLeader
	// 1. election -> 0
	r.electionElapsed = 0
	// 2. 更新 nextIndex,matchIndex
	for _, progress := range r.Prs {
		progress.Next = r.RaftLog.LastIndex() + 1
	}
	// 3. lead = me
	r.Lead = r.id
	// 4.todo:append Empty Log
	log.Infof("%s became %s at term %d", r.info(), r.State, r.Term)

}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.hup()
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m)

	}
	return nil
}
func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.handleHeartbeat(m)
	//case pb.MessageType_MsgHup:
	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, m.MsgType, !m.Reject) //Reject = true stand not vote
		log.Infof("%s has received %s %d votes and %d vote rejections", r.info(), MessageStr(r, m), gr, rj)
		switch res {
		case VoteWon:
			if r.State == StateCandidate {
				r.becomeLeader()
				r.bcastAppend()
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
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.Visit(func(idx int, to uint64) {
			r.sendHeartbeat(to)
		}, false)

	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	log.Infof("%s receive msg: %s %+v", r.info(), MessageStr(r, m), m)
	// 判断是否合法
	if r.Term > m.Term { // 过时的
		log.Debug("out dated")
		m.Reject = m.MsgType == pb.MessageType_MsgRequestVote // 如果是投票的话就直接拒绝
		return nil
	}

	// Your Code Here (2A).
	err := r.step(r, m)
	if err != nil {
		log.Errorf("")
	}
	return nil
}

func (r *Raft) hup() {
	ids := nodes(r)
	for _, id := range ids {
		if id == r.id {
			r.step(r, r.NewResponseVoteMsg(r.id, false))
			continue
		}
		r.send(r.NewRequestVoteMsg(id))
	}

}
func (r *Raft) send(m pb.Message) {
	if m.Term == None {
		m.Term = r.Term
	}
	if m.From == None {
		m.From = r.id
	}

	r.msgs = append(r.msgs, m)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.becomeFollower(m.Term, m.To)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) bcastAppend() {
	r.Visit(func(idx int, to uint64) {
		r.sendAppend(to)
	}, false)
}
