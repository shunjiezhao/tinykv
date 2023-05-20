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

func (p *Progress) mayUpdateIndex(index uint64) {
	p.Match = max(p.Match, index)
	p.Next = max(p.Match+1, p.Next)
}

type Raft struct {
	id      uint64
	peers   []uint64
	step    stepFunc
	storage Storage
	Term    uint64
	Vote    uint64

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
	PendingConfIndex          uint64
	randomizedElectionTimeout int
}

var rd = rand.NewSource(time.Now().UnixNano())

func randN(n int) int {
	return int(rd.Int63()) % n
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	state, cfg, err := c.Storage.InitialState()
	log.Infof("state: %+v confState: %+v", state, cfg)
	if err != nil {
		log.Panic(err)
	}

	// Your Code Here (2A).
	raft := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		State:            StateFollower,
		votes:            map[uint64]bool{},
		storage:          c.Storage,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick, // [el, 2*el-1]
	}
	if raft.id == 0 {
		log.Panicf("id is 0, can't not be raft")
	}
	raft.peers = c.peers
	if len(cfg.Nodes) != 0 {
		raft.peers = cfg.Nodes
	}
	raft.step = stepFollower
	raft.reset(state.Term)
	raft.Vote = state.Vote
	raft.resetPrs()

	log.Debug("New Raft %+v\n", raft)
	return raft
}
func (r *Raft) resetPrs() {
	r.Prs = map[uint64]*Progress{}
	for _, peer := range r.peers {
		r.Prs[peer] = &Progress{
			Match: r.RaftLog.start,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64, null bool) bool {
	if r.Prs[to].Next > r.RaftLog.LastIndex() && null == false {
		log.Debugf("next %d > last %d", r.Prs[to].Next, r.RaftLog.LastIndex())
		return false
	}
	r.send(r.NewAppendMsg(to))
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	if to == r.id {
		log.Panicf("send your self ?")
	}
	// Your Code Here (2A).
	r.send(r.NewHeartbeatMsg(to))
}

func (r *Raft) Visit(f func(idx int, to uint64), sendMe bool) {
	for idx, to := range r.peers {
		if r.id == to && sendMe == false {
			continue
		}
		f(idx, to)
	}

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tickLeader() {
	r.heartbeatElapsed++
	// Your Code Here (2A).
	// 发送心跳
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		log.Debug("send heartbeat")
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Panic(err)
		}
	}
}

func (r *Raft) tick() {
	if r.State == StateLeader {
		r.tickLeader()
	} else {
		r.electionElapsed++
		if r.pastElectionTimeout() {
			log.Errorf("%s time out", r.Info())
			r.resetElectionTimeOut()
			if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
				log.Debugf("error occurred during election: %v", err)
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.step = stepFollower
	r.reset(term)
	r.Lead = lead
	r.electionElapsed = 0   // 清空选举超时
	r.State = StateFollower // 状态改变
	if r.Term != 0 {
		log.Infof("%s became %s at term %d Lead:%d", r.Info(), r.State, r.Term, lead)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
	r.votes = map[uint64]bool{} // RESET
	log.Infof("%s became candidate at term %d", r.Info(), r.Term)
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
	// 3. lead = me
	r.Lead = r.id
	entry := &pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: nil}
	r.leaderAppendEntries(entry)
	if len(r.peers) == 1 {
		log.Infof("single node")
		r.updateCommit()
	}
	log.Infof("%s became %s at term %d", r.Info(), r.State, r.Term)
}

// updateCommit update and return commit index
func (r *Raft) updateCommit() uint64 {
	prev := r.RaftLog.committed // for debug
	start := max(prev+1, r.RaftLog.First())

	for index := start; index <= r.RaftLog.LastIndex(); index++ {
		if r.RaftLog.entries[index-r.RaftLog.start].Term != r.Term {
			continue
		}
		var count = 1
		for _, id := range r.peers {
			if id != r.id && r.Prs[id].Match >= index {
				count++
			}
		}
		// 假设存在 N 满足N > CommitIndex，使得大多数的 matchIndex[i] ≥ N以及log[N].term == CurrentTerm 成立，则令 CommitIndex = N（5.3 和 5.4 节）
		if count > len(r.peers)/2 {
			r.RaftLog.committed = index
			log.Debugf("%s update commit to %d", r.Info(), r.RaftLog.committed)
		}
	}
	return r.RaftLog.committed
}

func (r *Raft) bckstHeart() {
	r.Visit(func(idx int, to uint64) {
		r.sendHeartbeat(to)
	}, false)
}
func (r *Raft) hup() {
	if r.State == StateLeader {
		log.Infof("%s is already leader", r.Info())
		return
	}
	r.becomeCandidate()
	for _, id := range r.peers {
		if id == r.id {
			r.Step(r.NewRespVoteMsg(r.id, false))
			continue
		}
		r.send(r.NewRequestVoteMsg(id))
	}
	log.Debugf("send done %+v", r.msgs)

}
func (r *Raft) send(m pb.Message) {
	if m.Term == None {
		m.Term = r.Term
	}
	if m.From == None {
		m.From = r.id
	}

	r.msgs = append(r.msgs, m)
	log.Debugf("send %+v", MessageStr(r, m))
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	log.Debugf("recv %s", m.MsgType)
	// Your Code Here (2A).
	var reject = false
	var index uint64 = 0
	var myCommit uint64

	// is prevLog Index
	prevLog, err := r.RaftLog.entryAt(m.Index)
	if err != nil { // don't match may be i'm compact or exceed
		reject = true
		if errors.Is(err, ErrCompacted) {
			index = r.RaftLog.committed
			reject = false // the log is consistency is quarom,but is snapshot, leader can send snapshot
		}
		goto send
	}
	// compare prevLog
	if prevLog.Term != m.LogTerm {
		// conflict
		reject = true
		//todo(performance)
		goto send
	}

	// append
	r.resetElectionTimeOut() // todo(in req vote and vote to other)
	// log
	r.appendEntries(m.Entries...)
	// update commit
	myCommit = r.RaftLog.committed

	index = m.Index + uint64(len(m.Entries)) // update index all log we received
	if myCommit < m.Commit {                 // < leader commit
		r.RaftLog.updateCommitIndex(min(index, m.Commit))
		log.Infof("%s update commit %d->%d", r.Info(), myCommit, min(index, m.Commit))
	}
	// update send index
	log.Debugf("%s commit %d NewIndex: %d LeaderCommit: %d", r.Info(), r.RaftLog.committed, index, m.Commit)
send:
	msg := r.NewRespAppendMsg(m.From, index, reject)
	r.send(msg)
	log.Debugf("%s send append response to %x %s", r.Info(), m.From, MessageStr(r, msg))
}
func (r *Raft) resetElectionTimeOut() {
	r.electionElapsed = 0
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// 更新选举时间
	r.resetElectionTimeOut()
	// 发送响应
	r.send(r.NewRespHeartbeatMsg(m.From))
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleProse(m pb.Message) {
	r.leaderAppendEntries(m.Entries...)
	if len(r.peers) == 1 {
		r.updateCommit()
	}
}
func (r *Raft) handleSnapshot(m pb.Message) {
	log.Debug("handleSnapshot")
	// Your Code Here (2C).
	if m.Snapshot == nil || m.Snapshot.Metadata == nil {
		log.Panicf("recv snapshot is nil")
	}
	snapShot, metaData := m.Snapshot, m.Snapshot.Metadata
	index, term := metaData.Index, metaData.Term
	if index < r.RaftLog.First() {
		log.Debugf("handleSnapshot %s ignore snapshot %d < %d", r.Info(), index, r.RaftLog.First())
		return
	}
	r.RaftLog.pendingSnapshot = snapShot

	// update state
	if metaData.ConfState != nil {
		newPeers := metaData.ConfState.Nodes
		prs := make(map[uint64]*Progress)
		for _, id := range newPeers {
			if pr, ok := r.Prs[id]; ok {
				prs[id] = pr
			} else {
				prs[id] = &Progress{
					Match: 0,
					Next:  r.RaftLog.LastIndex(),
				}
			}
		}
		r.peers = newPeers
		r.Prs = prs
	}

	r.RaftLog.CutDown(index, term)
	log.Infof("%s cut down log to %d", r.Info(), index)
	r.send(r.NewRespAppendMsg(m.From, r.RaftLog.committed, false))
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) bcastAppend(me bool, null bool) {
	r.Visit(func(idx int, to uint64) {
		r.sendAppend(to, null)
	}, me)
}

// leaderAppendEntries don't boardcast
func (r *Raft) leaderAppendEntries(es ...*pb.Entry) uint64 {
	if r.State != StateLeader {
		log.Panicf("you should check your state %s", r.State)
	}
	esA := make([]pb.Entry, len(es))
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
		esA[i] = *es[i]
	}
	li = r.RaftLog.append(esA...)
	r.Prs[r.id].Next = li + 1
	r.Prs[r.id].Match = li
	return li
}

func (r *Raft) appendEntries(entries ...*pb.Entry) uint64 {
	for _, entry := range entries {
		// if has this log we should truncate
		if entry.Index <= r.RaftLog.LastIndex() && r.RaftLog.IsConflict(entry.Index, entry.Term) {
			log.Debugf("%s truncate log %d", r.Info(), entry.Index)
			r.RaftLog.truncate(entry.Index) //
		} else if r.RaftLog.Contain(entry.Index) {
			continue
		}
		// new log
		log.Debugf("%s append log %s", r.Info(), entry)
		r.RaftLog.append(*entry)
	}
	// update commit
	return r.RaftLog.LastIndex()
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.resetRandomizedElectionTimeout()
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = map[uint64]bool{}
}
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + randN(r.electionTimeout)
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}
