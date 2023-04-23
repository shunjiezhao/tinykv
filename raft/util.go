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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func covEntry2PtrC(es ...pb.Entry) []*pb.Entry {
	var ans = make([]*pb.Entry, len(es))
	for i := range es {
		ans[i] = &pb.Entry{
			EntryType:            es[i].EntryType,
			Term:                 es[i].Term,
			Index:                es[i].Index,
			Data:                 es[i].Data,
			XXX_NoUnkeyedLiteral: es[i].XXX_NoUnkeyedLiteral,
			XXX_unrecognized:     es[i].XXX_unrecognized,
			XXX_sizecache:        es[i].XXX_sizecache,
		}
	}
	return ans
}
func covEntry2StructC(es ...*pb.Entry) []pb.Entry {
	var ans = make([]pb.Entry, len(es))
	for i := range es {
		ans[i] = pb.Entry{
			EntryType:            es[i].EntryType,
			Term:                 es[i].Term,
			Index:                es[i].Index,
			Data:                 es[i].Data,
			XXX_NoUnkeyedLiteral: es[i].XXX_NoUnkeyedLiteral,
			XXX_unrecognized:     es[i].XXX_unrecognized,
			XXX_sizecache:        es[i].XXX_sizecache,
		}
	}
	return ans
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, pb.HardState{})
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp *pb.Snapshot) bool {
	if sp == nil || sp.Metadata == nil {
		return true
	}
	return sp.Metadata.Index == 0
}

func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}

func nodes(r *Raft) []uint64 {
	nodes := make([]uint64, 0, len(r.Prs))
	for id := range r.Prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func diffu(a, b string) string {
	if a == b {
		return ""
	}
	aname, bname := mustTemp("base", a), mustTemp("other", b)
	defer os.Remove(aname)
	defer os.Remove(bname)
	cmd := exec.Command("diff", "-u", aname, bname)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// do nothing
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

func mustTemp(pre, body string) string {
	f, err := ioutil.TempFile("", pre)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func ltoa(l *RaftLog) string {
	s := fmt.Sprintf("committed: %d\n", l.committed)
	s += fmt.Sprintf("applied:  %d\n", l.applied)
	for i, e := range l.entries {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func IsLocalMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgHup || msgt == pb.MessageType_MsgBeat
}

func IsResponseMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgAppendResponse || msgt == pb.MessageType_MsgRequestVoteResponse || msgt == pb.MessageType_MsgHeartbeatResponse
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func MessageStr(r *Raft, m pb.Message) string {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		return fmt.Sprintf("[HUP] send")
	case pb.MessageType_MsgBeat:
		return fmt.Sprintf("[HeartBeat] send")
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgRequestVote:
		return fmt.Sprintf("[RequestVote] Commit: %v to %v", r.RaftLog.committed, m.To)
	case pb.MessageType_MsgRequestVoteResponse:
		return fmt.Sprintf("[RequestVoteResponse]  Commit: %v to %v Reject: %v", r.RaftLog.committed, m.To, m.Reject)
	case pb.MessageType_MsgHeartbeat:
		return fmt.Sprintf("[HeartBeat] Commit: %v to %v", r.RaftLog.committed, m.To)
	case pb.MessageType_MsgHeartbeatResponse:
		return fmt.Sprintf("[HeartBeatResponse] Commit: %v to %v", r.RaftLog.committed, m.To)
	case pb.MessageType_MsgAppend:
		return fmt.Sprintf("[Appendd] PrevInfo:{%v:%v}", m.Index, m.LogTerm)
	case pb.MessageType_MsgAppendResponse:
		return fmt.Sprintf("[AppendRespons] Commit: %v Reject: %v to %v", r.RaftLog.committed, m.Reject, m.To)

	}
	return ""
}

func (r *Raft) info() string {
	return fmt.Sprintf("{%x:%s:%d} ", r.id, r.State, r.Term)
}
