package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx  *GlobalContext
	pros []*proposal
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func getRegionMarshalData(region *metapb.Region) []byte {
	state := new(rspb.RegionLocalState)
	state.State = rspb.PeerState_Normal
	state.Region = region
	data, err := state.Marshal()
	mustNil(err)
	return data
}

func (d *peerMsgHandler) updateSplitStoreMeta(region ...*metapb.Region) {
	if len(region) == 0 {
		panic("")
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	for i, _ := range region {
		meta.regionRanges.ReplaceOrInsert(&regionItem{region: region[i]})
		meta.regions[region[i].Id] = region[i]
		log.Infof("update meta region id %d", region[i].Id)
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}

	if !d.RaftGroup.HasReady() {
		return
	}

	// Your Code Here (2B).
	ready := d.RaftGroup.Ready()
	regionState, err := d.peer.peerStorage.SaveReadyState(&ready)
	updateMeta := func(shouldNotify bool) {
		meta := d.ctx.storeMeta
		meta.Lock()
		meta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		meta.regions[d.Region().Id] = d.Region()
		meta.Unlock()
		if shouldNotify {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	}

	mustNil(err)
	if regionState != nil {
		d.SetRegion(regionState.Region)
		updateMeta(true)
		mustNil(d.peerStorage.Engines.Kv.Update(func(txn *badger.Txn) error {
			return txn.Set(meta.RegionStateKey(d.Region().Id), getRegionMarshalData(d.Region()))
		}))
	}

	d.Send(d.ctx.trans, ready.Messages)

	var resp = map[*pb.Entry]*raft_cmdpb.RaftCmdResponse{}
	var adminResp = map[*pb.Entry]*raft_cmdpb.RaftCmdResponse{}
	if len(ready.CommittedEntries) > 0 {
		addNode := util.InvalidID
		db := d.peerStorage.Engines.Kv
		var entry []*pb.Entry
		var modify bool
		if err := db.Update(func(txn *badger.Txn) error {
			for _, en := range ready.CommittedEntries {
				if d.stopped { // for debug
					panic("")
				}

				entry = append(entry, &en)
				if en.EntryType == pb.EntryType_EntryConfChange {
					var cc pb.ConfChange
					mustNil(cc.Unmarshal(en.Data))

					d.RaftGroup.ApplyConfChange(cc)
					// conf change entry
					region := d.Region()
					region.RegionEpoch.ConfVer++

					var index = -1
					for i, p := range region.Peers {
						if p.Id == cc.NodeId {
							index = i
							break
						}
					}

					if cc.ChangeType == pb.ConfChangeType_AddNode && index == -1 {
						pe := &metapb.Peer{Id: cc.NodeId, StoreId: cc.StoreId}
						region.Peers = append(region.Peers, pe)
						d.insertPeerCache(pe)
						log.Infof("%d insert peer %d", d.PeerId(), cc.NodeId)
						addNode = cc.NodeId
					} else if cc.ChangeType == pb.ConfChangeType_RemoveNode && index != -1 {
						if cc.NodeId == d.PeerId() {
							d.tryToDestroy() // corner case: https://asktug.com/t/topic/274196/5?replies_to_post_number=2
							return nil
						}
						region.Peers = append(region.Peers[:index], region.Peers[index+1:]...)
						log.Infof("%d remove peer index: %d", d.PeerId(), cc.NodeId)
						d.removePeerCache(cc.NodeId)
					}
					modify = true // if remove self not update region data
					log.Infof("RegionInfo : {peerID: %d}  {ConfVer: %d} {Peers: %+v}", d.peer.PeerId(), region.RegionEpoch.ConfVer, region.Peers)

					// set new region
					mustNil(txn.Set(meta.RegionStateKey(region.Id), getRegionMarshalData(region)))
					if addNode != util.InvalidID && d.RaftGroup.Raft.State == raft.StateLeader {
						d.RaftGroup.Raft.AppendEmptyEntryAndBckst() // if add node we should append empty entry to let new node catch up log
					}
				} else {
					request := raft_cmdpb.RaftCmdRequest{}
					mustNil(request.Unmarshal(en.Data))

					log.Debugf("handle raft ready %+v", request)
					switch {
					case request.AdminRequest != nil:
						if request.Header != nil && request.Header.RegionEpoch != nil && util.IsEpochStale(request.Header.RegionEpoch, d.Region().RegionEpoch) {
							log.Infof("stale epoch %d %d", request.Header.RegionEpoch.Version, d.Region().RegionEpoch.Version)
							ans := ErrRespStaleCommand(d.Term())
							ans.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: request.AdminRequest.CmdType}
							adminResp[&en] = ans
							continue
						}
						adminResp[&en] = d.handleAdminReq(request.AdminRequest, txn)
					case len(request.Requests) > 0:
						resp[&en] = d.handleReq(&request, txn)
					}
				}
			}
			updateMeta(modify)

			d.peerStorage.applyState.AppliedIndex = max(d.peerStorage.AppliedIndex(), ready.CommittedEntries[len(ready.CommittedEntries)-1].Index)
			marshal, err := proto.Marshal(d.peerStorage.applyState)
			mustNil(err)
			mustNil(txn.Set(meta.ApplyStateKey(d.regionId), marshal))
			return nil
		}); err != nil {
			panic(err)
		}

		for _, en := range entry {
			if pro := d.handlePro(en); pro != nil && pro.cb != nil {
				if cmdResponse, ok := resp[en]; ok {
					log.Infof("cmd-proposals %+v %+v", pro, cmdResponse)
					if len(cmdResponse.Responses) > 0 && cmdResponse.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
						pro.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
					}
					pro.cb.Done(cmdResponse)
				} else if adminResponse, ok := adminResp[en]; ok {
					log.Infof("admin-proposals %+v %+v", pro, adminResponse)
					pro.cb.Done(adminResponse)
				}
				log.Debug("proposals done", pro)
			}
		}
		if addNode != util.InvalidID && d.RaftGroup.Raft.State == raft.StateLeader {
			d.RaftGroup.Raft.AppendEmptyEntryAndBckst() // if add node we should append empty entry to let new node catch up log
		}

	}

	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) handlePro(entry *pb.Entry) *proposal {
	if entry == nil {
		log.Panicf("entry is nil")
	}
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]
		if entry.Term < proposal.term {
			break
		}

		if entry.Term > proposal.term {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Term == proposal.term && entry.Index < proposal.index {
			break
		}

		if entry.Term == proposal.term && entry.Index > proposal.index {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Index == proposal.index && entry.Term == proposal.term {
			d.proposals = d.proposals[1:]
			return proposal
		}

		panic("This should not happen.")
	}
	return nil
}
func (d *peerMsgHandler) handleAdminReq(req *raft_cmdpb.AdminRequest, txn *badger.Txn) *raft_cmdpb.RaftCmdResponse {
	if req == nil {
		log.Panicf("req is nil")
		return nil
	}

	var resp = &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: req.CmdType,
		},
	}

	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		index, term := req.GetCompactLog().GetCompactIndex(), req.GetCompactLog().GetCompactTerm()
		if index < d.RaftGroup.Raft.RaftLog.First() {
			log.Warn("compact index less than first index")
			return resp
		}
		d.peerStorage.applyState.TruncatedState = &rspb.RaftTruncatedState{
			Index: index,
			Term:  term,
		}
		d.ScheduleCompactLog(index)
		return resp
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
		resp.AdminResponse.CmdType = raft_cmdpb.AdminCmdType_TransferLeader
		resp.AdminResponse.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
		return resp
	case raft_cmdpb.AdminCmdType_ChangePeer:

	case raft_cmdpb.AdminCmdType_Split:

		log.Warnf("split req %+v", req)
		// split
		// 1. clone region
		oldRegion, newRegion := d.Region(), new(metapb.Region)
		util.CloneMsg(oldRegion, newRegion)
		// 2. check peer len is equal
		if len(oldRegion.Peers) != len(req.Split.NewPeerIds) {
			log.Warnf("peer len is not equal %d %d", len(oldRegion.Peers), len(req.Split.NewPeerIds))
			return ErrRespStaleCommand(d.Term())
		}
		// 2.2 end key

		if engine_util.ExceedEndKey(oldRegion.EndKey, req.Split.SplitKey) {
			resp.Header.Error = d.buildKeyNotInRegionErr(req.Split.SplitKey)
			return resp
		}
		// for debug
		if util.CheckKeyInRegion(req.Split.SplitKey, d.Region()) != nil {
			log.Panicf("key not in region %s %+v", req.Split.SplitKey, d.Region())
		}
		for i, id := range req.Split.NewPeerIds {
			newRegion.Peers[i].Id = id
			log.Debugf("new peer id %d", id)
			d.insertPeerCache(newRegion.Peers[i])
		}
		cloneStr := func(s []byte) []byte {
			cp := make([]byte, len(s))
			copy(cp, s)
			return cp
		}
		// 3. add version
		// 4. set state to new region ( endkey, peer,id , regionEpoch, startkey)

		newRegion.Id = req.Split.NewRegionId
		newRegion.StartKey = cloneStr(req.Split.SplitKey)
		oldRegion.EndKey = cloneStr(req.Split.SplitKey) // [start, split)
		newRegion.RegionEpoch.Version++
		oldRegion.RegionEpoch.Version++
		d.updateSplitStoreMeta(oldRegion, newRegion)
		mustNil(txn.Set(meta.RegionStateKey(oldRegion.Id), getRegionMarshalData(oldRegion)))
		mustNil(txn.Set(meta.RegionStateKey(newRegion.Id), getRegionMarshalData(newRegion)))
		//meIndex := 0
		ctx := d.ctx
		p, _ := createPeer(d.storeID(), ctx.cfg, ctx.regionTaskSender, ctx.engine, newRegion)
		d.ctx.router.register(p)
		mustNil(d.ctx.router.send(p.regionId, message.Msg{Type: message.MsgTypeStart}))
		d.notifyHeartbeatScheduler(oldRegion, d.peer)
		d.notifyHeartbeatScheduler(newRegion, p)

		return resp
	default:
		log.Panicf("unknown admin cmd type %v", req.CmdType)
	}
	return resp

}

func (d *peerMsgHandler) buildKeyNotInRegionErr(key []byte) *errorpb.Error {
	region := d.Region()
	return &errorpb.Error{
		KeyNotInRegion: &errorpb.KeyNotInRegion{
			Key:      key,
			RegionId: region.Id,
			StartKey: region.StartKey,
			EndKey:   region.EndKey,
		},
	}
}
func (d *peerMsgHandler) handleReq(request *raft_cmdpb.RaftCmdRequest, txn *badger.Txn) *raft_cmdpb.RaftCmdResponse {
	if len(request.Requests) != 1 {
		log.Panicf("len(request.Requests) != 1 %d", len(request.Requests))
	}

	// check key is in region range
	//util.CheckKeyInRegion()

	req := request.Requests[0]
	var res raft_cmdpb.Response
	var resp = &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()},
	}
	resp.Responses = []*raft_cmdpb.Response{&res}

	res.CmdType = req.CmdType
	if request.Header != nil && request.Header.RegionEpoch != nil && util.IsEpochStale(request.Header.RegionEpoch, d.Region().RegionEpoch) {
		resp = ErrRespStaleCommand(d.Term())
		resp.Responses = []*raft_cmdpb.Response{&res}
		return resp
	}

	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		if err := util.CheckKeyInRegion(req.GetGet().Key, d.Region()); err != nil {
			resp.Header.Error = d.buildKeyNotInRegionErr(req.GetGet().Key)
			return resp
		}
		log.Infof("%s get %s %s", d.RaftGroup.Raft.Info(), req.Get.Cf, req.Get.Key)
		val, err := txn.Get(engine_util.KeyWithCF(req.Get.Cf, req.Get.Key))
		mustNil(err)
		value, err := val.Value()
		mustNil(err)
		res.Get = &raft_cmdpb.GetResponse{Value: value}
	case raft_cmdpb.CmdType_Put:
		if err := util.CheckKeyInRegion(req.GetPut().Key, d.Region()); err != nil {
			resp.Header.Error = d.buildKeyNotInRegionErr(req.GetPut().Key)
			return resp
		}
		log.Infof("%s Put (%s,%s) %s", d.RaftGroup.Raft.Info(), req.GetPut().GetCf(), req.GetPut().Key, req.GetPut().Value)
		mustNil(txn.Set(engine_util.KeyWithCF(req.GetPut().GetCf(), req.GetPut().Key), req.GetPut().Value))
		res.Put = &raft_cmdpb.PutResponse{}
	case raft_cmdpb.CmdType_Delete:
		if err := util.CheckKeyInRegion(req.GetDelete().Key, d.Region()); err != nil {
			resp.Header.Error = d.buildKeyNotInRegionErr(req.GetDelete().Key)
			return resp
		}
		log.Infof("Delete (%s,%s)", req.Delete.Cf, req.Delete.Key)
		mustNil(txn.Delete(engine_util.KeyWithCF(req.GetDelete().GetCf(), req.GetDelete().Key)))
		res.Delete = &raft_cmdpb.DeleteResponse{}
	case raft_cmdpb.CmdType_Snap:
		log.Infof("snap")
		res.Snap = &raft_cmdpb.SnapResponse{Region: d.Region()}
	}

	return resp
}

func (d *peerMsgHandler) findPro(index, term uint64) *proposal {
	for _, p := range d.proposals {
		if p.index == index && p.term == term {
			return p
		}
	}
	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		log.Warnf("%s check store id error %v", d.Tag, err)
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		log.Info(d.RaftGroup.Raft.Info(), "leader is ", leaderID)
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	log.Debug("proposeRaftCommand")
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		//log.Warnf("%s pre propose error %v", d.Tag, err)
		return
	}
	if d.RaftGroup.Raft.State != raft.StateLeader {
		log.Panicf("%s is not leader", d.Tag)
	}
	// Your Code Here (2B).
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	var pro proposal
	pro.cb = cb
	pro.term = d.Term()
	pro.index = d.nextProposalIndex()
	admin := msg.AdminRequest != nil
	for {
		if admin &&
			msg.AdminRequest.ChangePeer != nil {
			change := msg.AdminRequest.ChangePeer
			log.Infof("%+v", change)
			if d.RaftGroup.Raft.CanChangeConf() {
				if err := d.RaftGroup.ProposeConfChange(pb.ConfChange{
					ChangeType: change.ChangeType,
					NodeId:     change.Peer.Id,
					StoreId:    change.Peer.StoreId,
				}); err != nil {
					log.Panicf("propose conf change error %v", err)
				}
			} else {
				cb.Done(ErrRespStaleCommand(d.Term()))
				return
			}

		}
		//if admin && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_Split {
		//	log.Infof("")
		//}

		err := d.peer.RaftGroup.Propose(data)
		if err == nil {
			break
		}
		if err == raft.ErrProposalDropped {
			if admin && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader { // when transfer leader don't propose
				//cb.Done(ErrRespStaleCommand(d.Term()))
				return
			}
			continue
		}

	}

	log.Debugf("%s append proposals %+v", d.RaftGroup.Raft.State, pro)
	d.proposals = append(d.proposals, &pro)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
func (d *peerMsgHandler) tryToDestroy() {
	if len(d.Region().Peers) == 2 && d.IsLeader() {
		var targetPeer uint64 = 0
		for _, peer := range d.Region().Peers {
			if peer.Id != d.PeerId() {
				targetPeer = peer.Id
				break
			}
		}
		if targetPeer == 0 {
			panic("This should not happen")
		}

		msg := d.RaftGroup.Raft.NewAppendMsg(targetPeer)
		msg.From = d.PeerId()
		msg.Term = d.Term()
		m := []pb.Message{}
		for i := 0; i < 10; i++ {
			d.Send(d.ctx.trans, m)
			time.Sleep(50 * time.Millisecond)
		}
	}
	d.destroyPeer()
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	req := &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
	log.Infof("%s notify heartbeat scheduler %+v", req)
	d.ctx.schedulerTaskSender <- req
}
