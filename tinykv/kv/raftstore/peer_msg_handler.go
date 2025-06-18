package raftstore

import (
    "fmt"
    "time"

    "github.com/Connor1996/badger/y"
    "github.com/pingcap-incubator/tinykv/kv/raftstore/message"
    "github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
    "github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
    "github.com/pingcap-incubator/tinykv/kv/raftstore/util"
    "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
    "github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
    "github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
    rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
    "github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
    "github.com/pingcap/errors"
    "github.com/pingcap/log"
    "go.uber.org/zap"
)

type PeerTick int

const (
    PeerTickRaft               PeerTick = 0 // Raft 基础定时器
    PeerTickRaftLogGC          PeerTick = 1 // 日志 GC 定时器
    PeerTickSplitRegionCheck   PeerTick = 2 // 分裂检查定时器
    PeerTickSchedulerHeartbeat PeerTick = 3 // 心跳定时器
)

// peerMsgHandler 是 Raft 节点消息的核心处理器
type peerMsgHandler struct {
    *peer                          // 嵌入 peer，获得 Raft 节点的基本功能
    applyCh chan []message.Msg     // 应用消息通道，将已提交的日志发送给状态机执行
    ctx     *GlobalContext         // 全局上下文，包含存储引擎、配置、调度器等共享资源
}

// newPeerMsgHandler 创建新的消息处理器实例
func newPeerMsgHandler(peer *peer, applyCh chan []message.Msg, ctx *GlobalContext) *peerMsgHandler {
    return &peerMsgHandler{
        peer:    peer,    // 关联的 Raft 节点
        applyCh: applyCh, // 应用消息通道
        ctx:     ctx,     // 全局上下文
    }
}

// HandleMsg 是消息处理的总入口，根据消息类型分发到具体的处理函数
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
    switch msg.Type {
    case message.MsgTypeRaftMessage:
        // 处理 Raft 协议消息（投票、日志复制、心跳等）
        raftMsg := msg.Data.(*rspb.RaftMessage)
        if err := d.onRaftMsg(raftMsg); err != nil {
            log.Error(fmt.Sprintf("%s handle raft message error %v", d.Tag, err))
        }
    case message.MsgTypeRaftCmd:
        // 处理客户端的读写命令请求
        raftCMD := msg.Data.(*message.MsgRaftCmd)
        d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
    case message.MsgTypeTick:
        // 处理定时器事件，驱动 Raft 状态机
        d.onTick()
    case message.MsgTypeApplyRes:
        // 处理命令应用结果，更新节点状态
        res := msg.Data.(*MsgApplyRes)
        d.onApplyResult(res)
    case message.MsgTypeSplitRegion:
        // 处理区域分裂请求
        split := msg.Data.(*message.MsgSplitRegion)
        log.Info(fmt.Sprintf("%s on split with %v", d.Tag, split.SplitKey))
        d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
    case message.MsgTypeRegionApproximateSize:
        // 更新区域大小估算值
        d.onApproximateRegionSize(msg.Data.(uint64))
    case message.MsgTypeGcSnap:
        // 处理快照垃圾回收
        gcSnap := msg.Data.(*message.MsgGCSnap)
        d.onGCSnap(gcSnap.Snaps)
    case message.MsgTypeStart:
        // 启动定时器
        d.startTicker()
    }
}

// onTick 处理定时器事件，检查各种定时器是否到期并执行相应操作
func (d *peerMsgHandler) onTick() {
    // 如果节点已停止，直接返回
    if d.stopped {
        return
    }
    // 推进定时器时钟
    d.ticker.tickClock()

    // 检查 Raft 基础定时器（选举超时、心跳等）
    if d.ticker.isOnTick(PeerTickRaft) {
        d.onRaftBaseTick()
    }
    // 检查日志垃圾回收定时器
    if d.ticker.isOnTick(PeerTickRaftLogGC) {
        d.onRaftGCLogTick()
    }
    // 检查调度器心跳定时器
    if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
        d.onSchedulerHeartbeatTick()
    }
    // 检查区域分裂检查定时器
    if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
        d.onSplitRegionCheckTick()
    }
    // 通知定时器驱动器继续处理
    d.ctx.tickDriverSender <- d.regionId
}

// 启动所有定时器
func (d *peerMsgHandler) startTicker() {
    d.ticker = newTicker(d.regionId, d.ctx.cfg)
    d.ctx.tickDriverSender <- d.regionId
    d.ticker.schedule(PeerTickRaft)
    d.ticker.schedule(PeerTickRaftLogGC)
    d.ticker.schedule(PeerTickSplitRegionCheck)
    d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

// 处理快照垃圾回收
func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
    compactedIdx := d.peerStorage.truncatedIndex()
    compactedTerm := d.peerStorage.truncatedTerm()
    for _, snapKeyWithSending := range snaps {
        key := snapKeyWithSending.SnapKey
        if snapKeyWithSending.IsSending {
            snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
            if err != nil {
                log.Error(fmt.Sprintf("%s failed to load snapshot for %s %v", d.Tag, key, err))
                continue
            }
            if key.Term < compactedTerm || key.Index < compactedIdx {
                log.Info(fmt.Sprintf("%s snap file %s has been compacted, delete", d.Tag, key))
                d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
            } else if fi, err1 := snap.Meta(); err1 == nil {
                modTime := fi.ModTime()
                if time.Since(modTime) > 4*time.Hour {
                    log.Info(fmt.Sprintf("%s snap file %s has been expired, delete", d.Tag, key))
                    d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
                }
            }
        } else if key.Term <= compactedTerm &&
            (key.Index < compactedIdx || key.Index == compactedIdx) {
            log.Info(fmt.Sprintf("%s snap file %s has been applied, delete", d.Tag, key))
            a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
            if err != nil {
                log.Error(fmt.Sprintf("%s failed to load snapshot for %s %v", d.Tag, key, err))
                continue
            }
            d.ctx.snapMgr.DeleteSnapshot(key, a, false)
        }
    }
}

// 处理 Raft 就绪状态，包括应用快照、提交日志等
func (d *peerMsgHandler) HandleRaftReady() {
    if d.stopped {
        return
    }

    msgs := make([]message.Msg, 0)
    if p := d.TakeApplyProposals(); p != nil {
        msg := message.Msg{Type: message.MsgTypeApplyProposal, Data: p, RegionID: p.RegionId}
        msgs = append(msgs, msg)
    }
    applySnapResult, msgs := d.peer.HandleRaftReady(msgs, d.ctx.schedulerTaskSender, d.ctx.trans)
    if applySnapResult != nil {
        prevRegion := applySnapResult.PrevRegion
        region := applySnapResult.Region

        log.Info(fmt.Sprintf("%s snapshot for region %s is applied", d.Tag, region))
        meta := d.ctx.storeMeta
        meta.Lock()
        defer meta.Unlock()
        initialized := len(prevRegion.Peers) > 0
        if initialized {
            log.Info(fmt.Sprintf("%s region changed from %s -> %s after applying snapshot", d.Tag, prevRegion, region))
            meta.regionRanges.Delete(&regionItem{region: prevRegion})
        }
        if oldRegion := meta.regionRanges.ReplaceOrInsert(&regionItem{region: region}); oldRegion != nil {
            panic(fmt.Sprintf("%s unexpected old region %+v, region %+v", d.Tag, oldRegion, region))
        }
        meta.regions[region.Id] = region
    }
    d.applyCh <- msgs
}

// 处理 Raft 基础定时器事件
func (d *peerMsgHandler) onRaftBaseTick() {
    // 有待处理快照时跳过
    if d.HasPendingSnapshot() {
        d.ticker.schedule(PeerTickRaft)
        return
    }
    // 推进 Raft 状态机
    d.RaftGroup.Tick()
    d.ticker.schedule(PeerTickRaft)
}

// 处理命令应用结果
func (d *peerMsgHandler) onApplyResult(res *MsgApplyRes) {
    log.Debug(fmt.Sprintf("%s async apply finished %v", d.Tag, res))
    // 处理已提交日志的执行结果
    for _, result := range res.execResults {
        switch x := result.(type) {
        case *execResultChangePeer:
            d.onReadyChangePeer(x)
        case *execResultCompactLog:
            d.onReadyCompactLog(x.firstIndex, x.truncatedIndex)
        case *execResultSplitRegion:
            d.onReadySplitRegion(x.derived, x.regions)
        }
    }
    res.execResults = nil
    if d.stopped {
        return
    }

    diff := d.SizeDiffHint + res.sizeDiffHint
    if diff > 0 {
        d.SizeDiffHint = diff
    } else {
        d.SizeDiffHint = 0
    }
}

// 处理 Raft 消息
func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
    log.Debug(fmt.Sprintf("%s handle raft message %s from %d to %d",
        d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId()))
    if !d.validateRaftMessage(msg) {
        return nil
    }
    if d.stopped {
        return nil
    }
    if msg.GetIsTombstone() {
        // 收到 tombstone 消息，尝试销毁自身
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
        // 快照文件不再需要，删除
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

// 校验 Raft 消息是否合法
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
    regionID := msg.GetRegionId()
    from := msg.GetFromPeer()
    to := msg.GetToPeer()
    log.Debug(fmt.Sprintf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId()))
    if to.GetStoreId() != d.storeID() {
        log.Warn(fmt.Sprintf("[region %d] store not match, to store id %d, mine %d, ignore it",
            regionID, to.GetStoreId(), d.storeID()))
        return false
    }
    if msg.RegionEpoch == nil {
        log.Error(fmt.Sprintf("[region %d] missing epoch in raft message, ignore it", regionID))
        return false
    }
    return true
}

// 检查消息是否应该被丢弃
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
    fromEpoch := msg.GetRegionEpoch()
    isVoteMsg := util.IsVoteMessage(msg.Message)
    fromStoreID := msg.FromPeer.GetStoreId()

    // 处理各种过期消息的场景
    region := d.Region()
    if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
        handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
        return true
    }
    target := msg.GetToPeer()
    if target.Id < d.PeerId() {
        log.Info(fmt.Sprintf("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId()))
        return true
    } else if target.Id > d.PeerId() {
        if d.MaybeDestroy() {
            log.Info(fmt.Sprintf("%s is stale as received a larger peer %s, destroying", d.Tag, target))
            d.destroyPeer()
            d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
        }
        return true
    }
    return false
}

// 处理过期消息，必要时发送 tombstone
func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
    needGC bool) {
    regionID := msg.RegionId
    fromPeer := msg.FromPeer
    toPeer := msg.ToPeer
    msgType := msg.Message.GetMsgType()

    if !needGC {
        log.Info(fmt.Sprintf("[region %d] raft message %s is stale, current %v ignore it",
            regionID, msgType, curEpoch))
        return
    }
    gcMsg := &rspb.RaftMessage{
        RegionId:    regionID,
        FromPeer:    fromPeer,
        ToPeer:      toPeer,
        RegionEpoch: curEpoch,
        IsTombstone: true,
    }
    if err := trans.Send(gcMsg); err != nil {
        log.Error(fmt.Sprintf("[region %d] send message failed %v", regionID, err))
    }
}

// 处理 tombstone 消息，尝试销毁自身
func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
    fromEpoch := msg.RegionEpoch
    if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
        return
    }
    if !util.PeerEqual(d.Meta, msg.ToPeer) {
        log.Info(fmt.Sprintf("%s receive stale gc msg, ignore", d.Tag))
        return
    }
    log.Info(fmt.Sprintf("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer))
    if d.MaybeDestroy() {
        d.destroyPeer()
    }
}

// 检查快照是否合法，返回冲突的快照 key
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
        log.Info(fmt.Sprintf("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID))
        return &key, nil
    }
    meta := d.ctx.storeMeta
    meta.Lock()
    defer meta.Unlock()
    if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
        if !d.isInitialized() {
            log.Info(fmt.Sprintf("%s stale delegate detected, skip", d.Tag))
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
        log.Info(fmt.Sprintf("%s region overlapped %s %s", d.Tag, existRegion, snapRegion))
        return &key, nil
    }

    // 检查快照文件是否存在
    _, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
    if err != nil {
        return nil, err
    }
    return nil, nil
}

// 销毁 peer 节点
func (d *peerMsgHandler) destroyPeer() {
    log.Info(fmt.Sprintf("%s starts destroy", d.Tag))
    regionID := d.regionId
    // 不能销毁正在应用快照的 peer
    meta := d.ctx.storeMeta
    meta.Lock()
    defer meta.Unlock()
    isInitialized := d.isInitialized()
    if err := d.Destroy(d.ctx.engine, false); err != nil {
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

// 处理配置变更结果
func (d *peerMsgHandler) onReadyChangePeer(cp *execResultChangePeer) {
    changeType := cp.confChange.ChangeType
    d.RaftGroup.ApplyConfChange(*cp.confChange)
    if cp.confChange.NodeId == 0 {
        // Apply failed, skip.
        return
    }
    meta := d.ctx.storeMeta
    meta.Lock()
    meta.setRegion(cp.region, d.peer)
    meta.Unlock()
    peerID := cp.peer.Id
    switch changeType {
    case eraftpb.ConfChangeType_AddNode:
        // 添加 peer 到缓存和心跳
        now := time.Now()
        if d.IsLeader() {
            d.PeersStartPendingTime[peerID] = now
        }
        d.insertPeerCache(cp.peer)
    case eraftpb.ConfChangeType_RemoveNode:
        // 从缓存移除 peer
        if d.IsLeader() {
            delete(d.PeersStartPendingTime, peerID)
        }
        d.removePeerCache(peerID)
    }

    // 如果是 leader，立即通知调度器
    if d.IsLeader() {
        log.Info(fmt.Sprintf("%s notify scheduler with change peer region %s", d.Tag, d.Region()))
        d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
    }
    myPeerID := d.PeerId()

    // 只关心移除自身
    if changeType == eraftpb.ConfChangeType_RemoveNode && cp.peer.StoreId == d.storeID() {
        if myPeerID == peerID {
            d.destroyPeer()
        } else {
            panic(fmt.Sprintf("%s trying to remove unknown peer %s", d.Tag, cp.peer))
        }
    }
}

// 处理日志压缩结果
func (d *peerMsgHandler) onReadyCompactLog(firstIndex uint64, truncatedIndex uint64) {
    raftLogGCTask := &runner.RaftLogGCTask{
        RaftEngine: d.ctx.engine.Raft,
        RegionID:   d.regionId,
        StartIdx:   d.LastCompactedIdx,
        EndIdx:     truncatedIndex + 1,
    }
    d.LastCompactedIdx = raftLogGCTask.EndIdx
    d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

// 处理分裂结果
func (d *peerMsgHandler) onReadySplitRegion(derived *metapb.Region, regions []*metapb.Region) {
    meta := d.ctx.storeMeta
    meta.Lock()
    defer meta.Unlock()
    regionID := derived.Id
    meta.setRegion(derived, d.peer)
    d.SizeDiffHint = 0
    isLeader := d.IsLeader()
    if isLeader {
        d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
        log.Info(fmt.Sprintf("%s notify scheduler with split count %d", d.Tag, len(regions)))
    }

    if meta.regionRanges.Delete(&regionItem{region: regions[0]}) == nil {
        panic(d.Tag + " original region should exist")
    }
    d.ApproximateSize = nil

    for _, newRegion := range regions {
        newRegionID := newRegion.Id
        notExist := meta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
        if notExist != nil {
            panic(fmt.Sprintf("%v %v newregion:%v, region:%v", d.Tag, notExist.(*regionItem).region, newRegion, regions[0]))
        }
        if newRegionID == regionID {
            continue
        }

        log.Info(fmt.Sprintf("[region %d] inserts new region %s", regionID, newRegion))
        if r, ok := meta.regions[newRegionID]; ok {
            if len(r.Peers) > 0 {
                panic(fmt.Sprintf("[region %d] duplicated region %s for split region %s",
                    newRegionID, r, newRegion))
            }
            d.ctx.router.close(newRegionID)
        }

        newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
        if err != nil {
            panic(fmt.Sprintf("create new split region %s error %v", newRegion, err))
        }
        metaPeer := newPeer.Meta

        for _, p := range newRegion.GetPeers() {
            newPeer.insertPeerCache(p)
        }

        campaigned := newPeer.MaybeCampaign(isLeader)

        if isLeader {
            newPeer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
        }

        meta.regions[newRegionID] = newRegion
        d.ctx.router.register(newPeer)
        _ = d.ctx.router.send(newRegionID, message.NewPeerMsg(message.MsgTypeStart, newRegionID, nil))
        if !campaigned {
            for i, msg := range meta.pendingVotes {
                if util.PeerEqual(msg.ToPeer, metaPeer) {
                    meta.pendingVotes = append(meta.pendingVotes[:i], meta.pendingVotes[i+1:]...)
                    _ = d.ctx.router.send(newRegionID, message.NewPeerMsg(message.MsgTypeRaftMessage, newRegionID, msg))
                    break
                }
            }
        }
    }
}

// preProposeRaftCommand 执行命令提议前的各种检查
func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
    // 检查 store_id，确保消息分发到正确的存储节点
    if err := util.CheckStoreID(req, d.storeID()); err != nil {
        return err
    }

    // 检查是否为 leader
    regionID := d.regionId
    leaderID := d.LeaderId()
    if !d.IsLeader() {
        leader := d.getPeerFromCache(leaderID)
        return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
    }
    // 检查 peer_id 是否一致
    if err := util.CheckPeerID(req, d.PeerId()); err != nil {
        return err
    }
    // 检查任期是否过期
    if err := util.CheckTerm(req, d.Term()); err != nil {
        return err
    }
    // 检查 region epoch 是否匹配
    err := util.CheckRegionEpoch(req, d.Region(), true)
    if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
        siblingRegion := d.findSiblingRegion()
        if siblingRegion != nil {
            errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
        }
        return errEpochNotMatching
    }
    return err
}

// proposeRaftCommand 处理客户端的 Raft 命令提议请求
// 这是分布式共识的入口方法，负责：
// 1. 验证命令的合法性（权限、任期、区域等检查）
// 2. 确保节点状态正常（未停止、能够处理请求）
// 3. 将命令提议给 Raft 组进行共识处理
//
// 在分布式系统中，只有通过 Raft 共识的命令才能被执行，
// 这保证了数据的一致性和系统的可靠性。
//
// 参数说明：
//   msg: 客户端发送的 Raft 命令请求
//   cb: 回调函数，用于返回处理结果给客户端
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
    // YOUR CODE HERE (lab1).

    // === 第一步：命令预检查 ===
    // Hint1: do `preProposeRaftCommand` check for the command, if the check fails, need to execute the
    // callback function and return the error results. `ErrResp` is useful to generate error response.

    // 执行命令的前置验证，包括：
    // - 存储 ID 检查：确保消息发送到正确的存储节点
    // - 领导者检查：只有领导者才能处理写请求
    // - 节点 ID 检查：确保消息发送到正确的节点
    // - 任期检查：防止处理过期的请求
    // - 区域纪元检查：确保区域信息是最新的
    commandCheckError := d.preProposeRaftCommand(msg)
    if commandCheckError != nil {
        // 如果任何检查失败，立即通过回调返回错误响应
        log.Warn(fmt.Sprintf("[region %d] Command validation failed: %v", d.regionId, commandCheckError))
        cb.Done(ErrResp(commandCheckError))
        return
    }

    // === 第二步：节点状态检查 ===
    // Hint2: Check if peer is stopped already, if so notify the callback that the region is removed, check
    // the `destroy` function for related utilities. `NotifyReqRegionRemoved` is useful to generate error response.

    // 检查当前节点是否已经停止服务
    // 如果节点已停止，需要清理资源并通知客户端区域已被移除
    if d.stopped {
        log.Warn(fmt.Sprintf("[region %d] Node has been stopped, cannot process requests", d.regionId))

        // 尝试清理节点资源
        destroyError := d.Destroy(d.ctx.engine, false)
        if destroyError != nil {
            log.Error(fmt.Sprintf("[region %d] Failed to destroy stopped node: %v", d.regionId, destroyError))
        }

        // 通知客户端该区域已被移除，无法继续处理请求
        NotifyReqRegionRemoved(d.regionId, cb)
        return
    }

    // === 第三步：准备响应并提交提议 ===
    // Hint3: Bind the possible response with term then do the real requests propose using the `Propose` function.
    // 提议请求时，需要将当前的 Raft term（任期）信息绑定到响应中
    // Note:
    // The peer that is being checked is a leader. It might step down to be a follower later. It
    // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
    // command log entry can't be committed. There are some useful information in the `ctx` of the `peerMsgHandler`.

    // 创建命令响应对象并绑定当前任期
    // 任期信息用于客户端检测领导者变更和处理网络分区场景
    commandResponse := newCmdResp()
    BindRespTerm(commandResponse, d.Term())

    // 将命令提议给 Raft 组进行共识处理
    // Propose 方法会：
    // 1. 将命令添加到 Raft 日志
    // 2. 尝试与其他节点达成共识
    // 3. 在命令被提交后执行回调
    proposalSuccessful := d.peer.Propose(d.ctx.engine.Raft, d.ctx.cfg, cb, msg, commandResponse)

    if !proposalSuccessful {
        // 提议失败可能的原因：
        // - 节点不再是领导者
        // - Raft 组状态异常
        // - 系统资源不足
        log.Warn(fmt.Sprintf("[region %d] Failed to propose command to Raft group", d.regionId))
        return
    }

    // 提议成功提交到 Raft，等待共识完成
    log.Debug(fmt.Sprintf("[region %d] Command successfully proposed to Raft group", d.regionId))
}

// 查找兄弟 region（用于 epoch 不匹配时补充 region 信息）
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

// 处理 Raft 日志 GC 定时器
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

    // 原始代码 magic：减 1
    y.Assert(compactIdx > 0)
    compactIdx -= 1
    if compactIdx < firstIdx {
        return
    }

    term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
    if err != nil {
        log.Fatal(fmt.Sprintf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx))
        panic(err)
    }

    // 创建并提议日志压缩请求
    regionID := d.regionId
    request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
    d.proposeRaftCommand(request, nil)
}

// 处理分裂检查定时器
func (d *peerMsgHandler) onSplitRegionCheckTick() {
    d.ticker.schedule(PeerTickSplitRegionCheck)
    // 避免频繁扫描，确保前一个任务完成
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

// 处理分裂准备
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

// 校验分裂请求
func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
    if len(splitKey) == 0 {
        err := errors.Errorf("%s split key should not be empty", d.Tag)
        log.Error("split key empty error", zap.Error(err))
        return err
    }

    if !d.IsLeader() {
        log.Info(fmt.Sprintf("%s not leader, skip", d.Tag))
        return &util.ErrNotLeader{
            RegionId: d.regionId,
            Leader:   d.getPeerFromCache(d.LeaderId()),
        }
    }

    region := d.Region()
    latestEpoch := region.GetRegionEpoch()

    // 分裂场景只需检查 version
    if latestEpoch.Version != epoch.Version {
        log.Info(fmt.Sprintf("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
            d.Tag, latestEpoch, epoch))
        return &util.ErrEpochNotMatch{
            Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
            Regions: []*metapb.Region{region},
        }
    }
    return nil
}

// 更新区域大小估算
func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
    d.ApproximateSize = &size
}

// 处理心跳定时器
func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
    d.ticker.schedule(PeerTickSchedulerHeartbeat)

    if !d.IsLeader() {
        return
    }
    d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

// 构造 Admin 请求
func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
    return &raft_cmdpb.RaftCmdRequest{
        Header: &raft_cmdpb.RaftRequestHeader{
            RegionId: regionID,
            Peer:     peer,
        },
    }
}

// 构造 CompactLog 请求
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